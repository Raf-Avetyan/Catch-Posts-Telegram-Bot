import asyncio
import threading
from datetime import datetime
from difflib import SequenceMatcher
from email.utils import parsedate_to_datetime
from html import escape as html_escape
import os
import tempfile
import json
import re
import time
import uuid
import xml.etree.ElementTree as ET
from typing import Any, Dict, Iterable, List, Optional
from urllib import error, parse, request

try:
    from PIL import Image
except Exception:
    Image = None  # type: ignore[assignment]

try:
    import twikit
    from twikit import Client as TwikitClient
except Exception:
    twikit = None  # type: ignore[assignment]
    TwikitClient = None  # type: ignore[assignment]

_TWIKIT_PATCH_APPLIED = False


def _apply_twikit_transaction_monkey_patch() -> bool:
    """
    Runtime patch for Twikit KEY_BYTE parsing breakage on newer X bundles.
    Remove when upstream twikit fixes transaction parsing.
    """
    global _TWIKIT_PATCH_APPLIED
    if _TWIKIT_PATCH_APPLIED:
        return True
    if twikit is None:
        return False

    try:
        tx_mod = __import__("twikit.x_client_transaction.transaction", fromlist=["ClientTransaction"])
        tx_mod.ON_DEMAND_FILE_REGEX = re.compile(
            r""",(\d+):["']ondemand\.s["']""",
            flags=(re.VERBOSE | re.MULTILINE),
        )
        tx_mod.ON_DEMAND_HASH_PATTERN = r',{}:"([0-9a-f]+)"'

        async def _patched_get_indices(self, home_page_response, session, headers):
            key_byte_indices = []
            response = self.validate_response(home_page_response) or self.home_page_response

            on_demand_file_match = tx_mod.ON_DEMAND_FILE_REGEX.search(str(response))
            if not on_demand_file_match:
                raise Exception("Couldn't get ondemand file index")
            on_demand_file_index = on_demand_file_match.group(1)

            regex = re.compile(tx_mod.ON_DEMAND_HASH_PATTERN.format(on_demand_file_index))
            filename_match = regex.search(str(response))
            if not filename_match:
                raise Exception("Couldn't get ondemand filename hash")
            filename = filename_match.group(1)

            on_demand_file_url = (
                f"https://abs.twimg.com/responsive-web/client-web/ondemand.s.{filename}a.js"
            )
            on_demand_file_response = await session.request(
                method="GET",
                url=on_demand_file_url,
                headers=headers,
            )
            key_byte_indices_match = tx_mod.INDICES_REGEX.finditer(str(on_demand_file_response.text))
            for item in key_byte_indices_match:
                key_byte_indices.append(item.group(2))
            if not key_byte_indices:
                raise Exception("Couldn't get KEY_BYTE indices")
            key_byte_indices = list(map(int, key_byte_indices))
            return key_byte_indices[0], key_byte_indices[1:]

        tx_mod.ClientTransaction.get_indices = _patched_get_indices
        _TWIKIT_PATCH_APPLIED = True
        return True
    except Exception:
        return False


if twikit is not None:
    _apply_twikit_transaction_monkey_patch()

try:
    from twscrape import API
except Exception:
    API = None  # type: ignore[assignment]

try:
    import snscrape.modules.twitter as sntwitter
except Exception:
    sntwitter = None  # type: ignore[assignment]

from telethon import Button, TelegramClient, events

from config import (
    api_hash,
    api_id,
    bot_token,
    comment_ai_provider,
    forward_to_channel,
    forwarding_enabled,
    gemini_api_key,
    gemini_model,
    openrouter_api_key,
    openrouter_app_name,
    openrouter_model,
    openrouter_site_url,
    twitter_bot_session_name,
    twitter_account_email,
    twitter_account_email_password,
    twitter_account_password,
    twitter_account_username,
    twitter_enabled,
    twitter_fetch_limit,
    twitter_clean_min_hype_score,
    twitter_clean_forward_channel,
    twitter_min_hype_score,
    twitter_poll_seconds,
    twitter_usernames,
    twitter_use_twikit_only,
    twitter_use_saved_cookies_only,
    twitter_cookies_json,
    twikit_cookies_path,
    twscrape_accounts_db,
)
from db import Database
from gemini_client import GeminiRewriter


def _to_iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return value
    return datetime.utcnow().isoformat()


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


class TwitterCollector:
    def __init__(self, db: Database):
        self.db = db
        self.enabled = twitter_enabled and bool(twitter_usernames)
        self.twikit_only = twitter_use_twikit_only
        self._deps_ok = bool(TwikitClient is not None) if self.twikit_only else bool(TwikitClient is not None and API is not None)
        self.api = API(str(twscrape_accounts_db)) if API is not None else None
        self.usernames = twitter_usernames
        self.poll_seconds = max(30, twitter_poll_seconds)
        self.fetch_limit = max(1, twitter_fetch_limit)
        self.min_hype_score = min(10, max(5, twitter_min_hype_score))
        self.clean_min_hype_score = min(10, max(5, twitter_clean_min_hype_score))
        self.clean_forward_channel = twitter_clean_forward_channel
        self._ready = False
        self.rewriter = GeminiRewriter(api_key=gemini_api_key, model=gemini_model)
        self.bot_client: Optional[TelegramClient] = None
        self.twikit_client: Optional[TwikitClient] = None
        self.last_seen_tweet_id: Dict[str, int] = {}
        self._twikit_disabled_until_ts = 0.0
        self._snscrape_unavailable_logged = False
        self._publish_jobs: Dict[str, Dict[str, Any]] = {}
        self.unpublished_ttl_seconds = 24 * 60 * 60
        self.main_post_ttl_seconds = 60 * 60
        self._main_post_cleanup_jobs: Dict[str, Dict[str, Any]] = {}
        self._gemini_comment_disabled_until_ts = 0.0
        self.comment_ai_provider = comment_ai_provider or "openrouter"
        self.openrouter_api_key = openrouter_api_key
        self.openrouter_model = openrouter_model or "openrouter/auto"
        self.openrouter_site_url = openrouter_site_url
        self.openrouter_app_name = openrouter_app_name or "Catch Posts Bot"
        self._comment_ai_last_error = ""

    def _twikit_disabled(self) -> bool:
        return time.time() < self._twikit_disabled_until_ts

    def _disable_twikit_temporarily(self, seconds: int, reason: str) -> None:
        self._twikit_disabled_until_ts = max(self._twikit_disabled_until_ts, time.time() + max(60, seconds))
        until = datetime.fromtimestamp(self._twikit_disabled_until_ts).isoformat(timespec="seconds")
        print(f"[X][WARN] Twikit temporarily disabled until {until} ({reason}).")

    def _gemini_comment_disabled(self) -> bool:
        return time.time() < self._gemini_comment_disabled_until_ts

    def _disable_gemini_comment_temporarily(self, seconds: float, reason: str) -> None:
        seconds = max(15.0, float(seconds))
        self._gemini_comment_disabled_until_ts = max(
            self._gemini_comment_disabled_until_ts,
            time.time() + seconds,
        )
        until = datetime.fromtimestamp(self._gemini_comment_disabled_until_ts).isoformat(timespec="seconds")
        print(f"[X][WARN] Gemini comment generation paused until {until} ({reason}).")

    @staticmethod
    def _extract_retry_after_seconds(error_text: str) -> float:
        value = (error_text or "").strip()
        if not value:
            return 0.0
        match = re.search(r"retry in\s+([0-9]+(?:\.[0-9]+)?)s", value, flags=re.IGNORECASE)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                return 0.0
        return 0.0

    def _generate_openrouter_text(self, prompt: str, temperature: float = 0.9) -> str:
        self._comment_ai_last_error = ""
        if not self.openrouter_api_key:
            self._comment_ai_last_error = "OpenRouter disabled: missing OPENROUTER_API_KEY."
            return ""

        payload = {
            "model": self.openrouter_model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": temperature,
        }
        headers = {
            "Authorization": f"Bearer {self.openrouter_api_key}",
            "Content-Type": "application/json",
        }
        if self.openrouter_site_url:
            headers["HTTP-Referer"] = self.openrouter_site_url
        if self.openrouter_app_name:
            headers["X-Title"] = self.openrouter_app_name

        req = request.Request(
            url="https://openrouter.ai/api/v1/chat/completions",
            method="POST",
            headers=headers,
            data=json.dumps(payload).encode("utf-8"),
        )
        try:
            with request.urlopen(req, timeout=25) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except error.HTTPError as exc:
            body = ""
            try:
                body = exc.read().decode("utf-8", errors="ignore").strip()
            except Exception:
                body = ""
            self._comment_ai_last_error = f"OpenRouter HTTP {exc.code} {exc.reason}".strip()
            if body:
                self._comment_ai_last_error = f"{self._comment_ai_last_error}: {body[:500]}"
            return ""
        except error.URLError as exc:
            self._comment_ai_last_error = f"OpenRouter URL error: {exc.reason}"
            return ""
        except TimeoutError:
            self._comment_ai_last_error = "OpenRouter request timed out."
            return ""
        except json.JSONDecodeError as exc:
            self._comment_ai_last_error = f"OpenRouter invalid JSON: {exc}"
            return ""

        choices = data.get("choices") or []
        for choice in choices:
            message = choice.get("message") or {}
            content = (message.get("content") or "").strip()
            if content:
                return content

        err = data.get("error") or {}
        if isinstance(err, dict) and err.get("message"):
            self._comment_ai_last_error = f"OpenRouter error: {err.get('message')}"
        else:
            self._comment_ai_last_error = "OpenRouter returned no usable text."
        return ""

    def _rewrite_tweet_text(self, text: str) -> str:
        original = self.rewriter.clean_footer_text(text or "")
        if not original:
            return original

        prompts = [
            (
                "Rewrite this Telegram post by changing wording and sentence structure. "
                "Keep exact meaning, facts, links, and emojis. "
                "Do not add or remove facts. "
                "Do not change the core message. "
                "If the original post already starts with an emoji/title lead line, keep that first line unchanged. "
                "Only rewrite the main body text below it. "
                "If the original post has no lead line, create a one-word uppercase lead label in this format: "
                "WORD:, then one empty line, then the post body. "
                "Do not include source/footer labels. "
                "Return only rewritten post text.\n\n"
                f"Post:\n{original}"
            ),
            (
                "Paraphrase this post more strongly while preserving exact meaning, links, emojis, and facts. "
                "Ensure noticeably different wording and sentence structure. "
                "If there is a lead label, use BREAKING or LATEST, not KEY. "
                "Return only paraphrased text.\n\n"
                f"Post:\n{original}"
            ),
            (
                "Rewrite this post with strong wording changes while preserving all facts and links exactly. "
                "Use a different sentence flow from the source. "
                "Do not copy source phrases except proper nouns, numbers, or links. "
                "Return only the final rewritten text.\n\n"
                f"Source:\n{original}"
            ),
        ]
        temperatures = [0.6, 0.85, 0.95]

        best = original
        best_similarity = 1.0
        for prompt, temperature in zip(prompts, temperatures):
            candidate = ""
            if self.openrouter_api_key:
                candidate = self._generate_openrouter_text(prompt, temperature=temperature).strip()
                if not candidate and self._comment_ai_last_error:
                    print(f"[X][WARN] OpenRouter rewrite attempt failed: {self._comment_ai_last_error}")
            if not candidate:
                candidate = (self.rewriter._generate_text(prompt, temperature=temperature) or "").strip()
                if not candidate:
                    last_error = (getattr(self.rewriter, "last_error", "") or "").strip()
                    if last_error:
                        print(f"[X][WARN] Gemini rewrite attempt failed: {last_error}")
                    continue

            cleaned = self.rewriter.clean_footer_text(candidate) or original
            cleaned = self.rewriter._normalize_lead_label(cleaned)
            cleaned = self.rewriter._ensure_lead_banner_block(cleaned, original)
            similarity = SequenceMatcher(a=original, b=cleaned).ratio()
            if similarity < best_similarity:
                best = cleaned
                best_similarity = similarity
            if similarity <= 0.86 and cleaned.strip().lower() != original.strip().lower():
                return cleaned

        if best.strip().lower() != original.strip().lower():
            return best
        return self.rewriter._force_surface_change_preserving_source_lead(original)

    async def _get_twikit_cookie_header(self) -> str:
        if TwikitClient is None:
            return ""

        client = TwikitClient("en-US")
        cookies_file = str(twikit_cookies_path)

        # 1) Explicit cookie JSON from env has highest priority.
        if twitter_cookies_json:
            try:
                cookie_dict = json.loads(twitter_cookies_json)
                if isinstance(cookie_dict, dict) and cookie_dict:
                    client.set_cookies(cookie_dict)
                    print("[X][INFO] Twikit cookies loaded from TWITTER_COOKIES_JSON.")
                else:
                    print("[X][ERROR] TWITTER_COOKIES_JSON must be a JSON object.")
                    return ""
            except Exception as exc:
                print(f"[X][ERROR] Invalid TWITTER_COOKIES_JSON: {exc}")
                return ""
            cookies = client.get_cookies()
            if not cookies:
                print("[X][ERROR] Twikit has no cookies after TWITTER_COOKIES_JSON load.")
                return ""
            self.twikit_client = client
            return "; ".join(f"{k}={v}" for k, v in cookies.items())

        # 2) Prefer existing cookies file to avoid login challenge flow.
        if twikit_cookies_path.exists():
            try:
                client.load_cookies(cookies_file)
                cookies = client.get_cookies()
                if cookies:
                    print(f"[X][INFO] Twikit loaded cookies from {cookies_file}")
                    self.twikit_client = client
                    return "; ".join(f"{k}={v}" for k, v in cookies.items())
                print(f"[X][WARN] Cookies file exists but empty: {cookies_file}")
            except Exception as exc:
                print(f"[X][WARN] Twikit failed loading cookies file: {exc}")

        # 3) Cookie-only mode requested: stop before password login.
        if twitter_use_saved_cookies_only:
            print(f"[X][ERROR] Cookie-only mode enabled, but valid cookies not found: {cookies_file}")
            return ""

        # 4) Last resort: password login (can be blocked by Cloudflare).
        if not twitter_account_username or not twitter_account_password:
            print("[X][ERROR] Twikit credentials missing and no valid cookies available.")
            return ""

        try:
            await client.login(
                auth_info_1=twitter_account_username,
                auth_info_2=twitter_account_email or None,
                password=twitter_account_password,
                cookies_file=cookies_file,
                enable_ui_metrics=False,
            )
            print("[X][INFO] Twikit login successful.")
        except Exception as exc:
            print(f"[X][ERROR] Twikit login failed: {exc}")
            return ""

        cookies: Dict[str, str] = client.get_cookies()
        if not cookies:
            print("[X][ERROR] Twikit has no cookies after auth.")
            return ""
        self.twikit_client = client
        return "; ".join(f"{k}={v}" for k, v in cookies.items())

    async def _ensure_twikit_client(self) -> bool:
        if TwikitClient is None:
            return False
        if self.twikit_client is not None:
            return True
        _ = await self._get_twikit_cookie_header()
        return self.twikit_client is not None

    async def _bootstrap_twscrape_account(self) -> bool:
        if self.twikit_only:
            return True
        if self.api is None:
            print("[X][WARN] Twitter deps missing. Install twscrape + twikit.")
            return False
        if not twitter_account_username or not twitter_account_password:
            print("[X][WARN] Missing TWITTER_ACCOUNT_USERNAME/TWITTER_ACCOUNT_PASSWORD.")
            return False

        cookie_header = await self._get_twikit_cookie_header()
        normalized_username = twitter_account_username.lstrip("@")

        try:
            await self.api.pool.add_account(
                normalized_username,
                twitter_account_password,
                twitter_account_email,
                twitter_account_email_password,
                cookies=cookie_header or None,
            )
        except Exception:
            # Usually account already exists in twscrape db.
            pass

        try:
            await self.api.pool.login_all()
            return True
        except Exception as exc:
            # Cloudflare blocks are common on explicit login; continue with cookie-only mode if available.
            if cookie_header:
                print(f"[X][WARN] twscrape login blocked, using cookie-only mode: {exc}")
                return True
            print(f"[X][ERROR] twscrape login failed: {exc}")
            return False

    @staticmethod
    def _extract_media_urls(tweet: Any) -> List[str]:
        media_urls: List[str] = []
        video_urls: List[str] = []

        def _is_video_like(url: str) -> bool:
            u = (url or "").lower()
            return (
                "video.twimg.com" in u
                or ".mp4" in u
                or ".webm" in u
                or "/video/" in u
            )

        def _add_url(value: Any) -> None:
            if not isinstance(value, str):
                return
            url = value.strip()
            if not url.startswith("http"):
                return
            media_urls.append(url)
            if _is_video_like(url):
                video_urls.append(url)

        def _extract_from_item(item: Any) -> None:
            if item is None:
                return

            if isinstance(item, str):
                _add_url(item)
                return

            if isinstance(item, dict):
                for key in (
                    "media_url_https",
                    "media_url",
                    "url",
                    "expanded_url",
                    "previewUrl",
                    "fullUrl",
                    "thumbnail_url",
                    "thumbnailUrl",
                ):
                    _add_url(item.get(key))

                variants = item.get("variants") or ((item.get("video_info") or {}).get("variants"))
                if isinstance(variants, list):
                    mp4_variants = [
                        v for v in variants
                        if isinstance(v, dict)
                        and isinstance(v.get("url"), str)
                        and "mp4" in str(v.get("content_type", "")).lower()
                    ]
                    if mp4_variants:
                        best = sorted(mp4_variants, key=lambda v: int(v.get("bitrate") or 0), reverse=True)[0]
                        _add_url(best.get("url"))
                return

            for attr in (
                "media_url_https",
                "media_url",
                "url",
                "expanded_url",
                "previewUrl",
                "fullUrl",
                "thumbnail_url",
                "thumbnailUrl",
            ):
                _add_url(getattr(item, attr, None))

            variants = getattr(item, "variants", None)
            if isinstance(variants, list):
                mp4_variants = []
                for v in variants:
                    if not isinstance(v, dict):
                        continue
                    if "mp4" in str(v.get("content_type", "")).lower() and isinstance(v.get("url"), str):
                        mp4_variants.append(v)
                if mp4_variants:
                    best = sorted(mp4_variants, key=lambda v: int(v.get("bitrate") or 0), reverse=True)[0]
                    _add_url(best.get("url"))

        # 1) Common containers across twikit/snscrape/fallback wrappers.
        for attr in ("media", "photos", "videos", "video"):
            value = getattr(tweet, attr, None)
            if value is None:
                continue
            if isinstance(value, list):
                for it in value:
                    _extract_from_item(it)
            else:
                _extract_from_item(value)

        # 2) Some wrappers expose raw dict payload.
        raw = getattr(tweet, "_data", None) or getattr(tweet, "raw", None)
        if isinstance(raw, dict):
            media = raw.get("media") or ((raw.get("entities") or {}).get("media")) or ((raw.get("extended_entities") or {}).get("media"))
            if isinstance(media, list):
                for it in media:
                    _extract_from_item(it)

        # unique preserve order + skip unsupported stream manifests
        uniq: List[str] = []
        seen = set()
        for url in media_urls:
            if ".m3u8" in url.lower():
                continue
            if url in seen:
                continue
            seen.add(url)
            uniq.append(url)

        # If video exists, prefer real video URLs over video thumbnail images.
        if video_urls:
            filtered: List[str] = []
            for url in uniq:
                lu = url.lower()
                is_video_thumb = (
                    "ext_tw_video_thumb" in lu
                    or "video_thumb" in lu
                    or ("pbs.twimg.com" in lu and re.search(r"\.(jpg|jpeg|png|webp)(\?|$)", lu))
                )
                if is_video_thumb:
                    continue
                filtered.append(url)
            if filtered:
                uniq = filtered
        return uniq

    @staticmethod
    def _has_video_media_urls(media_urls: List[str]) -> bool:
        for url in media_urls:
            lu = (url or "").lower()
            if (
                "video.twimg.com" in lu
                or ".mp4" in lu
                or ".webm" in lu
                or ".mov" in lu
                or ".mkv" in lu
            ):
                return True
        return False

    @staticmethod
    def _tweet_has_video(tweet: Any) -> bool:
        def _item_has_video(item: Any) -> bool:
            if item is None:
                return False
            if isinstance(item, str):
                u = item.lower()
                return ("video.twimg.com" in u) or (".mp4" in u) or (".webm" in u)
            if isinstance(item, dict):
                media_type = str(item.get("type") or item.get("mediaType") or "").lower()
                if media_type in {"video", "animated_gif"}:
                    return True
                if item.get("video_info") or item.get("variants"):
                    return True
                for key in ("url", "media_url", "media_url_https", "previewUrl", "fullUrl"):
                    if _item_has_video(item.get(key)):
                        return True
                return False

            for attr in ("type", "mediaType"):
                val = str(getattr(item, attr, "") or "").lower()
                if val in {"video", "animated_gif"}:
                    return True
            if getattr(item, "video_info", None) or getattr(item, "variants", None):
                return True
            for attr in ("url", "media_url", "media_url_https", "previewUrl", "fullUrl"):
                if _item_has_video(getattr(item, attr, None)):
                    return True
            return False

        for attr in ("media", "videos", "video"):
            value = getattr(tweet, attr, None)
            if isinstance(value, list):
                if any(_item_has_video(v) for v in value):
                    return True
            elif _item_has_video(value):
                return True

        raw = getattr(tweet, "_data", None) or getattr(tweet, "raw", None)
        if isinstance(raw, dict):
            media = raw.get("media") or ((raw.get("entities") or {}).get("media")) or ((raw.get("extended_entities") or {}).get("media"))
            if isinstance(media, list) and any(_item_has_video(v) for v in media):
                return True
        return False

    async def _start_forward_bot(self) -> None:
        if not forwarding_enabled or not forward_to_channel or not bot_token:
            print("[X][WARN] Twitter -> Telegram forwarding disabled by config.")
            return
        try:
            self.bot_client = TelegramClient(twitter_bot_session_name, api_id, api_hash)
            await self.bot_client.start(bot_token=bot_token)
            self.bot_client.add_event_handler(self._on_publish_click, events.CallbackQuery(pattern=b"^pub:"))
            print(f"[X][INFO] Twitter forwarding target: {forward_to_channel}")
            if self.comment_ai_provider == "openrouter" and self.openrouter_api_key:
                print(f"[X][INFO] Comment AI provider: OpenRouter ({self.openrouter_model})")
            else:
                print(f"[X][INFO] Comment AI provider: Gemini ({gemini_model})")
        except Exception as exc:
            self.bot_client = None
            print(f"[X][ERROR] Failed to start Twitter forward bot: {exc}")

    @staticmethod
    def _split_text(text: str, chunk_size: int = 4000) -> List[str]:
        if len(text) <= chunk_size:
            return [text]
        return [text[i : i + chunk_size] for i in range(0, len(text), chunk_size)]

    @staticmethod
    def _chunks(items: List[str], size: int) -> List[List[str]]:
        return [items[i : i + size] for i in range(0, len(items), size)]

    @staticmethod
    def _strip_publish_meta_lines(text: str) -> str:
        value = (text or "").strip()
        if not value:
            return value
        lines = value.splitlines()
        while lines and not lines[-1].strip():
            lines.pop()
        patterns = [
            re.compile(r"^Hype Score:\s*(10|[1-9])/10$", re.IGNORECASE),
            re.compile(r"^@\w{1,30}$"),
            re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+\-]\d{2}:\d{2})?$"),
        ]
        removed = True
        while lines and removed:
            removed = False
            tail = lines[-1].strip()
            for pattern in patterns:
                if pattern.match(tail):
                    lines.pop()
                    removed = True
                    while lines and not lines[-1].strip():
                        lines.pop()
                    break
        return "\n".join(lines).strip()

    @staticmethod
    def _build_x_compose_url(clean_text: str, media_urls: List[str]) -> str:
        text = (clean_text or "").strip()
        if text:
            text = f"{text}\n\nMore updates in Telegram (link in bio)"
        else:
            text = "More updates in Telegram (link in bio)"
        # Keep URL length safer for intent endpoint.
        if len(text) > 1200:
            text = text[:1197].rstrip() + "..."
        encoded = parse.quote_plus(text)
        return f"https://x.com/intent/tweet?text={encoded}"

    @staticmethod
    def _build_tweet_url(username: str, tweet_id: Any) -> str:
        username_clean = (username or "").strip().lstrip("@")
        tweet_id_str = str(tweet_id or "").strip()
        if not username_clean or not tweet_id_str:
            return ""
        return f"https://x.com/{username_clean}/status/{tweet_id_str}"

    @staticmethod
    def _build_x_reply_url(tweet_id: Any, reply_text: str) -> str:
        tweet_id_str = str(tweet_id or "").strip()
        text = (reply_text or "").strip()
        if not tweet_id_str or not text:
            return ""
        if len(text) > 240:
            text = text[:237].rstrip() + "..."
        encoded = parse.quote_plus(text)
        return f"https://twitter.com/intent/tweet?in_reply_to={tweet_id_str}&text={encoded}"

    @staticmethod
    def _extract_comment_keywords(text: str) -> List[str]:
        value = (text or "").lower()
        words = re.findall(r"[a-z][a-z0-9]{3,24}", value)
        stop = {
            "this", "that", "with", "from", "have", "will", "into", "about", "their", "there",
            "today", "latest", "breaking", "update", "live", "updates", "who", "meet", "heading",
            "back", "very", "more", "only", "just", "really", "then", "than", "what", "when",
            "where", "which", "while", "post", "tweet", "crew",
        }
        out: List[str] = []
        for word in words:
            if word in stop:
                continue
            if word not in out:
                out.append(word)
            if len(out) >= 8:
                break
        return out

    def _is_relevant_comment(self, source_text: str, comment_text: str) -> bool:
        source = (source_text or "").lower()
        comment = (comment_text or "").lower()
        if not source or not comment:
            return False

        keywords = self._extract_comment_keywords(source)
        if any(keyword in comment for keyword in keywords):
            return True

        topic_aliases = [
            {"moon", "nasa", "astronaut", "space", "artemis", "rocket"},
            {"bitcoin", "btc", "crypto", "ethereum", "eth", "altcoin", "token"},
            {"stocks", "stock", "equities", "nasdaq", "dow", "s&p", "wall street", "market"},
            {"sec", "cftc", "fed", "rates", "cpi", "inflation", "regulation"},
            {"war", "iran", "russia", "ukraine", "china", "tariff", "sanction"},
        ]
        for alias_group in topic_aliases:
            if any(token in source for token in alias_group) and any(token in comment for token in alias_group):
                return True

        return False

    def _generate_reply_comment(self, post_text: str, username: str = "") -> str:
        value = self._strip_publish_meta_lines(post_text or "")
        if not value:
            return ""
        if self._gemini_comment_disabled():
            return ""

        hash_value = sum(ord(c) for c in value)
        styles = [
            "funny",
            "hype",
            "casual",
            "playful",
            "sharp",
            "punchy",
        ]
        style = styles[hash_value % len(styles)]
        prompts = [
            (
                f"Write 5 distinct very short human replies to this X post in a {style} style. "
                "It must sound like a real person on Twitter, not a news account, analyst, or corporate brand. "
                "Make it funny, interesting, hype, punchy, or playful depending on the post. "
                "Use 3 to 12 words only. "
                "No hashtags. No quotation marks. No markdown. "
                "No role-speech, no formal tone, no boring summary, no generic filler like 'huge if true'. "
                "Avoid repeating the headline. "
                "A tiny slang touch is okay if natural. "
                "A laughing emoji like 😂, 🤣, or 😭 is allowed when it genuinely fits and makes the reply feel more human. "
                "Return exactly 5 options, one per line, and make them clearly different from each other.\n\n"
                f"Post:\n{value}"
            ),
            (
                "Write 5 short Twitter replies for this post. "
                "Sound human, casual, funny, interesting, or hype, never formal. "
                "Each reply must be 3 to 7 words. "
                "Do not restate the headline. "
                "No hashtags. No quotes. No markdown. "
                "You may use one laughing-style emoji like 😂, 🤣, or 😭 if it feels natural. "
                "Return one option per line only.\n\n"
                f"Post:\n{value}"
            ),
            (
                "Give 5 natural one-line reactions a real person would reply with on Twitter. "
                "Keep them short, punchy, funny, interesting, and different. "
                "No hashtags. "
                "A laughing emoji like 😂, 🤣, or 😭 is okay if it improves the reaction. "
                "No corporate tone. "
                "Return only the 5 lines.\n\n"
                f"Post:\n{value}"
            ),
        ]

        temperatures = [0.95, 1.0, 0.85]
        for prompt, temperature in zip(prompts, temperatures):
            generated = ""
            provider = self.comment_ai_provider
            if provider == "openrouter":
                generated = (self._generate_openrouter_text(prompt, temperature=temperature) or "").strip()
                if not generated:
                    detail = (self._comment_ai_last_error or "").strip() or "unknown empty response"
                    print(f"[X][WARN] OpenRouter comment generation attempt failed @{username or 'unknown'}: {detail}")
            if not generated and provider != "gemini":
                generated = (self.rewriter._generate_text(prompt, temperature=temperature) or "").strip()
            if not generated:
                last_error = (getattr(self.rewriter, "last_error", "") or "").strip()
                last_finish = (getattr(self.rewriter, "last_finish_reason", "") or "").strip()
                detail = last_error or (f"finishReason={last_finish}" if last_finish else "unknown empty response")
                provider_name = "Gemini" if provider != "openrouter" or not self.openrouter_api_key else "OpenRouter->Gemini fallback"
                print(f"[X][WARN] {provider_name} comment generation attempt failed @{username or 'unknown'}: {detail}")
                retry_after_seconds = self._extract_retry_after_seconds(detail)
                if "429" in detail or "RESOURCE_EXHAUSTED" in detail or "quota" in detail.lower():
                    self._disable_gemini_comment_temporarily(
                        retry_after_seconds or 60.0,
                        "Gemini quota/rate limit reached",
                    )
                    return ""
                continue

            options: List[str] = []
            seen = set()
            for line in generated.splitlines():
                cleaned = re.sub(r"^\s*[-*0-9.)]+\s*", "", line).strip()
                cleaned = re.sub(r"\s+", " ", cleaned).strip(" \"'`")
                cleaned = re.sub(r"[#`*_>\[\]]", "", cleaned).strip()
                if not cleaned:
                    continue
                word_count = len(cleaned.split())
                if word_count < 2 or word_count > 10:
                    continue
                if not self._is_relevant_comment(value, cleaned):
                    continue
                lowered = cleaned.lower()
                if lowered in seen:
                    continue
                seen.add(lowered)
                options.append(cleaned)

            if options:
                return options[hash_value % len(options)]
            print(
                f"[X][WARN] Gemini comment generation returned unusable lines @{username or 'unknown'}: "
                f"raw={generated[:300]!r}"
            )
        return ""

    async def _send_to_channel_media_first(
        self,
        target_channel: str,
        text: str,
        media_paths: List[str],
        parse_mode: Optional[str] = None,
        buttons: Optional[Any] = None,
    ) -> Optional[int]:
        if not self.bot_client:
            return None

        def _is_video_file(path: str) -> bool:
            lp = (path or "").lower()
            return lp.endswith(".mp4") or lp.endswith(".webm") or lp.endswith(".mov") or lp.endswith(".mkv")

        first_message_id: Optional[int] = None
        button_attached = False
        if media_paths:
            remaining = text or ""
            caption_sent = False
            contains_video = any(_is_video_file(p) for p in media_paths)

            # Image-only posts: send as albums so all images stay in one post.
            if not contains_video:
                for batch_index, batch in enumerate(self._chunks(media_paths, 10)):
                    prepared_batch, temp_fixed_paths = self._prepare_image_batch_for_album(batch)
                    caption = None
                    pm = None
                    can_attach_button_here = bool(buttons) and len(batch) == 1 and not button_attached

                    if not caption_sent and remaining:
                        if len(remaining) <= 1024:
                            caption = remaining
                            remaining = ""
                        else:
                            cut_at = remaining.rfind("\n", 0, 1024)
                            if cut_at < 120:
                                cut_at = 1024
                            caption = remaining[:cut_at].strip()
                            remaining = remaining[cut_at:].lstrip()
                        caption_sent = True
                        if caption and parse_mode:
                            pm = parse_mode

                    try:
                        sent = await self.bot_client.send_file(
                            target_channel,
                            prepared_batch,
                            caption=caption,
                            parse_mode=pm,
                            buttons=buttons if can_attach_button_here else None,
                        )
                        if can_attach_button_here:
                            button_attached = True
                    except Exception as media_exc:
                        if "Failure while processing image" in str(media_exc):
                            print("[X][WARN] Image album failed; dropping this batch to avoid split/file fallback.")
                            sent = None
                        else:
                            raise
                    finally:
                        for p in temp_fixed_paths:
                            try:
                                os.remove(p)
                            except OSError:
                                pass

                    if first_message_id is None:
                        if isinstance(sent, list) and sent:
                            first_message_id = sent[0].id
                        elif hasattr(sent, "id"):
                            first_message_id = sent.id
            else:
                # Mixed/video posts: send per-file to preserve video behavior.
                for path in media_paths:
                    caption = None
                    pm = None
                    can_attach_button_here = bool(buttons) and not button_attached and first_message_id is None
                    if not caption_sent and remaining:
                        if len(remaining) <= 1024:
                            caption = remaining
                            remaining = ""
                        else:
                            cut_at = remaining.rfind("\n", 0, 1024)
                            if cut_at < 120:
                                cut_at = 1024
                            caption = remaining[:cut_at].strip()
                            remaining = remaining[cut_at:].lstrip()
                        caption_sent = True
                        if caption and parse_mode:
                            pm = parse_mode

                    try:
                        sent = await self.bot_client.send_file(
                            target_channel,
                            path,
                            caption=caption,
                            parse_mode=pm,
                            buttons=buttons if can_attach_button_here else None,
                            supports_streaming=_is_video_file(path),
                        )
                        if can_attach_button_here:
                            button_attached = True
                    except Exception as media_exc:
                        if "Failure while processing image" in str(media_exc):
                            fixed = self._convert_image_to_jpeg_temp(path)
                            if fixed:
                                try:
                                    sent = await self.bot_client.send_file(
                                        target_channel,
                                        fixed,
                                        caption=caption,
                                        parse_mode=pm,
                                        buttons=buttons if can_attach_button_here else None,
                                    )
                                finally:
                                    try:
                                        os.remove(fixed)
                                    except OSError:
                                        pass
                                if can_attach_button_here:
                                    button_attached = True
                            else:
                                print("[X][WARN] Dropping unsupported media instead of sending as file.")
                                continue
                        else:
                            raise

                    if first_message_id is None:
                        if isinstance(sent, list) and sent:
                            first_message_id = sent[0].id
                        elif hasattr(sent, "id"):
                            first_message_id = sent.id

            if remaining:
                chunks = self._split_text(remaining)
                for i, chunk in enumerate(chunks):
                    msg = await self.bot_client.send_message(
                        target_channel,
                        chunk,
                        parse_mode=parse_mode,
                        buttons=buttons if (not button_attached and i == 0) else None,
                    )
                    if not button_attached and i == 0 and buttons is not None:
                        button_attached = True
                    if first_message_id is None:
                        first_message_id = msg.id
        elif text:
            msg = await self.bot_client.send_message(
                target_channel,
                text,
                parse_mode=parse_mode,
                buttons=buttons,
            )
            if buttons is not None:
                button_attached = True
            first_message_id = msg.id

        # If button was not attached during send flow (common for media albums), attach it by editing first message.
        if buttons is not None and not button_attached and first_message_id is not None:
            try:
                await self.bot_client.edit_message(target_channel, first_message_id, buttons=buttons)
                button_attached = True
            except Exception:
                pass
        return first_message_id

    def _download_media_urls_to_temp(self, media_urls: List[str]) -> List[str]:
        paths: List[str] = []
        for media_url in media_urls:
            req = request.Request(media_url, headers={"User-Agent": "Mozilla/5.0"})
            try:
                with request.urlopen(req, timeout=20) as resp:
                    content = resp.read()
                    content_type = (resp.headers.get("Content-Type") or "").lower()
            except (error.URLError, error.HTTPError, TimeoutError):
                continue

            if not content:
                continue

            suffix = ".jpg"
            if "png" in content_type:
                suffix = ".png"
            elif "webp" in content_type:
                suffix = ".webp"
            elif "gif" in content_type:
                suffix = ".gif"
            elif "mp4" in content_type:
                suffix = ".mp4"
            else:
                lower_url = media_url.lower()
                if ".mp4" in lower_url:
                    suffix = ".mp4"
                elif ".webm" in lower_url:
                    suffix = ".webm"
                elif ".mov" in lower_url:
                    suffix = ".mov"
                elif ".png" in lower_url:
                    suffix = ".png"
                elif ".webp" in lower_url:
                    suffix = ".webp"
                elif ".gif" in lower_url:
                    suffix = ".gif"

            fd, tmp_path = tempfile.mkstemp(prefix="tweet_media_", suffix=suffix)
            with os.fdopen(fd, "wb") as f:
                f.write(content)
            paths.append(tmp_path)
        return paths

    @staticmethod
    def _convert_image_to_jpeg_temp(path: str) -> Optional[str]:
        if Image is None:
            return None
        try:
            with Image.open(path) as img:
                if img.mode != "RGB":
                    img = img.convert("RGB")
                fd, out_path = tempfile.mkstemp(prefix="tweet_media_fix_", suffix=".jpg")
                os.close(fd)
                img.save(out_path, format="JPEG", quality=92, optimize=True)
                return out_path
        except Exception:
            return None

    def _prepare_image_batch_for_album(self, paths: List[str]) -> tuple[List[str], List[str]]:
        prepared: List[str] = []
        temp_created: List[str] = []
        for p in paths:
            lp = (p or "").lower()
            # Keep already safe formats.
            if lp.endswith(".jpg") or lp.endswith(".jpeg") or lp.endswith(".png"):
                prepared.append(p)
                continue
            fixed = self._convert_image_to_jpeg_temp(p)
            if fixed:
                prepared.append(fixed)
                temp_created.append(fixed)
            else:
                # Keep original if conversion unavailable; Telegram may still accept.
                prepared.append(p)
        return prepared, temp_created

    @staticmethod
    def _format_tweet_footer(username: str, created_at: str) -> str:
        value = (created_at or "").strip()
        if value:
            return f"@{username}\n{value}"
        return f"@{username}"

    @staticmethod
    def _strip_author_profile_links(text: str, username: str) -> str:
        value = (text or "").strip()
        if not value:
            return value

        escaped_username = re.escape(username)
        patterns = [
            rf"https?://(?:www\.|mobile\.)?(?:x|twitter)\.com/{escaped_username}(?:[/?#]\S*)?",
            rf"(?:www\.|mobile\.)?(?:x|twitter)\.com/{escaped_username}(?:[/?#]\S*)?",
        ]
        for pattern in patterns:
            value = re.sub(pattern, "", value, flags=re.IGNORECASE)

        # Remove short Twitter links often appended to tweet text.
        value = re.sub(r"https?://t\.co/[A-Za-z0-9]+", "", value, flags=re.IGNORECASE)

        # Also remove trailing short Twitter links if any remained with punctuation/spaces.
        while True:
            updated = re.sub(r"(?:\s|\n)*(https?://t\.co/[A-Za-z0-9]+)\s*$", "", value, flags=re.IGNORECASE)
            if updated == value:
                break
            value = updated

        value = value.strip()
        value = re.sub(r"[ \t]{2,}", " ", value)
        value = re.sub(r"\n{3,}", "\n\n", value)
        return value

    async def _forward_to_telegram(
        self,
        username: str,
        tweet_id: Any,
        text: str,
        media_urls: List[str],
        created_at: str,
        has_video_media: bool = False,
    ) -> None:
        if not self.bot_client or not forward_to_channel:
            return
        if has_video_media or self._has_video_media_urls(media_urls):
            print(f"[X][SKIP] @{username} contains video media; not forwarding to main channel.")
            return

        clean_text = self.rewriter.clean_footer_text(text)
        if clean_text and (self.rewriter.enabled or self.openrouter_api_key):
            rewritten = await asyncio.to_thread(self._rewrite_tweet_text, clean_text)
            if rewritten:
                clean_text = self.rewriter.clean_footer_text(rewritten)
        clean_text = self._strip_author_profile_links(clean_text, username)
        score_input = clean_text or (text or "")
        hype_score = await asyncio.to_thread(self.rewriter.get_hype_score, score_input)
        send_main = hype_score >= self.min_hype_score
        if not send_main:
            print(
                f"[X][SKIP] @{username} score={hype_score}/10 below thresholds "
                f"main={self.min_hype_score}/10 clean={self.clean_min_hype_score}/10"
            )
            return

        footer = self._format_tweet_footer(username=username, created_at=created_at)
        score_line = f"Hype Score: {hype_score}/10"
        footer_lines = [line.strip() for line in footer.splitlines() if line.strip()]
        username_clean = username.strip().lstrip("@")
        profile_url = f"https://x.com/{username_clean}"
        meta_lines_html: List[str] = []
        if username_clean:
            meta_lines_html.append(
                f'<a href="{html_escape(profile_url, quote=True)}">@{html_escape(username_clean)}</a>'
            )
        for line in footer_lines:
            if line.startswith("@"):
                continue
            meta_lines_html.append(html_escape(line))
        meta_lines_html.append(html_escape(score_line))
        meta_text_plain_lines: List[str] = []
        if username_clean:
            meta_text_plain_lines.append(f"@{username_clean}")
        for line in footer_lines:
            if line.startswith("@"):
                continue
            meta_text_plain_lines.append(line)
        meta_text_plain_lines.append(score_line)
        meta_text_plain = "\n".join([x for x in meta_text_plain_lines if x]).strip()
        meta_html = f"<blockquote>{'\n'.join(meta_lines_html).strip()}</blockquote>" if meta_lines_html else ""
        body_raw = html_escape(clean_text).strip() if clean_text else ""
        body_html = f"<pre>{body_raw}</pre>" if body_raw else ""
        # Long messages can be truncated when HTML tags are split across chunks.
        # Use safe plain-text mode for long body text.
        use_safe_plain_mode = len(clean_text or "") > 3000
        if use_safe_plain_mode:
            full_payload = f"{clean_text}\n\n{meta_text_plain}".strip() if clean_text else meta_text_plain
            payload_parse_mode: Optional[str] = None
        elif body_html and meta_html:
            full_payload = f"{body_html}\n\n{meta_html}"
            payload_parse_mode = "html"
        else:
            full_payload = body_html or meta_html
            payload_parse_mode = "html"

        if media_urls and len(full_payload) > 1024:
            print(f"[X][SKIP] @{username} caption would exceed 1024 chars; not forwarding to main channel.")
            return

        temp_media_paths = await asyncio.to_thread(self._download_media_urls_to_temp, media_urls)
        try:
            publish_buttons = None
            publish_token = None
            comment_text = await asyncio.to_thread(self._generate_reply_comment, clean_text or text or "", username)
            comment_url = self._build_x_reply_url(tweet_id=tweet_id, reply_text=comment_text)
            if not comment_url:
                comment_url = self._build_tweet_url(username=username, tweet_id=tweet_id)
            if self.clean_forward_channel or comment_url:
                publish_token = uuid.uuid4().hex[:16]
                x_compose_url = self._build_x_compose_url((clean_text or "").strip(), media_urls)
                row = []
                if self.clean_forward_channel and self.clean_forward_channel != forward_to_channel:
                    row.append(Button.inline("TELEGRAM", data=f"pub:{publish_token}".encode("utf-8")))
                row.append(Button.url("X", x_compose_url))
                if comment_url:
                    row.append(Button.url("Comment", comment_url))
                publish_buttons = [row] if row else None

            main_message_id = await self._send_to_channel_media_first(
                target_channel=forward_to_channel,
                text=full_payload,
                media_paths=temp_media_paths,
                parse_mode=payload_parse_mode,
                buttons=None,
            )
            print(f"[X][FORWARDED] @{username} -> {forward_to_channel}")

            if publish_token and main_message_id:
                button_msg = await self.bot_client.send_message(
                    forward_to_channel,
                    "\u2063",
                    buttons=publish_buttons,
                    reply_to=main_message_id,
                )
                self._publish_jobs[publish_token] = {
                    "channel": forward_to_channel,
                    "message_id": main_message_id,
                    "button_message_id": int(getattr(button_msg, "id", 0) or 0),
                    "username": username,
                    "hype_score": hype_score,
                    "clean_channel": self.clean_forward_channel,
                    "published": False,
                    "created_ts": time.time(),
                }
                print(f"[X][INFO] Publish button attached for @{username} (token={publish_token}).")
                self._main_post_cleanup_jobs[publish_token] = {
                    "channel": forward_to_channel,
                    "message_id": main_message_id,
                    "button_message_id": int(getattr(button_msg, "id", 0) or 0),
                    "created_ts": time.time(),
                }
            elif main_message_id:
                cleanup_id = uuid.uuid4().hex[:16]
                self._main_post_cleanup_jobs[cleanup_id] = {
                    "channel": forward_to_channel,
                    "message_id": main_message_id,
                    "button_message_id": 0,
                    "created_ts": time.time(),
                }
        except Exception as exc:
            print(f"[X][ERROR] Forwarding tweet failed @{username}: {exc}")
        finally:
            for path in temp_media_paths:
                try:
                    os.remove(path)
                except OSError:
                    pass

    async def _forward_clean_copy(
        self,
        username: str,
        target_channel: str,
        text: str,
        media_paths: List[str],
    ) -> None:
        if not self.bot_client or not target_channel:
            return
        def _is_video_file(path: str) -> bool:
            lp = (path or "").lower()
            return lp.endswith(".mp4") or lp.endswith(".webm") or lp.endswith(".mov") or lp.endswith(".mkv")
        try:
            if media_paths:
                remaining = text
                caption_sent = False
                contains_video = any(_is_video_file(p) for p in media_paths)

                if not contains_video:
                    for batch in self._chunks(media_paths, 10):
                        prepared_batch, temp_fixed_paths = self._prepare_image_batch_for_album(batch)
                        caption = None
                        if not caption_sent and remaining:
                            caption = remaining[:1024]
                            remaining = remaining[1024:].lstrip()
                            caption_sent = True
                        try:
                            await self.bot_client.send_file(target_channel, prepared_batch, caption=caption)
                        except Exception as media_exc:
                            if "Failure while processing image" in str(media_exc):
                                print("[X][WARN] Clean-channel image album failed; dropping this batch.")
                            else:
                                raise
                        finally:
                            for p in temp_fixed_paths:
                                try:
                                    os.remove(p)
                                except OSError:
                                    pass
                else:
                    for path in media_paths:
                        caption = None
                        if not caption_sent and remaining:
                            caption = remaining[:1024]
                            remaining = remaining[1024:].lstrip()
                            caption_sent = True
                        try:
                            await self.bot_client.send_file(
                                target_channel,
                                path,
                                caption=caption,
                                supports_streaming=_is_video_file(path),
                            )
                        except Exception as media_exc:
                            if "Failure while processing image" in str(media_exc):
                                fixed = self._convert_image_to_jpeg_temp(path)
                                if fixed:
                                    try:
                                        await self.bot_client.send_file(
                                            target_channel,
                                            fixed,
                                            caption=caption,
                                        )
                                    finally:
                                        try:
                                            os.remove(fixed)
                                        except OSError:
                                            pass
                                else:
                                    print("[X][WARN] Dropping unsupported clean-channel media.")
                            else:
                                raise

                if remaining:
                    for chunk in self._split_text(remaining):
                        await self.bot_client.send_message(target_channel, chunk)
            elif text:
                for chunk in self._split_text(text):
                    await self.bot_client.send_message(target_channel, chunk)

            print(f"[X][FORWARDED-CLEAN] @{username} -> {target_channel}")
        except Exception as exc:
            print(f"[X][ERROR] Clean forward failed @{username} -> {target_channel}: {exc}")

    async def _on_publish_click(self, event: events.CallbackQuery.Event) -> None:
        if not self.bot_client:
            return
        data = (event.data or b"").decode("utf-8", errors="ignore")
        if not data.startswith("pub:"):
            return
        token = data.split(":", 1)[1].strip()
        if not token:
            await event.answer("Invalid publish token.", alert=True)
            return
        job = self._publish_jobs.get(token)
        if not job:
            await event.answer("Publish data expired. Repost latest tweet.", alert=True)
            return
        if job.get("published"):
            await event.answer("Already published.", alert=True)
            return

        hype_score = int(job.get("hype_score") or 0)

        source_channel = str(job.get("channel") or forward_to_channel)
        clean_channel = str(job.get("clean_channel") or "")
        message_id = int(job.get("message_id") or 0)
        username = str(job.get("username") or "")
        if not clean_channel or not message_id:
            await event.answer("Missing publish target.", alert=True)
            return

        try:
            source_msg = await self.bot_client.get_messages(source_channel, ids=message_id)
            if isinstance(source_msg, list):
                source_msg = source_msg[0] if source_msg else None
            if not source_msg:
                await event.answer("Source message not found.", alert=True)
                return

            text_value = (getattr(source_msg, "raw_text", None) or getattr(source_msg, "text", None) or "").strip()
            clean_text = self._strip_publish_meta_lines(text_value)
            if clean_text:
                clean_text = await asyncio.to_thread(
                    self.rewriter._ensure_three_hashtags,
                    clean_text,
                    clean_text,
                )

            media_paths: List[str] = []
            grouped_id = getattr(source_msg, "grouped_id", None)
            if grouped_id:
                from_id = max(1, int(source_msg.id) - 30)
                to_id = int(source_msg.id) + 30
                near_ids = list(range(from_id, to_id + 1))
                near_msgs = await self.bot_client.get_messages(source_channel, ids=near_ids)
                if not isinstance(near_msgs, list):
                    near_msgs = [near_msgs] if near_msgs else []
                media_msgs = [
                    m for m in near_msgs
                    if m is not None and getattr(m, "grouped_id", None) == grouped_id and getattr(m, "media", None)
                ]
                media_msgs = sorted(media_msgs, key=lambda m: int(m.id))
            else:
                media_msgs = [source_msg] if getattr(source_msg, "media", None) else []

            for m in media_msgs:
                path = await self.bot_client.download_media(m, file=tempfile.gettempdir())
                if isinstance(path, str) and os.path.exists(path):
                    media_paths.append(path)

            await self._forward_clean_copy(
                username=username,
                target_channel=clean_channel,
                text=clean_text,
                media_paths=media_paths,
            )
            job["published"] = True
            await event.answer("Published to clean channel.", alert=False)
        except Exception as exc:
            print(f"[X][ERROR] Publish callback failed token={token}: {exc}")
            await event.answer("Publish failed. Check logs.", alert=True)
        finally:
            for p in locals().get("media_paths", []):
                try:
                    if isinstance(p, str) and os.path.exists(p):
                        os.remove(p)
                except OSError:
                    pass

    async def _cleanup_expired_unpublished_posts(self) -> None:
        if not self.bot_client or not self._publish_jobs:
            return
        now = time.time()
        expired_tokens: List[str] = []
        for token, job in list(self._publish_jobs.items()):
            if job.get("published"):
                continue
            created_ts = float(job.get("created_ts") or 0.0)
            if not created_ts or (now - created_ts) < self.unpublished_ttl_seconds:
                continue

            channel = str(job.get("channel") or forward_to_channel)
            msg_ids: List[int] = []
            main_id = int(job.get("message_id") or 0)
            btn_id = int(job.get("button_message_id") or 0)
            if main_id > 0:
                msg_ids.append(main_id)
            if btn_id > 0 and btn_id != main_id:
                msg_ids.append(btn_id)

            try:
                if msg_ids:
                    await self.bot_client.delete_messages(channel, msg_ids)
                expired_tokens.append(token)
                print(f"[X][CLEANUP] Deleted unpublished post token={token} after 24h.")
            except Exception as exc:
                print(f"[X][WARN] Failed to delete expired unpublished post token={token}: {exc}")

        for token in expired_tokens:
            self._publish_jobs.pop(token, None)

    async def _cleanup_expired_main_posts(self) -> None:
        if not self.bot_client or not self._main_post_cleanup_jobs:
            return
        now = time.time()
        expired_keys: List[str] = []
        for key, job in list(self._main_post_cleanup_jobs.items()):
            created_ts = float(job.get("created_ts") or 0.0)
            if not created_ts or (now - created_ts) < self.main_post_ttl_seconds:
                continue

            channel = str(job.get("channel") or forward_to_channel)
            msg_ids: List[int] = []
            main_id = int(job.get("message_id") or 0)
            btn_id = int(job.get("button_message_id") or 0)
            if main_id > 0:
                msg_ids.append(main_id)
            if btn_id > 0 and btn_id != main_id:
                msg_ids.append(btn_id)

            try:
                # If main message belongs to a media group, delete the whole group.
                if main_id > 0:
                    source_msg = await self.bot_client.get_messages(channel, ids=main_id)
                    if isinstance(source_msg, list):
                        source_msg = source_msg[0] if source_msg else None
                    grouped_id = getattr(source_msg, "grouped_id", None) if source_msg else None
                    if grouped_id:
                        from_id = max(1, int(main_id) - 30)
                        to_id = int(main_id) + 30
                        near_ids = list(range(from_id, to_id + 1))
                        near_msgs = await self.bot_client.get_messages(channel, ids=near_ids)
                        if not isinstance(near_msgs, list):
                            near_msgs = [near_msgs] if near_msgs else []
                        for m in near_msgs:
                            if m is not None and getattr(m, "grouped_id", None) == grouped_id:
                                mid = int(getattr(m, "id", 0) or 0)
                                if mid > 0:
                                    msg_ids.append(mid)

                if msg_ids:
                    unique_ids = sorted(set(msg_ids))
                    await self.bot_client.delete_messages(channel, unique_ids)
                print(f"[X][CLEANUP] Deleted main forwarded post key={key} after 1h.")
            except Exception as exc:
                print(f"[X][WARN] Failed to delete 1h-expired main post key={key}: {exc}")
            finally:
                expired_keys.append(key)

        for key in expired_keys:
            self._main_post_cleanup_jobs.pop(key, None)
            self._publish_jobs.pop(key, None)

    async def _fetch_user_tweets(self, username: str) -> List[Any]:
        if self.twikit_only:
            tweets = await self._collect_user_tweets_twikit(username)
        elif self.api is None:
            tweets = []
        else:
            try:
                user = await self.api.user_by_login(username)
                if not user:
                    raise RuntimeError("User not found in twscrape")
                user_id = getattr(user, "id", None)
                if user_id is None:
                    raise RuntimeError("Missing user id in twscrape")
                tweets = []
                async for tweet in self.api.user_tweets(user_id, limit=self.fetch_limit):
                    tweets.append(tweet)
            except Exception as exc:
                print(f"[X][WARN] twscrape failed for @{username}, trying twikit fallback: {exc}")
                tweets = await self._collect_user_tweets_twikit(username)
        return tweets

    async def _collect_user_tweets(self, username: str) -> None:
        tweets = await self._fetch_user_tweets(username)
        if not tweets:
            print(f"[X][WARN] No tweets fetched for @{username}.")
            return

        current_seen = self.last_seen_tweet_id.get(username, 0)
        newest_tweet: Optional[Any] = None
        newest_id = current_seen
        for tweet in tweets:
            tid = _safe_int(getattr(tweet, "id", 0))
            if tid > newest_id:
                newest_id = tid
                newest_tweet = tweet

        if newest_tweet is None:
            return

        self.last_seen_tweet_id[username] = newest_id
        inserted_any = False

        for tweet in [newest_tweet]:
            message_id = _safe_int(getattr(tweet, "id", 0))
            if not message_id:
                continue

            text = (getattr(tweet, "rawContent", None) or getattr(tweet, "content", None) or "").strip()
            if not text:
                text = (getattr(tweet, "text", None) or "").strip()
            created_at = _to_iso(getattr(tweet, "date", None))
            if created_at == _to_iso(None):
                created_at = _to_iso(getattr(tweet, "created_at", None))
            media_urls = self._extract_media_urls(tweet)
            has_video_media = self._tweet_has_video(tweet)
            media_path = "|".join(media_urls) if media_urls else None

            inserted = self.db.insert_post(
                source="twitter",
                channel=f"@{username}",
                message_id=message_id,
                text=text,
                media_path=media_path,
                created_at=created_at,
            )
            if inserted:
                inserted_any = True
                print(
                    f"[X][NEW] user=@{username} tweet_id={message_id} "
                    f"media={len(media_urls)}"
                )
                await self._forward_to_telegram(
                    username=username,
                    tweet_id=message_id,
                    text=text,
                    media_urls=media_urls,
                    created_at=created_at,
                    has_video_media=has_video_media,
                )

        if not inserted_any:
            print(f"[X][INFO] Latest tweet already seen for @{username}.")

    async def _collect_user_tweets_twikit(self, username: str) -> List[Any]:
        if self._twikit_disabled():
            return self._collect_user_tweets_snscrape(username)

        if not await self._ensure_twikit_client():
            return []
        assert self.twikit_client is not None
        try:
            user = await self.twikit_client.get_user_by_screen_name(username)
            user_id = getattr(user, "id", None)
            if user_id is None:
                return []
            tweets = await self.twikit_client.get_user_tweets(user_id, "Tweets", count=self.fetch_limit)
            return list(tweets or [])
        except Exception as exc:
            msg = str(exc)
            if "KEY_BYTE" in msg:
                print(f"[X][WARN] twikit profile fetch failed for @{username} (KEY_BYTE). Trying search fallback.")
                try:
                    try:
                        tweets = await self.twikit_client.search_tweet(
                            query=f"from:{username}",
                            product="Latest",
                            count=self.fetch_limit,
                        )
                    except TypeError:
                        tweets = await self.twikit_client.search_tweet(
                            f"from:{username}",
                            "Latest",
                            self.fetch_limit,
                        )
                    return list(tweets or [])
                except Exception as search_exc:
                    print(f"[X][ERROR] twikit search fallback failed for @{username}: {search_exc}")
                    search_msg = str(search_exc).lower()
                    if (
                        "maximum recursion depth" in search_msg
                        or "rate limit" in search_msg
                        or "status: 429" in search_msg
                        or "clienttransaction" in search_msg
                    ):
                        self._disable_twikit_temporarily(1800, "twikit search error")
                    return self._collect_user_tweets_snscrape(username)

            print(f"[X][ERROR] twikit fallback failed for @{username}: {exc}")
            lower_msg = msg.lower()
            if (
                "maximum recursion depth" in lower_msg
                or "rate limit" in lower_msg
                or "status: 429" in lower_msg
                or "clienttransaction" in lower_msg
            ):
                self._disable_twikit_temporarily(1800, "twikit request error")
            return self._collect_user_tweets_snscrape(username)

    def _collect_user_tweets_snscrape(self, username: str) -> List[Any]:
        if sntwitter is None:
            if not self._snscrape_unavailable_logged:
                self._snscrape_unavailable_logged = True
                print("[X][INFO] snscrape fallback unavailable in this Python; using Nitter RSS fallback.")
            return self._collect_user_tweets_nitter(username)
        try:
            results: List[Any] = []
            scraper = sntwitter.TwitterUserScraper(username)
            for i, tweet in enumerate(scraper.get_items()):
                results.append(tweet)
                if i + 1 >= self.fetch_limit:
                    break
            if results:
                print(f"[X][INFO] snscrape fallback fetched {len(results)} tweets for @{username}.")
            return results
        except Exception as exc:
            print(f"[X][ERROR] snscrape fallback failed for @{username}: {exc}")
            return self._collect_user_tweets_nitter(username)

    def _collect_user_tweets_nitter(self, username: str) -> List[Any]:
        urls = [
            f"https://nitter.net/{username}/rss",
            f"https://nitter.privacydev.net/{username}/rss",
            f"https://nitter.poast.org/{username}/rss",
        ]

        class _RssTweet:
            def __init__(self, tweet_id: int, text: str, date_value: datetime, media: List[str]):
                self.id = tweet_id
                self.rawContent = text
                self.date = date_value
                self.media = media

        for rss_url in urls:
            req = request.Request(rss_url, headers={"User-Agent": "Mozilla/5.0"})
            try:
                with request.urlopen(req, timeout=20) as resp:
                    raw_xml = resp.read()
            except Exception:
                continue

            try:
                root = ET.fromstring(raw_xml)
            except ET.ParseError:
                continue

            channel = root.find("channel")
            if channel is None:
                continue

            out: List[Any] = []
            for item in channel.findall("item"):
                link = (item.findtext("link") or "").strip()
                title = (item.findtext("title") or "").strip()
                pub_date = (item.findtext("pubDate") or "").strip()

                match = re.search(r"/status/(\d+)", link)
                if not match:
                    continue
                tweet_id = _safe_int(match.group(1))
                if not tweet_id:
                    continue

                try:
                    dt = parsedate_to_datetime(pub_date) if pub_date else datetime.utcnow()
                except Exception:
                    dt = datetime.utcnow()

                # Nitter title often starts with "DisplayName: ..."; keep only tweet content part.
                cleaned_title = re.sub(r"^[^:]{1,60}:\s*", "", title).strip() or title
                media_urls: List[str] = []

                # enclosure URLs (if present)
                for enc in item.findall("enclosure"):
                    url = (enc.attrib.get("url") or "").strip()
                    if url.startswith("http"):
                        media_urls.append(url)

                # image/video URLs often appear in description HTML.
                description = (item.findtext("description") or "").strip()
                if description:
                    for candidate in re.findall(r"https?://[^\s\"'<>]+", description):
                        c = candidate.strip()
                        if any(x in c.lower() for x in ("pbs.twimg.com", "video.twimg.com", "twimg.com")):
                            media_urls.append(c)
                        elif re.search(r"\.(jpg|jpeg|png|webp|gif|mp4)(\?|$)", c, flags=re.IGNORECASE):
                            media_urls.append(c)

                # unique preserve order
                cleaned_media: List[str] = []
                seen_media = set()
                for m in media_urls:
                    if m in seen_media:
                        continue
                    seen_media.add(m)
                    cleaned_media.append(m)

                out.append(_RssTweet(tweet_id=tweet_id, text=cleaned_title, date_value=dt, media=cleaned_media))
                if len(out) >= self.fetch_limit:
                    break

            if out:
                print(f"[X][INFO] Nitter RSS fallback fetched {len(out)} tweets for @{username}.")
                return out

        print(f"[X][WARN] Nitter RSS fallback failed for @{username}.")
        return []

    async def run(self) -> None:
        if not self.enabled:
            print("[X][INFO] Twitter collector disabled.")
            return
        if not self._deps_ok:
            print("[X][WARN] Twitter collector disabled: install twscrape and twikit first.")
            return

        if twikit is not None:
            ver = getattr(twikit, "__version__", "unknown")
            print(f"[X][INFO] Twikit version: {ver}")
            print(f"[X][INFO] Twikit transaction patch: {'applied' if _TWIKIT_PATCH_APPLIED else 'not applied'}")

        twikit_ready = await self._ensure_twikit_client()
        if not twikit_ready:
            if sntwitter is None:
                print("[X][WARN] Twikit auth failed and snscrape not installed. Twitter collector stopped.")
                return
            print("[X][WARN] Twikit auth failed. Continuing with snscrape fallback mode.")

        if not await self._bootstrap_twscrape_account():
            print("[X][WARN] Twitter collector could not authenticate.")
            return

        await self._start_forward_bot()

        # Baseline: do not forward historical tweets on startup.
        for username in self.usernames:
            try:
                tweets = await self._fetch_user_tweets(username)
                latest_id = 0
                for tweet in tweets:
                    latest_id = max(latest_id, _safe_int(getattr(tweet, "id", 0)))
                self.last_seen_tweet_id[username] = latest_id
                if latest_id:
                    print(f"[X][INFO] Baseline set @{username} last_tweet_id={latest_id}")
            except Exception as exc:
                print(f"[X][WARN] Baseline init failed for @{username}: {exc}")

        self._ready = True
        print(
            f"[X][INFO] Collector started. Users={', '.join('@' + u for u in self.usernames)} "
            f"poll={self.poll_seconds}s mode={'twikit-only' if self.twikit_only else 'hybrid'}"
        )

        while True:
            await self._cleanup_expired_main_posts()
            for username in self.usernames:
                await self._collect_user_tweets(username)
            await self._cleanup_expired_main_posts()
            await asyncio.sleep(self.poll_seconds)


def run_twitter_collector_in_background(db: Database) -> Optional[threading.Thread]:
    if not twitter_enabled:
        return None

    collector = TwitterCollector(db)

    def _runner() -> None:
        try:
            asyncio.run(collector.run())
        except Exception as exc:
            print(f"[X][ERROR] Collector crashed: {exc}")

    thread = threading.Thread(target=_runner, name="twitter-collector", daemon=True)
    thread.start()
    return thread
