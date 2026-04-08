import asyncio
import hashlib
import json
import os
import threading
from datetime import datetime
from html import escape as html_escape
import re
import time
import uuid
from typing import Any, Dict, List, Optional
from urllib import error, parse, request

from telethon import Button, TelegramClient, events

from config import (
    api_hash,
    api_id,
    bluesky_clean_forward_channel,
    bluesky_clean_min_hype_score,
    bluesky_enabled,
    bluesky_fetch_limit,
    bluesky_identifiers,
    bluesky_min_hype_score,
    bluesky_poll_seconds,
    bluesky_bot_session_name,
    bot_token,
    forward_to_channel,
    forwarding_enabled,
)
from db import Database
from twitter_collector import TwitterCollector


class BlueskyCollector(TwitterCollector):
    PUBLIC_API_BASE = "https://public.api.bsky.app/xrpc"

    def __init__(self, db: Database):
        super().__init__(db)
        self.enabled = bluesky_enabled and bool(bluesky_identifiers)
        self.identifiers = bluesky_identifiers
        self.usernames: List[str] = []
        self.poll_seconds = max(30, bluesky_poll_seconds)
        self.fetch_limit = max(1, bluesky_fetch_limit)
        self.min_hype_score = min(10, max(5, bluesky_min_hype_score))
        self.clean_min_hype_score = min(10, max(5, bluesky_clean_min_hype_score))
        self.clean_forward_channel = bluesky_clean_forward_channel
        self.last_seen_post_uri: Dict[str, str] = {}
        self._deps_ok = True

    def _api_get_json(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        query = parse.urlencode({k: v for k, v in params.items() if v not in (None, "")})
        url = f"{self.PUBLIC_API_BASE}/{endpoint}"
        if query:
            url = f"{url}?{query}"
        req = request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))

    async def _start_forward_bot(self) -> None:
        if not forwarding_enabled or not forward_to_channel or not bot_token:
            print("[B][WARN] Bluesky -> Telegram forwarding disabled by config.")
            return
        try:
            self.bot_client = TelegramClient(bluesky_bot_session_name, api_id, api_hash)
            await self.bot_client.start(bot_token=bot_token)
            self.bot_client.add_event_handler(self._on_publish_click, events.CallbackQuery(pattern=b"^pub:"))
            print(f"[B][INFO] Bluesky forwarding target: {forward_to_channel}")
            if self.comment_ai_provider == "openrouter" and self.openrouter_api_key:
                print(f"[B][INFO] Comment AI provider: OpenRouter ({self.openrouter_model})")
            else:
                print(f"[B][INFO] Comment AI provider: Gemini fallback ({self.rewriter.model})")
        except Exception as exc:
            self.bot_client = None
            print(f"[B][ERROR] Failed to start Bluesky forward bot: {exc}")

    @staticmethod
    def _normalize_identifier(value: str) -> str:
        return re.sub(r"[^a-z0-9]", "", (value or "").lower())

    def _resolve_handle(self, identifier: str) -> str:
        raw = (identifier or "").strip().lstrip("@")
        if not raw:
            return ""
        try:
            if "." in raw:
                profile = self._api_get_json("app.bsky.actor.getProfile", {"actor": raw})
                handle = str(profile.get("handle") or "").strip()
                return handle
        except Exception:
            pass

        try:
            data = self._api_get_json("app.bsky.actor.searchActorsTypeahead", {"q": raw, "limit": 10})
        except Exception as exc:
            print(f"[B][WARN] Bluesky search failed for '{raw}': {exc}")
            return ""

        actors = data.get("actors") or []
        if not actors:
            return ""

        ident_norm = self._normalize_identifier(raw)
        best_handle = ""
        best_score = -1
        for actor in actors:
            handle = str(actor.get("handle") or "").strip()
            display_name = str(actor.get("displayName") or "").strip()
            handle_norm = self._normalize_identifier(handle.split(".", 1)[0])
            display_norm = self._normalize_identifier(display_name)
            score = 0
            if handle.lower() == raw.lower():
                score += 100
            if handle_norm == ident_norm:
                score += 80
            if display_norm == ident_norm:
                score += 60
            if ident_norm and ident_norm in handle_norm:
                score += 20
            if ident_norm and ident_norm in display_norm:
                score += 10
            if score > best_score:
                best_score = score
                best_handle = handle
        return best_handle if best_score >= 20 else ""

    async def _bootstrap_handles(self) -> None:
        resolved: List[str] = []
        for identifier in self.identifiers:
            handle = await asyncio.to_thread(self._resolve_handle, identifier)
            if handle:
                if handle.lower() not in {x.lower() for x in resolved}:
                    resolved.append(handle)
                    print(f"[B][INFO] Resolved {identifier} -> {handle}")
            else:
                print(f"[B][WARN] No Bluesky handle found for {identifier}. Skipped.")
        self.usernames = resolved

    def _fetch_author_feed(self, handle: str) -> List[Dict[str, Any]]:
        try:
            data = self._api_get_json(
                "app.bsky.feed.getAuthorFeed",
                {"actor": handle, "limit": self.fetch_limit},
            )
            return list(data.get("feed") or [])
        except Exception as exc:
            print(f"[B][WARN] Feed fetch failed for {handle}: {exc}")
            return []

    @staticmethod
    def _post_rkey_from_uri(uri: str) -> str:
        value = (uri or "").strip()
        return value.rsplit("/", 1)[-1] if value else ""

    @staticmethod
    def _stable_message_id(uri: str) -> int:
        digest = hashlib.sha1((uri or "").encode("utf-8")).hexdigest()
        return int(digest[:15], 16)

    @staticmethod
    def _extract_record_text(post_view: Dict[str, Any]) -> str:
        record = post_view.get("record") or {}
        return str(record.get("text") or "").strip()

    def _extract_bluesky_media(self, post_view: Dict[str, Any]) -> tuple[List[str], bool]:
        urls: List[str] = []
        has_video = False

        def _walk(node: Any) -> None:
            nonlocal has_video
            if isinstance(node, dict):
                node_type = str(node.get("$type") or "").lower()
                if "video" in node_type:
                    has_video = True
                for key in ("fullsize", "thumb"):
                    val = str(node.get(key) or "").strip()
                    if val.startswith("http") and val not in urls:
                        urls.append(val)
                for value in node.values():
                    _walk(value)
            elif isinstance(node, list):
                for item in node:
                    _walk(item)

        _walk(post_view.get("embed") or {})
        return urls, has_video

    @staticmethod
    def _build_bluesky_post_url(handle: str, post_uri: str) -> str:
        rkey = BlueskyCollector._post_rkey_from_uri(post_uri)
        if not handle or not rkey:
            return ""
        return f"https://bsky.app/profile/{handle}/post/{rkey}"

    @staticmethod
    def _strip_author_profile_links(text: str, username: str) -> str:
        value = (text or "").strip()
        if not value:
            return value
        escaped = re.escape(username)
        patterns = [
            rf"https?://bsky\.app/profile/{escaped}(?:[/?#]\S*)?",
            rf"https?://bsky\.app/profile/{escaped}\.bsky\.social(?:[/?#]\S*)?",
        ]
        for pattern in patterns:
            value = re.sub(pattern, "", value, flags=re.IGNORECASE)
        value = re.sub(r"[ \t]{2,}", " ", value)
        value = re.sub(r"\n{3,}", "\n\n", value)
        return value.strip()

    @staticmethod
    def _build_bluesky_compose_url(clean_text: str) -> str:
        text = (clean_text or "").strip()
        if text:
            text = f"{text}\n\nMore updates in Telegram (link in bio)"
        else:
            text = "More updates in Telegram (link in bio)"
        if len(text) > 1200:
            text = text[:1197].rstrip() + "..."
        return f"https://bsky.app/intent/compose?text={parse.quote_plus(text)}"

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
        if has_video_media:
            print(f"[B][SKIP] {username} contains video media; not forwarding to main channel.")
            return

        clean_text = self.rewriter.clean_footer_text(text)
        if clean_text and (self.rewriter.enabled or self.openrouter_api_key):
            rewritten = await asyncio.to_thread(self._rewrite_tweet_text, clean_text)
            if rewritten:
                clean_text = self.rewriter.clean_footer_text(rewritten)
        clean_text = self._strip_author_profile_links(clean_text, username)
        score_input = clean_text or (text or "")
        hype_score = await asyncio.to_thread(self.rewriter.get_hype_score, score_input)
        if hype_score < self.min_hype_score:
            print(f"[B][SKIP] {username} score={hype_score}/10 below main threshold {self.min_hype_score}/10")
            return

        footer_lines = [f"@{username}", created_at, f"Hype Score: {hype_score}/10"]
        meta_lines_html = [
            f'<a href="{html_escape(f"https://bsky.app/profile/{username}", quote=True)}">@{html_escape(username)}</a>',
            html_escape(created_at),
            html_escape(f"Hype Score: {hype_score}/10"),
        ]
        meta_text_plain = "\n".join([x for x in footer_lines if x]).strip()
        meta_html = f"<blockquote>{'\n'.join(meta_lines_html).strip()}</blockquote>"
        body_raw = html_escape(clean_text).strip() if clean_text else ""
        body_html = f"<pre>{body_raw}</pre>" if body_raw else ""
        if body_html and meta_html:
            full_payload = f"{body_html}\n\n{meta_html}"
            payload_parse_mode: Optional[str] = "html"
        else:
            full_payload = body_html or meta_html
            payload_parse_mode = "html"
        if len(clean_text or "") > 3000:
            full_payload = f"{clean_text}\n\n{meta_text_plain}".strip()
            payload_parse_mode = None
        if media_urls and len(full_payload) > 1024:
            print(f"[B][SKIP] {username} caption would exceed 1024 chars; not forwarding to main channel.")
            return

        temp_media_paths = await asyncio.to_thread(self._download_media_urls_to_temp, media_urls)
        try:
            publish_buttons = None
            publish_token = None
            post_url = self._build_bluesky_post_url(username, str(tweet_id))
            if self.clean_forward_channel or post_url:
                publish_token = uuid.uuid4().hex[:16]
                bluesky_compose_url = self._build_bluesky_compose_url((clean_text or "").strip())
                row = []
                if self.clean_forward_channel and self.clean_forward_channel != forward_to_channel:
                    row.append(Button.inline("TELEGRAM", data=f"pub:{publish_token}".encode("utf-8")))
                row.append(Button.url("Bluesky", bluesky_compose_url))
                if post_url:
                    row.append(Button.url("Comment", post_url))
                publish_buttons = [row] if row else None

            main_message_id = await self._send_to_channel_media_first(
                target_channel=forward_to_channel,
                text=full_payload,
                media_paths=temp_media_paths,
                parse_mode=payload_parse_mode,
                buttons=None,
            )
            print(f"[B][FORWARDED] {username} -> {forward_to_channel}")

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
                    "x_compose_url": bluesky_compose_url,
                    "comment_url": post_url,
                    "published": False,
                    "created_ts": time.time(),
                }
                self._main_post_cleanup_jobs[publish_token] = {
                    "channel": forward_to_channel,
                    "message_id": main_message_id,
                    "button_message_id": int(getattr(button_msg, "id", 0) or 0),
                    "created_ts": time.time(),
                }
        finally:
            for path in temp_media_paths:
                try:
                    if path and os.path.exists(path):
                        os.remove(path)
                except Exception:
                    pass

    async def _collect_author_posts(self, handle: str) -> None:
        feed = await asyncio.to_thread(self._fetch_author_feed, handle)
        if not feed:
            print(f"[B][WARN] No posts fetched for {handle}.")
            return

        latest_item = None
        latest_uri = self.last_seen_post_uri.get(handle, "")
        for item in feed:
            if item.get("reason"):
                continue
            post_view = item.get("post") or {}
            author = post_view.get("author") or {}
            author_handle = str(author.get("handle") or "").strip()
            if author_handle.lower() != handle.lower():
                continue
            uri = str(post_view.get("uri") or "").strip()
            if not uri:
                continue
            if uri == latest_uri:
                break
            latest_item = item
            latest_uri = uri
            break

        if not latest_item:
            print(f"[B][INFO] Latest post already seen for {handle}.")
            return

        self.last_seen_post_uri[handle] = latest_uri
        post_view = latest_item.get("post") or {}
        text = self._extract_record_text(post_view)
        created_at = str((post_view.get("record") or {}).get("createdAt") or post_view.get("indexedAt") or datetime.utcnow().isoformat())
        media_urls, has_video_media = self._extract_bluesky_media(post_view)
        media_path = "|".join(media_urls) if media_urls else None
        message_id = self._stable_message_id(latest_uri)

        inserted = self.db.insert_post(
            source="bluesky",
            channel=f"@{handle}",
            message_id=message_id,
            text=text,
            media_path=media_path,
            created_at=created_at,
        )
        if not inserted:
            print(f"[B][INFO] Latest post already stored for {handle}.")
            return

        print(f"[B][NEW] handle={handle} post={latest_uri} media={len(media_urls)}")
        await self._forward_to_telegram(
            username=handle,
            tweet_id=latest_uri,
            text=text,
            media_urls=media_urls,
            created_at=created_at,
            has_video_media=has_video_media,
        )

    async def run(self) -> None:
        if not self.enabled:
            print("[B][INFO] Bluesky collector disabled.")
            return

        await self._start_forward_bot()
        await self._bootstrap_handles()
        if not self.usernames:
            print("[B][WARN] No Bluesky handles resolved. Collector stopped.")
            return

        for handle in self.usernames:
            try:
                feed = await asyncio.to_thread(self._fetch_author_feed, handle)
                for item in feed:
                    post_view = item.get("post") or {}
                    uri = str(post_view.get("uri") or "").strip()
                    if uri:
                        self.last_seen_post_uri[handle] = uri
                        print(f"[B][INFO] Baseline set {handle} last_post={uri}")
                        break
            except Exception as exc:
                print(f"[B][WARN] Baseline init failed for {handle}: {exc}")

        self._ready = True
        print(
            f"[B][INFO] Collector started. Handles={', '.join(self.usernames)} "
            f"poll={self.poll_seconds}s"
        )

        while True:
            await self._cleanup_expired_main_posts()
            for handle in self.usernames:
                await self._collect_author_posts(handle)
            await self._cleanup_expired_main_posts()
            await asyncio.sleep(self.poll_seconds)


def run_bluesky_collector_in_background(db: Database) -> Optional[threading.Thread]:
    if not bluesky_enabled:
        return None

    collector = BlueskyCollector(db)

    def _runner() -> None:
        try:
            asyncio.run(collector.run())
        except Exception as exc:
            print(f"[B][ERROR] Collector crashed: {exc}")

    thread = threading.Thread(target=_runner, name="bluesky-collector", daemon=True)
    thread.start()
    return thread
