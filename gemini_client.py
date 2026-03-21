import json
import os
import re
import tempfile
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Optional
from urllib import error, parse, request


class GeminiRewriter:
    def __init__(self, api_key: str, model: str, timeout_seconds: int = 20):
        self.api_key = api_key.strip()
        self.model = model.strip() or "gemini-1.5-flash"
        self.timeout_seconds = timeout_seconds

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    @staticmethod
    def clean_footer_text(text: str) -> str:
        raw = (text or "").strip()
        if not raw:
            return ""

        lines = [line.strip() for line in raw.splitlines()]
        cleaned: List[str] = []
        footer_token_pattern = re.compile(
            r"^(news|markets?|youtube|watch|analysis|alerts?|x|twitter|telegram|website|app)$",
            re.IGNORECASE,
        )

        for line in lines:
            if not line:
                if cleaned and cleaned[-1]:
                    cleaned.append("")
                continue

            normalized = line.replace("вЂў", "|").replace("В·", "|")
            parts = [p.strip() for p in normalized.split("|") if p.strip()]
            if len(parts) >= 2 and all(footer_token_pattern.match(part) for part in parts):
                continue

            cleaned.append(line)

        while cleaned and cleaned[-1] == "":
            cleaned.pop()
        return "\n".join(cleaned).strip()

    def _api_url(self) -> str:
        return (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"{self.model}:generateContent?key={self.api_key}"
        )

    def _generate_text(
        self,
        prompt: str,
        enable_google_search: bool = False,
        temperature: float = 0.35,
    ) -> Optional[str]:
        if not self.enabled:
            return None

        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": temperature, "maxOutputTokens": 1200},
        }
        if enable_google_search:
            payload["tools"] = [{"google_search": {}}]

        req = request.Request(
            url=self._api_url(),
            method="POST",
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload).encode("utf-8"),
        )

        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except (error.URLError, error.HTTPError, TimeoutError, json.JSONDecodeError):
            return None

        for candidate in data.get("candidates") or []:
            content = candidate.get("content") or {}
            for part in content.get("parts") or []:
                text = (part.get("text") or "").strip()
                if text:
                    return text
        return None

    def rewrite(self, text: str) -> str:
        original = self.clean_footer_text(text)
        if not original or not self.enabled:
            return original

        prompt = (
            "Rewrite this Telegram post by changing wording and sentence structure. "
            "Keep exact meaning, facts, links, and emojis. "
            "Do not add or remove facts. "
            "Do not change the core message. "
            "If original post starts with a headline-style lead word (for example IMPORTANT/BREAKING/LATEST), preserve that lead word. "
            "At the very top, ensure a one-word uppercase lead label with an emoji in this format: "
            "EMOJI WORD:, then one empty line, then the post body. "
            "Do not include source/footer labels. "
            "At the end, add exactly 3 SEO-optimized hashtags relevant to the topic. "
            "Return only rewritten post text.\n\n"
            f"Post:\n{original}"
        )
        rewritten = self._generate_text(prompt, temperature=0.6)
        result = self.clean_footer_text((rewritten or "").strip()) or original

        similarity = SequenceMatcher(a=original, b=result).ratio()

        if similarity > 0.86:
            second = self._generate_text(
                "Paraphrase more strongly while preserving exact meaning, links, emojis, and facts. "
                "Ensure noticeably different wording and sentence structure. "
                "If there is a lead label, use BREAKING or LATEST, not KEY. "
                "Return only paraphrased text.\n\n"
                f"Post:\n{original}",
                temperature=0.85,
            )
            if second:
                result = self.clean_footer_text(second.strip()) or result
            similarity = SequenceMatcher(a=original, b=result).ratio()

        # Still too close => one aggressive paraphrase pass.
        if similarity > 0.82:
            third = self._generate_text(
                "Rewrite with strong wording changes while preserving all facts and links exactly. "
                "Use a different sentence flow from the source. "
                "Do not copy source phrases except proper nouns, numbers, or links. "
                "Return only the final rewritten text.\n\n"
                f"Source:\n{original}",
                temperature=0.95,
            )
            if third:
                result = self.clean_footer_text(third.strip()) or result
            similarity = SequenceMatcher(a=original, b=result).ratio()

        # Too close after retries => deterministic fallback rewrite.
        if similarity > 0.84:
            result = self._fallback_paraphrase(result)
            similarity = SequenceMatcher(a=original, b=result).ratio()

        # Too far => potential meaning drift, fallback to safer rewrite.
        if similarity < 0.45:
            safer = self._generate_text(
                "Rewrite this text conservatively: preserve exact meaning and all facts, "
                "change wording only slightly. Return only rewritten text.\n\n"
                f"Text:\n{original}",
                temperature=0.2,
            )
            if safer:
                result = self.clean_footer_text(safer.strip()) or result

        result = self._normalize_lead_label(result)
        result = self._ensure_lead_banner_block(result, original)
        return self._ensure_three_hashtags(result, original)

    def get_hype_score(self, text: str) -> int:
        cleaned = self.clean_footer_text(text or "")
        if not cleaned or not self.enabled:
            return 5

        prompt = (
            "Rate how hype/newsworthy this post is on a scale from 1 to 10. "
            "Use 1 for low-impact updates and 10 for major market-moving breaking news. "
            "Return only one integer from 1 to 10.\n\n"
            f"Post:\n{cleaned}"
        )
        raw = (self._generate_text(prompt, temperature=0.2) or "").strip()
        match = re.search(r"\b([1-9]|10)\b", raw)
        if not match:
            return 5
        value = int(match.group(1))
        return max(1, min(10, value))

    @staticmethod
    def _fallback_paraphrase(text: str) -> str:
        value = (text or "").strip()
        if not value:
            return value
        replacements = {
            "important": "breaking",
            "released": "published",
            "clarify": "provide clarity on",
            "specifically": "in particular",
            "addressing": "covering",
            "used as": "serving as",
            "new": "latest",
            "announced": "stated",
            "key": "breaking",
        }
        out = value
        for src, dst in replacements.items():
            out = re.sub(rf"\b{re.escape(src)}\b", dst, out, flags=re.IGNORECASE)
        return GeminiRewriter._normalize_lead_label(out)

    @staticmethod
    def _normalize_lead_label(text: str) -> str:
        value = (text or "").strip()
        if not value:
            return value

        # Normalize lead markers like "IMPORTANT:" / "KEY:" to BREAKING/LATEST.
        patterns = [
            r"^(?P<prefix>(?:[\U0001F1E6-\U0001F1FF]{2}\s*)?)(important|key)\s*:\s*",
            r"^(?P<prefix>(?:[\U0001F1E6-\U0001F1FF]{2}\s*)?)(important|key)\s*-\s*",
        ]
        for pattern in patterns:
            if re.search(pattern, value, flags=re.IGNORECASE):
                value = re.sub(pattern, r"\g<prefix>BREAKING: ", value, flags=re.IGNORECASE)
                break

        # If it starts with lowercase "breaking", normalize casing and separator.
        value = re.sub(r"^((?:[\U0001F1E6-\U0001F1FF]{2}\s*)?)breaking\s*[:\-]?\s*", r"\1BREAKING: ", value, flags=re.IGNORECASE)
        value = re.sub(r"^((?:[\U0001F1E6-\U0001F1FF]{2}\s*)?)latest\s*[:\-]?\s*", r"\1LATEST: ", value, flags=re.IGNORECASE)
        return value

    @staticmethod
    def _extract_hashtags(text: str) -> List[str]:
        tags = re.findall(r"(?<!\w)#([A-Za-z0-9_]{2,50})", text or "")
        unique: List[str] = []
        seen = set()
        for tag in tags:
            key = tag.lower()
            if key in seen:
                continue
            seen.add(key)
            unique.append(f"#{tag}")
        return unique

    def _generate_hashtags(self, source_text: str, count: int = 3) -> List[str]:
        raw = self._generate_text(
            f"Generate exactly {count} SEO-optimized hashtags for this post topic. "
            "Return hashtags only separated by spaces.\n\n"
            f"Post:\n{source_text}"
        )
        return self._extract_hashtags(raw or "")[:count]

    @staticmethod
    def _keyword_fallback_hashtags(source_text: str, count: int = 3) -> List[str]:
        value = (source_text or "").strip()
        if not value:
            return []
        # Remove links/hashtags and keep alnum words.
        value = re.sub(r"https?://\S+", " ", value)
        value = re.sub(r"#\w+", " ", value)
        words = re.findall(r"[A-Za-z][A-Za-z0-9]{2,24}", value)
        stop = {
            "the", "and", "for", "with", "that", "this", "from", "into", "about", "have",
            "has", "was", "were", "are", "will", "just", "now", "over", "under", "their",
            "your", "you", "its", "after", "before", "than", "then", "they", "them",
            "what", "when", "where", "which", "while", "also", "more", "most", "very",
            "news", "latest", "breaking", "update", "important", "crypto", "market",
        }
        freq: Dict[str, int] = {}
        for w in words:
            lw = w.lower()
            if lw in stop:
                continue
            freq[lw] = freq.get(lw, 0) + 1

        ranked = sorted(freq.items(), key=lambda x: (-x[1], -len(x[0]), x[0]))
        tags: List[str] = []
        for word, _ in ranked:
            tag = f"#{word[:1].upper()}{word[1:]}"
            if tag.lower() not in {t.lower() for t in tags}:
                tags.append(tag)
            if len(tags) >= count:
                break
        return tags[:count]

    def _strip_existing_hashtags(self, text: str) -> str:
        lines = (text or "").splitlines()
        kept: List[str] = []
        for line in lines:
            tokens = line.strip().split()
            if tokens and all(token.startswith("#") for token in tokens):
                continue
            kept.append(line)
        value = "\n".join(kept).strip()
        value = re.sub(r"[ \t]{2,}", " ", value)
        value = re.sub(r"\n{3,}", "\n\n", value)
        return value.strip()

    def _ensure_three_hashtags(self, rewritten: str, source_text: str) -> str:
        base = self._strip_existing_hashtags(rewritten)
        tags = self._extract_hashtags(rewritten)
        seen = {t.lower() for t in tags}

        if len(tags) < 3:
            for tag in self._generate_hashtags(source_text, count=3):
                if tag.lower() not in seen:
                    tags.append(tag)
                    seen.add(tag.lower())

        if len(tags) < 3:
            for tag in self._keyword_fallback_hashtags(source_text, count=5):
                if tag.lower() not in seen:
                    tags.append(tag)
                    seen.add(tag.lower())
                if len(tags) >= 3:
                    break

        if len(tags) < 3:
            for tag in ["#GlobalMarkets", "#DigitalAssets", "#MacroTrends"]:
                if tag.lower() not in seen:
                    tags.append(tag)
                    seen.add(tag.lower())
                if len(tags) >= 3:
                    break

        final = tags[:3]
        return f"{base}\n\n{' '.join(final)}".strip() if final else base

    @staticmethod
    def _choose_lead_word(text: str) -> str:
        value = (text or "").lower()
        breaking_signals = [
            "breaking",
            "urgent",
            "just in",
            "confirmed",
            "approval",
            "lawsuit",
            "etf",
            "liquidation",
            "hack",
        ]
        hot_signals = [
            "surge",
            "pump",
            "up ",
            "rally",
            "record",
            "soar",
            "%",
        ]
        big_signals = [
            "major",
            "big",
            "massive",
            "billion",
            "trillion",
            "partnership",
            "acquisition",
        ]
        if any(s in value for s in breaking_signals):
            return "BREAKING"
        if any(s in value for s in hot_signals):
            return "HOT"
        if any(s in value for s in big_signals):
            return "BIG"
        return "LATEST"

    @staticmethod
    def _extract_lead_from_text(text: str) -> tuple[str, str]:
        value = (text or "").strip()
        if not value:
            return "", ""
        first = value.splitlines()[0].strip()
        emoji_match = re.match(
            r"^(?P<emoji>(?:(?:[\U0001F1E6-\U0001F1FF]{2}|[\U0001F300-\U0001FAFF])\s*)+)?(?P<rest>.*)$",
            first,
            flags=re.UNICODE,
        )
        emoji = ((emoji_match.group("emoji") if emoji_match else "") or "").strip()
        rest = ((emoji_match.group("rest") if emoji_match else first) or "").strip()

        # Explicit labeled styles: WORD: ... / WORD - ...
        explicit = re.match(r"^(?P<label>[A-Za-z][A-Za-z0-9]{2,24})\s*[:\-]\s*", rest)
        if explicit:
            return emoji, explicit.group("label").upper().strip()

        # Known lead words without separator, e.g. "BREAKING Bitcoin..."
        known = [
            "BREAKING",
            "LATEST",
            "HOT",
            "BIG",
            "IMPORTANT",
            "ALERT",
            "UPDATE",
            "NEWS",
        ]
        for word in known:
            if re.match(rf"^{word}\b", rest, flags=re.IGNORECASE):
                return emoji, word

        # Uppercase lead token without separator, e.g. "IMPORTANT ..."
        caps = re.match(r"^(?P<label>[A-Z][A-Z0-9]{2,24})\b", rest)
        if caps:
            return emoji, caps.group("label").upper().strip()

        return emoji, ""

    @staticmethod
    def _emoji_for_lead(word: str) -> str:
        mapping = {
            "BREAKING": "рџљЁ",
            "LATEST": "",
            "HOT": "рџ”Ґ",
            "BIG": "вљЎ",
            "IMPORTANT": "рџљЁ",
            "ALERT": "рџљЁ",
            "UPDATE": "",
        }
        return mapping.get(word, "")

    def _ensure_lead_banner_block(self, text: str, source_text: str = "") -> str:
        value = (text or "").strip()
        if not value:
            return value

        src_emoji, src_label = self._extract_lead_from_text(source_text)
        lines = value.splitlines()
        first = lines[0].strip() if lines else ""
        rest = "\n".join(lines[1:]).strip()

        lead_re = re.compile(
            r"^(?P<prefix>(?:(?:[\U0001F1E6-\U0001F1FF]{2}|[\U0001F300-\U0001FAFF])\s*)+)?"
            r"(?P<label>[A-Za-z][A-Za-z0-9]{2,24})\s*[:\-]\s*(?P<tail>.*)$",
            flags=re.IGNORECASE,
        )
        m = lead_re.match(first)
        if m:
            raw_label = (m.group("label") or "").upper().strip()
            label = src_label or raw_label
            prefix = (m.group("prefix") or "").strip()
            emoji = src_emoji or prefix or self._emoji_for_lead(label)
            tail = (m.group("tail") or "").strip()

            body_parts = []
            if tail:
                body_parts.append(tail)
            if rest:
                body_parts.append(rest)
            body = "\n".join(body_parts).strip()
            lead = f"{emoji} {label}:".strip() if emoji else f"{label}:"
            return f"{lead}\n\n{body}".strip()

        # If rewritten first line has a known lead without separator, remove it from body and normalize style.
        inferred_emoji, inferred_label = self._extract_lead_from_text(value)
        label = src_label or inferred_label or self._choose_lead_word(value)
        emoji = src_emoji or inferred_emoji or self._emoji_for_lead(label)
        body_value = value
        if inferred_label:
            body_value = re.sub(
                rf"^(?:(?:[\U0001F1E6-\U0001F1FF]{{2}}|[\U0001F300-\U0001FAFF])\s*)*{re.escape(inferred_label)}\b[:\-]?\s*",
                "",
                value,
                flags=re.IGNORECASE,
            ).strip()
            if not body_value:
                body_value = value
        lead = f"{emoji} {label}:".strip() if emoji else f"{label}:"
        return f"{lead}\n\n{body_value}".strip()

    def _extract_image_urls(self, text: str) -> List[str]:
        urls = re.findall(r"https?://[^\s\"'<>]+", text or "")
        image_urls: List[str] = []
        for url in urls:
            normalized = url.rstrip(".,!?)")
            lower = normalized.lower()
            if any(ext in lower for ext in [".jpg", ".jpeg", ".png", ".webp"]):
                image_urls.append(normalized)
        return image_urls

    @staticmethod
    def _clean_for_image_search(text: str) -> str:
        cleaned = (text or "")
        cleaned = re.sub(r"#\w+", " ", cleaned)
        cleaned = re.sub(r"https?://\S+", " ", cleaned)
        cleaned = re.sub(r"[^\w\s]", " ", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned

    def _build_visual_search_brief(self, post_text: str) -> str:
        base = self._clean_for_image_search(self.clean_footer_text(post_text))
        if not base:
            return "finance market analysis"

        prompt = (
            "Convert this post into a short visual-image brief for photo search. "
            "Focus on what should be seen in the image, not document names. "
            "Avoid words like FAQ, announcement, report, statement, whitepaper, PDF. "
            "Return only one concise line.\n\n"
            f"Post:\n{base}"
        )
        brief = self._generate_text(prompt)
        return (brief or base).strip()

    def _extract_main_topic(self, post_text: str) -> str:
        base = self._clean_for_image_search(self.clean_footer_text(post_text))
        if not base:
            return "finance market analysis"

        prompt = (
            "Extract the main topic as one short phrase for image search. "
            "Focus on entities and core context, ignore meta terms like FAQ/report/announcement. "
            "Return one phrase only.\n\n"
            f"Post:\n{base}"
        )
        topic = self._generate_text(prompt)
        cleaned = self._clean_for_image_search(topic or "")
        return cleaned if cleaned else base[:120]

    def _build_search_queries(self, post_text: str, count: int) -> List[str]:
        main_topic = self._extract_main_topic(post_text)
        visual_brief = self._build_visual_search_brief(post_text)
        seed = f"{main_topic} {visual_brief}".strip()
        if not seed:
            return ["breaking news"] * count

        raw = self._generate_text(
            f"Generate exactly {count} specific image search queries for this text. "
            "Return one query per line with no numbering. "
            "Queries must describe visual scenes/objects, not documents or text screenshots. "
            "Do not include terms FAQ, report, whitepaper, PDF, document.\n\n"
            f"Text:\n{seed}"
        )
        queries: List[str] = []
        for line in (raw or "").splitlines():
            value = line.strip().lstrip("-*0123456789. ").strip()
            if value:
                queries.append(value)

        if main_topic:
            queries.insert(0, main_topic[:100].strip())

        if not queries:
            queries = [seed[:80].strip() or "breaking news"]

        unique: List[str] = []
        seen = set()
        for query in queries:
            key = query.lower()
            if key in seen:
                continue
            seen.add(key)
            unique.append(query)
        queries = unique

        while len(queries) < count:
            queries.append(queries[-1])
        return queries[:count]

    def _find_web_image_urls(self, post_text: str, count: int) -> List[str]:
        raw = self._generate_text(
            f"Find {count} direct image URLs highly relevant to this text using web search. "
            "Return URLs only, one per line.\n\n"
            f"Text:\n{post_text}",
            enable_google_search=True,
        )
        return self._extract_image_urls(raw or "")[:count]

    def _get_ddg_vqd(self, query: str) -> Optional[str]:
        page_url = f"https://duckduckgo.com/?q={parse.quote_plus(query)}&iax=images&ia=images"
        req = request.Request(page_url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                html = resp.read().decode("utf-8", errors="ignore")
        except (error.URLError, error.HTTPError, TimeoutError):
            return None
        match = re.search(r'vqd=["\']([^"\']+)["\']', html)
        return match.group(1) if match else None

    def _search_duckduckgo_image_candidates(self, query: str, limit: int = 12) -> List[Dict[str, str]]:
        vqd = self._get_ddg_vqd(query)
        if not vqd:
            return []

        api_url = (
            "https://duckduckgo.com/i.js"
            f"?l=us-en&o=json&q={parse.quote_plus(query)}&vqd={parse.quote_plus(vqd)}&f=,,,&p=1"
        )
        req = request.Request(
            api_url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://duckduckgo.com/",
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except (error.URLError, error.HTTPError, TimeoutError, json.JSONDecodeError):
            return []

        candidates: List[Dict[str, str]] = []
        blocked_title_terms = {
            "faq",
            "frequently asked questions",
            "whitepaper",
            "pdf",
            "document",
            "logo",
            "icon",
            "screenshot",
            "infographic",
            "template",
        }
        for item in data.get("results") or []:
            url = (item.get("image") or "").strip()
            if not url.startswith("http"):
                continue
            title = (item.get("title") or "").strip()
            title_l = title.lower()
            if any(term in title_l for term in blocked_title_terms):
                continue
            candidates.append(
                {
                    "url": url,
                    "title": title,
                    "source": (item.get("source") or "").strip(),
                    "query": query,
                }
            )
            if len(candidates) >= limit:
                break
        return candidates

    def _rank_image_candidate_urls(
        self,
        rewritten_text: str,
        candidates: List[Dict[str, str]],
        count: int,
    ) -> List[str]:
        if not candidates:
            return []

        lines: List[str] = []
        for idx, candidate in enumerate(candidates, start=1):
            lines.append(
                f"{idx}. query={candidate.get('query','')} | "
                f"title={candidate.get('title','')} | "
                f"source={candidate.get('source','')} | "
                f"url={candidate.get('url','')}"
            )

        prompt = (
            f"Text:\n{rewritten_text}\n\n"
            f"Select exactly {count} best image URLs from candidates for this text. "
            "Prioritize direct topical relevance and avoid unrelated visuals. "
            "Avoid document pages, screenshots, logos, icons, and images containing lots of text. "
            "If a regulator/organization is central to the text (e.g., CFTC), prioritize that main topic. "
            "Return URLs only, one per line.\n\n"
            "Candidates:\n"
            + "\n".join(lines)
        )
        ranked_raw = self._generate_text(prompt)
        ranked_urls = self._extract_image_urls(ranked_raw or "")
        if ranked_urls:
            return ranked_urls[:count]
        return [c["url"] for c in candidates[:count]]

    def _download_url_to_temp_file(self, url: str) -> Optional[str]:
        req = request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                content = resp.read()
                content_type = (resp.headers.get("Content-Type") or "").lower()
        except (error.URLError, error.HTTPError, TimeoutError):
            return None

        if not content:
            return None

        suffix = ".jpg"
        if "png" in content_type:
            suffix = ".png"
        elif "webp" in content_type:
            suffix = ".webp"
        elif "jpeg" in content_type or "jpg" in content_type:
            suffix = ".jpg"

        fd, tmp_path = tempfile.mkstemp(prefix="gemini_web_img_", suffix=suffix)
        Path(tmp_path).write_bytes(content)
        try:
            os.close(fd)
        except OSError:
            pass
        return tmp_path

    def _download_from_wikimedia_query(self, query: str) -> Optional[str]:
        if not query.strip():
            return None
        api_url = (
            "https://commons.wikimedia.org/w/api.php?action=query"
            "&generator=search&gsrnamespace=6&gsrlimit=1"
            f"&gsrsearch={parse.quote_plus(query)}"
            "&prop=imageinfo&iiprop=url&format=json"
        )
        req = request.Request(api_url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except (error.URLError, error.HTTPError, TimeoutError, json.JSONDecodeError):
            return None

        pages = ((data.get("query") or {}).get("pages") or {}).values()
        for page in pages:
            infos = page.get("imageinfo") or []
            if infos and infos[0].get("url"):
                return self._download_url_to_temp_file(infos[0]["url"])
        return None

    def get_replacement_images(self, post_text: str, count: int) -> List[str]:
        if not self.enabled or count <= 0:
            return []

        text = self.clean_footer_text(post_text or "")
        queries = self._build_search_queries(text, count)
        collected_paths: List[str] = []
        used_urls: set[str] = set()

        # 1) Gemini direct URLs
        for url in self._find_web_image_urls(text, count):
            if url in used_urls:
                continue
            path = self._download_url_to_temp_file(url)
            if path:
                collected_paths.append(path)
                used_urls.add(url)
            if len(collected_paths) >= count:
                return collected_paths

        # 2) Search candidates + Gemini ranking
        candidates: List[Dict[str, str]] = []
        for query in queries:
            candidates.extend(self._search_duckduckgo_image_candidates(query, limit=10))
        ranked_urls = self._rank_image_candidate_urls(text, candidates, count=count)
        for url in ranked_urls:
            if url in used_urls:
                continue
            path = self._download_url_to_temp_file(url)
            if path:
                collected_paths.append(path)
                used_urls.add(url)
            if len(collected_paths) >= count:
                return collected_paths

        # 3) Wikimedia fallback
        for query in queries:
            path = self._download_from_wikimedia_query(query)
            if path:
                collected_paths.append(path)
            if len(collected_paths) >= count:
                return collected_paths

        # 4) Last resort
        for query in queries:
            url = f"https://source.unsplash.com/1600x900/?{parse.quote_plus(query)}"
            path = self._download_url_to_temp_file(url)
            if path:
                collected_paths.append(path)
            if len(collected_paths) >= count:
                break

        return collected_paths[:count]

