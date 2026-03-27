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
        self.model = model.strip() or "gemini-2.5-flash"
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
            "If the original post already starts with an emoji/title lead line, keep that first line unchanged. "
            "Only rewrite the main body text below it. "
            "If the original post has no lead line, create a one-word uppercase lead label in this format: "
            "WORD:, then one empty line, then the post body. "
            "Do not include source/footer labels. "
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

        # If still almost same (or identical), force one more rewrite pass.
        if similarity > 0.90 or result.strip().lower() == original.strip().lower():
            forced = self._generate_text(
                "Rewrite this post so wording is clearly different from source while keeping facts identical. "
                "You must change sentence openings, phrase order, and connectors. "
                "Replace source phrasing wherever possible while keeping links, numbers, entities, and meaning unchanged. "
                "Return only rewritten text.\n\n"
                f"Source:\n{original}",
                temperature=0.95,
            )
            if forced:
                result = self.clean_footer_text(forced.strip()) or result
            similarity = SequenceMatcher(a=original, b=result).ratio()

        # Final hard fallback: deterministic surface change from original text.
        if similarity > 0.92 or result.strip().lower() == original.strip().lower():
            result = self._force_surface_change(original)
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
        if SequenceMatcher(a=original, b=result).ratio() > 0.90:
            result = self._force_surface_change_preserving_source_lead(original)
        return result

    def get_hype_score(self, text: str) -> int:
        cleaned = self.clean_footer_text(text or "")
        if not cleaned or not self.enabled:
            return 5

        prompt = (
            "Rate how hype/newsworthy this post on a scale 1-10.\n"
            "Scoring rubric:\n"
            "1-3: minor/no-impact updates\n"
            "4-5: routine developments\n"
            "6-7: meaningful market or policy impact\n"
            "8-9: major breaking event, sanctions, hacks, liquidations, large approvals\n"
            "10: exceptional global market-moving event\n"
            "Return JSON only: {\"score\": <integer 1..10>}.\n\n"
            f"Post:\n{cleaned}"
        )
        raw = (self._generate_text(prompt, temperature=0.15) or "").strip()
        model_score: Optional[int] = None

        if raw:
            # Try JSON first.
            try:
                data = json.loads(raw)
                if isinstance(data, dict):
                    val = data.get("score")
                    if isinstance(val, (int, float, str)):
                        model_score = int(float(val))
            except Exception:
                # Try to extract JSON object from text.
                try:
                    obj_match = re.search(r"\{.*\}", raw, flags=re.DOTALL)
                    if obj_match:
                        data = json.loads(obj_match.group(0))
                        val = data.get("score") if isinstance(data, dict) else None
                        if isinstance(val, (int, float, str)):
                            model_score = int(float(val))
                except Exception:
                    pass

            # Fallback: numeric extraction.
            if model_score is None:
                match = re.search(r"\b(10|[1-9])\b", raw)
                if match:
                    model_score = int(match.group(1))

        heuristic_score = self._heuristic_hype_score(cleaned)
        if model_score is None:
            return heuristic_score

        model_score = max(1, min(10, model_score))

        # Blend model + heuristic so neutral model outputs do not dominate.
        blended = int(round((model_score * 0.35) + (heuristic_score * 0.65)))

        # If model is neutral but heuristic is stronger, trust heuristic more.
        if model_score == 5 and abs(heuristic_score - 5) >= 1:
            blended = int(round((model_score * 0.2) + (heuristic_score * 0.8)))

        # Additional deterministic micro-jitter to reduce many identical 5/10 outputs.
        if blended == 5:
            jitter = (sum(ord(c) for c in cleaned.lower()) % 5) - 2  # -2..+2
            if jitter >= 1:
                blended += 1
            elif jitter <= -1:
                blended -= 1

        return max(1, min(10, blended))

    @staticmethod
    def _heuristic_hype_score(text: str) -> int:
        value = (text or "").lower()
        score = 5

        high_terms = [
            "breaking", "urgent", "hack", "exploit", "sanction", "lawsuit", "approved",
            "approval", "etf", "liquidation", "bankruptcy", "ban", "cease", "sec", "cftc",
            "fed", "fomc", "cpi", "war", "attack", "default", "tariff", "rate cut", "rate hike",
            "all-time high", "all time high", "ath", "investigation", "settlement", "security breach",
        ]
        medium_terms = [
            "launch", "partnership", "acquisition", "integration", "listing", "adoption",
            "regulation", "policy", "treasury", "exchange", "reserve", "proposal", "filing",
            "guidance", "framework", "roadmap", "upgrade", "mainnet",
        ]
        low_terms = ["faq", "recap", "weekly", "summary", "guide", "opinion", "thread"]

        for term in high_terms:
            if term in value:
                score += 1
        for term in medium_terms:
            if term in value:
                score += 0.5
        for term in low_terms:
            if term in value:
                score -= 0.75

        # Large percentage moves imply stronger hype.
        percents = [int(x) for x in re.findall(r"(\d{1,3})\s*%", value)]
        if percents:
            mx = max(percents)
            if mx >= 20:
                score += 2
            elif mx >= 10:
                score += 1
            elif mx >= 5:
                score += 0.5

        # Big money mention boosts score.
        if re.search(r"\b\d+(\.\d+)?\s*(billion|million|trillion|bn|tn|m)\b", value):
            score += 1

        # Strong directional market language.
        if re.search(r"\b(surge|soar|jump|rally|pump|crash|plunge|dump|sell[- ]off)\b", value):
            score += 1

        # Broader geopolitical/regulatory cues.
        if re.search(r"\b(white house|congress|senate|parliament|treasury|doj|sec|cftc|eu)\b", value):
            score += 0.5

        # Deterministic small variation so not everything becomes exactly 5.
        if int(round(score)) == 5:
            jitter = (sum(ord(c) for c in value) % 3) - 1  # -1..+1
            score += 0.5 * jitter

        return max(1, min(10, int(round(score))))

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
    def _force_surface_change(text: str) -> str:
        value = (text or "").strip()
        if not value:
            return value
        lines = [x.strip() for x in value.splitlines() if x.strip()]
        if not lines:
            return value

        # Keep first line if it looks like lead label; rotate the rest for visible change.
        lead_line = lines[0] if re.search(r"^[^\w\s]{0,4}\s*[A-Za-z]{3,24}\s*[:\-]", lines[0]) else ""
        body = lines[1:] if lead_line else lines
        if len(body) >= 2:
            body = body[1:] + body[:1]

        joined = " ".join(body)
        joined = re.sub(r"\bhowever\b", "still", joined, flags=re.IGNORECASE)
        joined = re.sub(r"\btherefore\b", "so", joined, flags=re.IGNORECASE)
        joined = re.sub(r"\bin addition\b", "also", joined, flags=re.IGNORECASE)
        joined = re.sub(r"\baccording to\b", "per", joined, flags=re.IGNORECASE)
        joined = re.sub(r"\s{2,}", " ", joined).strip()
        if lead_line:
            return f"{lead_line}\n{joined}".strip()
        return joined

    def _force_surface_change_preserving_source_lead(self, text: str) -> str:
        value = (text or "").strip()
        if not value:
            return value

        lead_line = self._extract_exact_source_lead_line(value)
        if not lead_line:
            return self._force_surface_change(value)

        lines = value.splitlines()
        body_lines = lines[1:]
        while body_lines and not body_lines[0].strip():
            body_lines.pop(0)
        body = "\n".join(body_lines).strip()
        if not body:
            return lead_line

        changed_body = self._force_surface_change(body)
        return f"{lead_line}\n\n{changed_body}".strip()

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
        value = (text or "")
        # Remove hashtags anywhere in text, not only hashtag-only lines.
        value = re.sub(r"(?<!\w)#[A-Za-z0-9_]{2,50}\b", "", value)
        lines = [line.strip() for line in value.splitlines()]
        # Drop lines that become empty after hashtag cleanup.
        lines = [line for line in lines if line]
        value = "\n".join(lines).strip()
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

    def _extract_exact_source_lead_line(self, text: str) -> str:
        value = (text or "").strip()
        if not value:
            return ""
        first = value.splitlines()[0].strip()
        _emoji, label = self._extract_lead_from_text(value)
        return first if label else ""

    @staticmethod
    def _emoji_for_lead(word: str) -> str:
        # Default now: no auto emoji for normal news.
        return ""

    @staticmethod
    def _is_market_crash_news(text: str) -> bool:
        value = (text or "").lower()
        crash_signals = [
            "market crash",
            "crash",
            "plunge",
            "plunged",
            "dump",
            "selloff",
            "sell-off",
            "liquidation",
            "wiped out",
            "down 20%",
            "down 30%",
            "down 40%",
            "bloodbath",
            "red market",
        ]
        return any(s in value for s in crash_signals)

    @staticmethod
    def _extract_country_flags(text: str) -> str:
        value = (text or "").lower()
        # Lightweight country->flag mapping for most common news countries.
        country_flags = {
            "united states": "\U0001F1FA\U0001F1F8",
            "usa": "\U0001F1FA\U0001F1F8",
            "us ": "\U0001F1FA\U0001F1F8",
            "u.s.": "\U0001F1FA\U0001F1F8",
            "uk": "\U0001F1EC\U0001F1E7",
            "united kingdom": "\U0001F1EC\U0001F1E7",
            "britain": "\U0001F1EC\U0001F1E7",
            "england": "\U0001F1EC\U0001F1E7",
            "eu": "\U0001F1EA\U0001F1FA",
            "european union": "\U0001F1EA\U0001F1FA",
            "china": "\U0001F1E8\U0001F1F3",
            "japan": "\U0001F1EF\U0001F1F5",
            "russia": "\U0001F1F7\U0001F1FA",
            "ukraine": "\U0001F1FA\U0001F1E6",
            "iran": "\U0001F1EE\U0001F1F7",
            "israel": "\U0001F1EE\U0001F1F1",
            "turkey": "\U0001F1F9\U0001F1F7",
            "india": "\U0001F1EE\U0001F1F3",
            "france": "\U0001F1EB\U0001F1F7",
            "germany": "\U0001F1E9\U0001F1EA",
            "italy": "\U0001F1EE\U0001F1F9",
            "spain": "\U0001F1EA\U0001F1F8",
            "brazil": "\U0001F1E7\U0001F1F7",
            "canada": "\U0001F1E8\U0001F1E6",
            "armenia": "\U0001F1E6\U0001F1F2",
        }
        flags: List[str] = []
        for key, flag in country_flags.items():
            if key in value and flag not in flags:
                flags.append(flag)
            if len(flags) >= 3:
                break
        return " ".join(flags).strip()

    def _ensure_lead_banner_block(self, text: str, source_text: str = "") -> str:
        def _strip_leading_flags_emojis(value: str) -> str:
            # Remove only prefix emojis/flags from body start (not whole body content).
            return re.sub(
                r"^(?:(?:[\U0001F1E6-\U0001F1FF]{2}|[\U0001F300-\U0001FAFF])\s*)+",
                "",
                (value or "").strip(),
                flags=re.UNICODE,
            ).strip()

        def _merge_unique_prefix(parts: List[str]) -> str:
            tokens: List[str] = []
            seen = set()
            for part in parts:
                for token in (part or "").split():
                    t = token.strip()
                    if not t:
                        continue
                    if t in seen:
                        continue
                    seen.add(t)
                    tokens.append(t)
            return " ".join(tokens).strip()

        value = (text or "").strip()
        if not value:
            return value

        source_lead_line = self._extract_exact_source_lead_line(source_text)
        src_emoji, src_label = self._extract_lead_from_text(source_text)
        src_flags = self._extract_country_flags(source_text)
        txt_flags = self._extract_country_flags(value)
        lead_flags = src_flags or txt_flags
        lines = value.splitlines()
        first = lines[0].strip() if lines else ""
        rest = "\n".join(lines[1:]).strip()

        lead_re = re.compile(
            r"^(?P<prefix>(?:(?:[\U0001F1E6-\U0001F1FF]{2}|[\U0001F300-\U0001FAFF])\s*)+)?"
            r"(?P<label>[A-Za-z][A-Za-z0-9]{2,24})\s*[:\-]\s*(?P<tail>.*)$",
            flags=re.IGNORECASE,
        )
        m = lead_re.match(first)
        inferred_emoji, inferred_label = self._extract_lead_from_text(value)

        if source_lead_line:
            body_value = value
            if m:
                tail = (m.group("tail") or "").strip()
                body_parts = []
                if tail:
                    body_parts.append(tail)
                if rest:
                    body_parts.append(rest)
                body_value = "\n".join(body_parts).strip()
            elif inferred_label:
                body_value = re.sub(
                    rf"^(?:(?:[\U0001F1E6-\U0001F1FF]{{2}}|[\U0001F300-\U0001FAFF])\s*)*{re.escape(inferred_label)}\b[:\-]?\s*",
                    "",
                    value,
                    flags=re.IGNORECASE,
                ).strip()
                if not body_value:
                    body_value = value

            body_value = _strip_leading_flags_emojis(body_value)
            return f"{source_lead_line}\n\n{body_value}".strip()

        if m:
            raw_label = (m.group("label") or "").upper().strip()
            label = src_label or raw_label
            prefix = (m.group("prefix") or "").strip()
            auto_emoji = "\U0001FA78" if self._is_market_crash_news(source_text or value) else ""
            emoji = src_emoji or prefix or auto_emoji or self._emoji_for_lead(label)
            tail = (m.group("tail") or "").strip()

            body_parts = []
            if tail:
                body_parts.append(tail)
            if rest:
                body_parts.append(rest)
            body = _strip_leading_flags_emojis("\n".join(body_parts).strip())
            lead_prefix = _merge_unique_prefix([lead_flags, emoji])
            lead = f"{lead_prefix} {label}:".strip() if lead_prefix else f"{label}:"
            return f"{lead}\n\n{body}".strip()

        # If rewritten first line has a known lead without separator, remove it from body and normalize style.
        label = src_label or inferred_label or self._choose_lead_word(value)
        auto_emoji = "\U0001FA78" if self._is_market_crash_news(source_text or value) else ""
        emoji = src_emoji or inferred_emoji or auto_emoji or self._emoji_for_lead(label)
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
        body_value = _strip_leading_flags_emojis(body_value)
        lead_prefix = _merge_unique_prefix([lead_flags, emoji])
        lead = f"{lead_prefix} {label}:".strip() if lead_prefix else f"{label}:"
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
