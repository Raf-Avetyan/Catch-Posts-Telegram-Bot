import json
from typing import Optional
from urllib import error, request


class GeminiRewriter:
    def __init__(self, api_key: str, model: str, timeout_seconds: int = 20):
        self.api_key = api_key.strip()
        self.model = model.strip() or "gemini-1.5-flash"
        self.timeout_seconds = timeout_seconds

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def rewrite(self, text: str) -> str:
        original = (text or "").strip()
        if not original or not self.enabled:
            return original

        prompt = (
            "Rewrite this Telegram post very slightly. "
            "Keep the exact meaning, facts, links, cashtags, hashtags, emojis, and tone. "
            "Only make tiny wording changes so it looks a little different. "
            "Return only the rewritten post text, nothing else.\n\n"
            f"Post:\n{original}"
        )

        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.3,
                "maxOutputTokens": 1024,
            },
        }

        url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"{self.model}:generateContent?key={self.api_key}"
        )

        req = request.Request(
            url=url,
            method="POST",
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload).encode("utf-8"),
        )

        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except (error.URLError, error.HTTPError, TimeoutError, json.JSONDecodeError):
            return original

        candidates = data.get("candidates") or []
        for candidate in candidates:
            content = candidate.get("content") or {}
            for part in content.get("parts") or []:
                rewritten = (part.get("text") or "").strip()
                if rewritten:
                    return rewritten

        return original
