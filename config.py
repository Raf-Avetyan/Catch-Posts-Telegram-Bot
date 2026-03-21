from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return

    # Use utf-8-sig so BOM-prefixed .env files are parsed correctly.
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip().lstrip("\ufeff")
        value = value.strip().strip('"').strip("'")

        if key and key not in os.environ:
            os.environ[key] = value


_load_env_file(ENV_PATH)

# Telegram API credentials from https://my.telegram.org
api_id = int(os.getenv("TELEGRAM_API_ID", "123456"))
api_hash = os.getenv("TELEGRAM_API_HASH", "your_api_hash")

# Public channel usernames (with or without @) or numeric IDs
channels_to_monitor = [
    "cointelegraph",
    "WatcherGuru",
    "AshCryptoTG"
]

# User client session name (listener)
user_session_name = os.getenv("TELEGRAM_SESSION_NAME", "telegram_listener")

# Bot forwarding settings
bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
forward_to_channel = os.getenv("FORWARD_TO_CHANNEL", "").strip()
bot_session_name = os.getenv("TELEGRAM_BOT_SESSION_NAME", "telegram_forwarder_bot")
forwarding_enabled = bool(bot_token and forward_to_channel)

# Gemini rewrite settings
gemini_api_key = os.getenv("GEMINI_API_KEY", "").strip()
gemini_model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip()

DB_PATH = BASE_DIR / "database.db"
MEDIA_DIR = BASE_DIR / "media"
