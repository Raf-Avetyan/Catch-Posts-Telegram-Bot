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
    "AshCryptoTG",
    "CoinvoNews",
]

# User client session name (listener)
user_session_name = os.getenv("TELEGRAM_SESSION_NAME", "telegram_listener")
telegram_enabled = os.getenv("TELEGRAM_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}

# Bot forwarding settings
bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
forward_to_channel = os.getenv("FORWARD_TO_CHANNEL", "").strip()
bot_session_name = os.getenv("TELEGRAM_BOT_SESSION_NAME", "telegram_forwarder_bot")
forwarding_enabled = bool(bot_token and forward_to_channel)

# Gemini rewrite settings
gemini_api_key = os.getenv("GEMINI_API_KEY", "").strip()
gemini_model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip()
telegram_min_hype_score = int(os.getenv("TELEGRAM_MIN_HYPE_SCORE", "5"))

# Twitter collector settings
twitter_enabled = os.getenv("TWITTER_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
twitter_usernames = [
    x.strip().lstrip("@")
    for x in os.getenv("TWITTER_USERNAMES", "").split(",")
    if x.strip()
]
twitter_poll_seconds = int(os.getenv("TWITTER_POLL_SECONDS", "600"))
twitter_fetch_limit = int(os.getenv("TWITTER_FETCH_LIMIT", "1"))
twitter_min_hype_score = int(os.getenv("TWITTER_MIN_HYPE_SCORE", "5"))
twitter_clean_forward_channel = os.getenv("TWITTER_CLEAN_FORWARD_CHANNEL", "").strip()
twitter_clean_min_hype_score = int(os.getenv("TWITTER_CLEAN_MIN_HYPE_SCORE", "6"))
twscrape_accounts_db = BASE_DIR / os.getenv("TWITTER_ACCOUNTS_DB", "twitter_accounts.db")
twikit_cookies_path = BASE_DIR / os.getenv("TWIKIT_COOKIES_FILE", "twikit_cookies.json")
twitter_bot_session_name = os.getenv("TWITTER_BOT_SESSION_NAME", "twitter_forwarder_bot")
twitter_use_twikit_only = os.getenv("TWITTER_USE_TWIKIT_ONLY", "true").strip().lower() in {"1", "true", "yes", "on"}
twitter_use_saved_cookies_only = os.getenv("TWITTER_USE_SAVED_COOKIES_ONLY", "false").strip().lower() in {"1", "true", "yes", "on"}
twitter_cookies_json = os.getenv("TWITTER_COOKIES_JSON", "").strip()

# Twikit/TWScrape account credentials (fill later)
twitter_account_username = os.getenv("TWITTER_ACCOUNT_USERNAME", "").strip().lstrip("@")
twitter_account_password = os.getenv("TWITTER_ACCOUNT_PASSWORD", "").strip()
twitter_account_email = os.getenv("TWITTER_ACCOUNT_EMAIL", "").strip()
twitter_account_email_password = os.getenv("TWITTER_ACCOUNT_EMAIL_PASSWORD", "").strip()

DB_PATH = BASE_DIR / "database.db"
MEDIA_DIR = BASE_DIR / "media"
