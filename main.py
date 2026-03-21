from config import DB_PATH, MEDIA_DIR, telegram_enabled
from db import Database
from telegram_client import run_listener
from twitter_collector import run_twitter_collector_in_background


def main() -> None:
    db = Database(DB_PATH)
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    run_twitter_collector_in_background(db=db)
    if telegram_enabled:
        run_listener(db=db, media_dir=MEDIA_DIR)
    else:
        print("[INFO] Telegram listener disabled (TELEGRAM_ENABLED=false).")
        print("[INFO] Twitter collector (if enabled) keeps running in background. Press Ctrl+C to stop.")
        try:
            import time

            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            print("\n[INFO] Stopped by user.")


if __name__ == "__main__":
    main()
