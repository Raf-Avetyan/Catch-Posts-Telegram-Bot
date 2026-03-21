from config import DB_PATH, MEDIA_DIR
from db import Database
from telegram_client import run_listener


def main() -> None:
    db = Database(DB_PATH)
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    run_listener(db=db, media_dir=MEDIA_DIR)


if __name__ == "__main__":
    main()
