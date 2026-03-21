import asyncio
import getpass
from pathlib import Path
from typing import Any

import qrcode
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

from config import BASE_DIR, api_hash, api_id, user_session_name


QR_IMAGE_PATH = Path(BASE_DIR) / "telegram_login_qr.png"


def save_qr_png(url: str) -> None:
    qr_code = qrcode.QRCode(border=2, box_size=12)
    qr_code.add_data(url)
    qr_code.make(fit=True)
    qr_image = qr_code.make_image(fill_color="black", back_color="white")
    qr_image.save(QR_IMAGE_PATH)


async def refresh_qr_token(qr_login: Any) -> Any:
    refreshed = await qr_login.recreate()
    return refreshed if refreshed is not None else qr_login


async def main() -> None:
    if api_id == 123456 or api_hash == "your_api_hash":
        raise ValueError("Set TELEGRAM_API_ID and TELEGRAM_API_HASH in .env before running.")

    client = TelegramClient(user_session_name, api_id, api_hash)

    try:
        await client.connect()

        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"[INFO] Session already authorized as {getattr(me, 'first_name', 'Unknown')} ({me.id})")
            return

        qr_login = await client.qr_login()
        print("[INFO] Scan the PNG in Telegram:")
        print("[INFO] Telegram -> Settings -> Devices -> Link Desktop Device")
        print(f"[INFO] QR image path: {QR_IMAGE_PATH}")
        print("[INFO] Keep the script running while you scan. The PNG will refresh automatically.")
        print()

        while True:
            try:
                save_qr_png(qr_login.url)
                print("[INFO] QR image refreshed. Scan the latest PNG now.")
            except Exception as exc:
                print(f"[WARN] Could not save PNG QR image: {exc}")

            try:
                await asyncio.wait_for(qr_login.wait(), timeout=25)
                break
            except asyncio.TimeoutError:
                qr_login = await refresh_qr_token(qr_login)
            except SessionPasswordNeededError:
                print("[INFO] Two-step verification is enabled for this account.")
                password = getpass.getpass("Enter your Telegram 2FA password: ").strip()
                if not password:
                    raise ValueError("Telegram 2FA password is required to finish QR login.")
                await client.sign_in(password=password)
                break

        me = await client.get_me()
        print(f"[SUCCESS] Logged in as {getattr(me, 'first_name', 'Unknown')} ({me.id})")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
