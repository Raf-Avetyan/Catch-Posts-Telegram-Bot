import asyncio
from pprint import pprint

from telethon import TelegramClient
from telethon.errors import (
    ApiIdInvalidError,
    FloodWaitError,
    PhoneNumberAppSignupForbiddenError,
    PhoneNumberBannedError,
    PhoneNumberInvalidError,
    PhonePasswordFloodError,
    RPCError,
)

from config import api_hash, api_id


DEBUG_SESSION_NAME = "telegram_login_debug"


async def main() -> None:
    if api_id == 123456 or api_hash == "your_api_hash":
        raise ValueError("Set TELEGRAM_API_ID and TELEGRAM_API_HASH in .env before running.")

    phone = input("Enter phone number in international format (example +374...): ").strip()
    if not phone:
        raise ValueError("Phone number is required.")

    client = TelegramClient(DEBUG_SESSION_NAME, api_id, api_hash)

    try:
        await client.connect()
        print("[DEBUG] Connected to Telegram.")
        print(f"[DEBUG] Session authorized before request: {await client.is_user_authorized()}")

        sent = await client.send_code_request(phone)
        print("[DEBUG] send_code_request succeeded.")
        print("[DEBUG] Response details:")
        pprint(
            {
                "type": type(sent).__name__,
                "phone_code_hash": getattr(sent, "phone_code_hash", None),
                "is_code_via_app": getattr(sent, "next_type", None),
                "timeout": getattr(sent, "timeout", None),
            }
        )
        print("[DEBUG] Telegram accepted the request. Check Telegram app/SMS/call for the code.")
    except FloodWaitError as exc:
        print(f"[ERROR] Flood wait. Retry after {exc.seconds} seconds.")
    except ApiIdInvalidError:
        print("[ERROR] TELEGRAM_API_ID / TELEGRAM_API_HASH are invalid.")
    except PhoneNumberInvalidError:
        print("[ERROR] Phone number format is invalid.")
    except PhoneNumberBannedError:
        print("[ERROR] This phone number is banned by Telegram.")
    except PhoneNumberAppSignupForbiddenError:
        print("[ERROR] This phone number cannot be used to sign in with this app.")
    except PhonePasswordFloodError:
        print("[ERROR] Too many 2FA password attempts. Wait before retrying.")
    except RPCError as exc:
        print(f"[ERROR] Telegram RPC error: {type(exc).__name__}: {exc}")
    except Exception as exc:
        print(f"[ERROR] Unexpected error: {type(exc).__name__}: {exc}")
    finally:
        await client.disconnect()
        print("[DEBUG] Disconnected.")


if __name__ == "__main__":
    asyncio.run(main())
