import asyncio
import re
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

from telethon import TelegramClient, events
from telethon import utils as telethon_utils
from telethon.errors import RPCError
from telethon.tl.custom.message import Message
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import MessageEntityTextUrl, MessageEntityUrl

from config import (
    api_hash,
    api_id,
    bot_session_name,
    bot_token,
    channels_to_monitor,
    forward_to_channel,
    forwarding_enabled,
    gemini_api_key,
    gemini_model,
    telegram_min_hype_score,
    user_session_name,
)
from db import Database
from gemini_client import GeminiRewriter

URL_REGEX = re.compile(r"https?://[^\s]+", re.IGNORECASE)


class TelegramChannelListener:
    def __init__(self, db: Database, media_dir: Path):
        self.db = db
        self.media_dir = media_dir
        self.media_dir.mkdir(parents=True, exist_ok=True)

        self.client = TelegramClient(user_session_name, api_id, api_hash)
        self.bot_client: Optional[TelegramClient] = None
        if forwarding_enabled:
            self.bot_client = TelegramClient(bot_session_name, api_id, api_hash)

        self.valid_channels: List[str] = []
        self.monitored_chat_ids: set[int] = set()
        self.forward_to_channel = forward_to_channel
        self.rewriter = GeminiRewriter(api_key=gemini_api_key, model=gemini_model)

    @staticmethod
    def _normalize_channel_input(channel: str) -> str:
        value = channel.strip()
        if not value:
            return ""

        value = value.replace("https://t.me/", "").replace("http://t.me/", "")
        value = value.strip("/")
        if "/" in value:
            value = value.split("/", 1)[0]
        return value

    async def _ensure_joined_public_channel(self, normalized_channel: str) -> None:
        if not normalized_channel:
            return
        if normalized_channel.startswith("-100") or normalized_channel.lstrip("-").isdigit():
            return

        channel_ref = normalized_channel if normalized_channel.startswith("@") else f"@{normalized_channel}"
        try:
            await self.client(JoinChannelRequest(channel_ref))
            print(f"[INFO] Joined channel for updates: {channel_ref}")
        except Exception:
            pass

    async def _resolve_channels(self, channels: Sequence[str]) -> List[str]:
        resolved: List[str] = []
        for ch in channels:
            normalized = self._normalize_channel_input(ch)
            if not normalized:
                continue
            try:
                await self._ensure_joined_public_channel(normalized)
                entity = await self.client.get_entity(normalized)
                username = getattr(entity, "username", None)
                resolved_name = f"@{username}" if username else str(getattr(entity, "id", normalized))
                resolved.append(resolved_name)

                entity_id = getattr(entity, "id", None)
                if isinstance(entity_id, int):
                    self.monitored_chat_ids.add(entity_id)

                peer_id = telethon_utils.get_peer_id(entity)
                if isinstance(peer_id, int):
                    self.monitored_chat_ids.add(peer_id)
            except Exception as exc:
                print(f"[WARN] Skipping invalid/unavailable channel '{normalized}': {exc}")
        return resolved

    def _is_monitored_message(self, message: Message) -> bool:
        chat_id = getattr(message, "chat_id", None)
        if isinstance(chat_id, int) and chat_id in self.monitored_chat_ids:
            return True

        peer_id = getattr(message, "peer_id", None)
        try:
            normalized_peer_id = telethon_utils.get_peer_id(peer_id) if peer_id else None
        except Exception:
            normalized_peer_id = None

        return isinstance(normalized_peer_id, int) and normalized_peer_id in self.monitored_chat_ids

    async def _probe_channel_access(self) -> None:
        for ch in self.valid_channels:
            try:
                latest = None
                async for msg in self.client.iter_messages(ch, limit=1):
                    latest = msg
                    break

                if latest:
                    print(
                        f"[PROBE] {ch} latest_message_id={latest.id} "
                        f"at={latest.date.isoformat() if latest.date else 'unknown'}"
                    )
                else:
                    print(f"[PROBE] {ch} has no visible messages for this user session")
            except Exception as exc:
                print(f"[PROBE-WARN] Cannot read {ch}: {exc}")

    @staticmethod
    def _channel_key(message: Message) -> str:
        chat = message.chat
        if chat is None:
            return "unknown"
        username = getattr(chat, "username", None)
        if username:
            return f"@{username}"
        chat_id = getattr(chat, "id", None)
        return str(chat_id) if chat_id is not None else "unknown"

    @staticmethod
    def _extract_links(message: Message) -> List[str]:
        links = set()
        text = message.message or ""

        for match in URL_REGEX.findall(text):
            links.add(match.rstrip(".,!?)"))

        entities = message.entities or []
        for entity in entities:
            if isinstance(entity, MessageEntityTextUrl) and entity.url:
                links.add(entity.url)
            elif isinstance(entity, MessageEntityUrl):
                start = entity.offset
                end = entity.offset + entity.length
                url = text[start:end].strip()
                if url:
                    links.add(url)

        return sorted(links)

    def _build_post_text(self, message: Message) -> tuple[str, List[str]]:
        text = (message.message or "").strip()
        links = self._extract_links(message)

        if links:
            links_block = "\n".join(links)
            text = f"{text}\n\n[links]\n{links_block}".strip()

        return text, links

    async def _download_media_for_message(self, message: Message, subdir: Optional[str] = None) -> List[str]:
        if not message.media:
            return []

        target_dir = self.media_dir / subdir if subdir else self.media_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        try:
            file_path = await message.download_media(file=target_dir)
            if not file_path:
                return []
            return [str(Path(file_path).resolve())]
        except Exception as exc:
            print(f"[ERROR] Failed to download media for message {message.id}: {exc}")
            return []

    def _save_post(
        self,
        channel: str,
        message_id: int,
        text: str,
        media_paths: Iterable[str],
        created_at: str,
        links_count: int,
    ) -> bool:
        media_paths_list = list(media_paths)
        media_value = "|".join(media_paths_list) if media_paths_list else None

        inserted = self.db.insert_post(
            source="telegram",
            channel=channel,
            message_id=message_id,
            text=text,
            media_path=media_value,
            created_at=created_at,
        )

        if inserted:
            print(
                f"[NEW] channel={channel} message_id={message_id} "
                f"media={len(media_paths_list)} links={links_count}"
            )
        else:
            print(f"[SKIP] Duplicate message channel={channel} message_id={message_id}")

        return inserted

    @staticmethod
    def _split_text(text: str, chunk_size: int = 4000) -> List[str]:
        if len(text) <= chunk_size:
            return [text]
        return [text[i : i + chunk_size] for i in range(0, len(text), chunk_size)]

    @staticmethod
    def _chunks(items: List[str], size: int) -> List[List[str]]:
        return [items[i : i + size] for i in range(0, len(items), size)]

    async def _forward_to_target(
        self,
        text: str,
        media_paths: Iterable[str],
    ) -> None:
        if not self.bot_client or not self.forward_to_channel:
            return

        media_paths_list = list(media_paths)
        clean_text = self.rewriter.clean_footer_text(text)
        if clean_text and self.rewriter.enabled:
            rewritten = await asyncio.to_thread(self.rewriter.rewrite, clean_text)
            if rewritten:
                rewritten_clean = self.rewriter.clean_footer_text(rewritten)
                if rewritten_clean and rewritten_clean != clean_text:
                    clean_text = rewritten_clean
                    print("[INFO] Post text rewritten by Gemini.")
                else:
                    print("[WARN] Gemini returned same/empty rewrite. Original text used.")
            else:
                print("[WARN] Gemini rewrite failed (no response). Original text used.")
        elif clean_text and not self.rewriter.enabled:
            print("[WARN] Gemini rewrite disabled. Original text used.")
        elif not clean_text:
            print("[INFO] No post text to rewrite (media-only post).")

        score_input = clean_text or (text or "")
        hype_score = await asyncio.to_thread(self.rewriter.get_hype_score, score_input)
        if hype_score < max(1, min(10, telegram_min_hype_score)):
            print(
                f"[INFO] Skipped forwarding post below threshold: "
                f"{hype_score}/10 < {max(1, min(10, telegram_min_hype_score))}/10"
            )
            return
        score_line = f"Hype Score: {hype_score}/10"
        quoted_meta = f"> {score_line}"
        clean_text = f"{clean_text}\n\n{quoted_meta}".strip() if clean_text else quoted_meta

        try:
            if media_paths_list:
                caption_sent = False
                for batch in self._chunks(media_paths_list, 10):
                    files = batch if len(batch) > 1 else batch[0]
                    caption = None
                    if clean_text and not caption_sent:
                        caption = clean_text[:1024]
                        caption_sent = True
                    await self.bot_client.send_file(self.forward_to_channel, files, caption=caption)

                if clean_text and len(clean_text) > 1024:
                    for chunk in self._split_text(clean_text[1024:]):
                        await self.bot_client.send_message(self.forward_to_channel, chunk)
            elif clean_text:
                for chunk in self._split_text(clean_text):
                    await self.bot_client.send_message(self.forward_to_channel, chunk)

            print(f"[FORWARDED] to={self.forward_to_channel}")
        except Exception as exc:
            print(f"[ERROR] Forwarding failed to {self.forward_to_channel}: {exc}")

    async def _handle_single_message(self, message: Message) -> None:
        if message.grouped_id:
            return

        channel = self._channel_key(message)
        message_id = message.id
        text, links = self._build_post_text(message)
        forward_text = (message.message or "").strip()
        created_at = message.date.isoformat() if message.date else ""

        media_paths = await self._download_media_for_message(message)

        inserted = self._save_post(
            channel=channel,
            message_id=message_id,
            text=text,
            media_paths=media_paths,
            created_at=created_at,
            links_count=len(links),
        )

        if inserted:
            await self._forward_to_target(forward_text, media_paths)

    async def _handle_album(self, messages: Sequence[Message]) -> None:
        if not messages:
            return

        first = messages[0]
        channel = self._channel_key(first)
        message_id = first.id
        text, links = self._build_post_text(first)
        forward_text = (first.message or "").strip()
        created_at = first.date.isoformat() if first.date else ""

        album_dir = f"{channel.strip('@').replace('/', '_')}_{first.grouped_id or first.id}"

        downloaded: List[str] = []
        for msg in messages:
            paths = await self._download_media_for_message(msg, subdir=album_dir)
            downloaded.extend(paths)

        inserted = self._save_post(
            channel=channel,
            message_id=message_id,
            text=text,
            media_paths=downloaded,
            created_at=created_at,
            links_count=len(links),
        )

        if inserted:
            await self._forward_to_target(forward_text, downloaded)

    async def run(self) -> None:
        if api_id == 123456 or api_hash == "your_api_hash":
            raise ValueError("Set TELEGRAM_API_ID and TELEGRAM_API_HASH in .env before running.")

        await self.client.start()
        me = await self.client.get_me()
        if getattr(me, "bot", False):
            raise ValueError(
                "telegram_listener.session is logged in as a BOT account. "
                "Delete telegram_listener.session and login with your PHONE number/code (user account), "
                "not bot token."
            )

        self.valid_channels = await self._resolve_channels(channels_to_monitor)
        if not self.valid_channels:
            raise ValueError("No valid channels to monitor. Check channels_to_monitor in config.py")

        print(f"[INFO] Listening to channels: {', '.join(self.valid_channels)}")
        print(f"[INFO] Monitored chat ids: {sorted(self.monitored_chat_ids)}")
        await self._probe_channel_access()

        if self.bot_client:
            try:
                await self.bot_client.start(bot_token=bot_token)
                print(f"[INFO] Bot forwarding enabled. Target: {self.forward_to_channel}")
            except Exception as exc:
                self.bot_client = None
                print(f"[ERROR] Failed to start bot forwarding: {exc}")
        else:
            print("[WARN] Bot forwarding is disabled. Set TELEGRAM_BOT_TOKEN and FORWARD_TO_CHANNEL in .env")

        if self.rewriter.enabled:
            print(f"[INFO] Gemini rewrite enabled. Model: {gemini_model}")
        else:
            print("[WARN] Gemini rewrite disabled. Set GEMINI_API_KEY in .env")

        @self.client.on(events.NewMessage())
        async def on_new_message(event):
            try:
                if not self._is_monitored_message(event.message):
                    return
                await self._handle_single_message(event.message)
            except RPCError as rpc_exc:
                print(f"[ERROR] Telegram RPC error: {rpc_exc}")
            except Exception as exc:
                print(f"[ERROR] NewMessage handler failed: {exc}")

        @self.client.on(events.Album())
        async def on_album(event):
            try:
                if not event.messages:
                    return
                if not self._is_monitored_message(event.messages[0]):
                    return
                await self._handle_album(event.messages)
            except RPCError as rpc_exc:
                print(f"[ERROR] Telegram RPC error (album): {rpc_exc}")
            except Exception as exc:
                print(f"[ERROR] Album handler failed: {exc}")

        print("[INFO] Collector is running. Press Ctrl+C to stop.")

        try:
            await self.client.run_until_disconnected()
        finally:
            if self.bot_client and self.bot_client.is_connected():
                await self.bot_client.disconnect()


def run_listener(db: Database, media_dir: Path) -> None:
    listener = TelegramChannelListener(db=db, media_dir=media_dir)
    try:
        asyncio.run(listener.run())
    except KeyboardInterrupt:
        print("\n[INFO] Stopped by user.")
