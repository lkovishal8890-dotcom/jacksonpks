#!/usr/bin/env python3
from __future__ import annotations

import base64
import json
import time
from datetime import datetime, timezone
from urllib import parse, request

# Decoded from:
# https://profexpanel.netlify.app/?s=aHR0cHM6Ly9zaGlscGEtZTcxMmEtZGVmYXVsdC1ydGRiLmZpcmViYXNlaW8uY29tfHx8QUl6YVN5Q3NYYm1VeEppVkpxNUw3eU5wbUUxRV9WUzNnQi1qUVJr
firebase_url = "https://shilpa-e712a-default-rtdb.firebaseio.com"
auth_key = "AIzaSyCsXbmUxJiVJq5L7yNpmE1E_VS3gB-jQRk"

ONLINE_MAX_AGE_SECONDS = 120
WAIT_FOR_DEVICE_ONLINE_SECONDS = 180
POLL_INTERVAL_SECONDS = 5

def firebase_get(base_url, auth_key, path):
    clean_base = base_url.rstrip("/")
    clean_path = path.strip("/")
    query = parse.urlencode({"auth": auth_key})
    url = "{}/{}.json?{}".format(clean_base, clean_path, query)
    with request.urlopen(url, timeout=20) as response:
        body = response.read().decode("utf-8")
        return json.loads(body) if body else None

def looks_online(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value == 1
    return str(value).strip().lower() in {"online", "true", "1", "yes", "connected"}

def looks_test(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value == 1
    return str(value).strip().lower() in {"test", "testing", "true", "1", "yes"}

def normalize_devices(raw):
    if not isinstance(raw, dict):
        return []
    found = set()
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

def fetch_online_test_devices(base_url, auth_key):
    candidate_paths = ["devices", "device_registry", "clients", "device_status"]
    devices = set()
    for path in candidate_paths:
        try:
            data = firebase_get(base_url, auth_key, path)
            devices.update(normalize_devices(data))
        except Exception:
            continue
    return sorted(devices)

def to_int(value):
    try:
        return int(value)
    except Exception:
        return None

def parse_last_seen_seconds(raw):
    """
    Returns age seconds if parseable, else None.
    Accepts unix ms, unix s, or ISO-like strings.
    """
    if raw is None:
        return None
    # numeric: unix ms or unix s
    if isinstance(raw, (int, float)):
        ts = float(raw)
        ts_ms = ts * 1000.0 if ts < 1_000_000_000_000 else ts
        age = (datetime.now(timezone.utc).timestamp() * 1000.0) - ts_ms
        return max(0, int(age / 1000.0))
    if isinstance(raw, str) and raw.strip():
        s = raw.strip()
        try:
            # Handle "Z"
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age = datetime.now(timezone.utc) - dt.astimezone(timezone.utc)
            return max(0, int(age.total_seconds()))
        except Exception:
            return None
    return None

def get_device_record(base_url, auth_key, device_id):
    # Most panels store devices under `clients/<deviceId>`
    rec = firebase_get(base_url, auth_key, f"clients/{device_id}")
    return rec if isinstance(rec, dict) else None

def is_device_id_listed_online(base_url, auth_key, device_id):
    online_ids = fetch_online_test_devices(base_url, auth_key)
    return (device_id in online_ids), online_ids

def is_device_online(rec):
    """
    Panel conventions vary. We check common fields:
    - status/state/online/isOnline
    - lastSeen/last_seen/lastOnline/timestamp/time/dateTime/updatedAt
    """
    if not rec:
        return False, "Device record not found at clients/<deviceId>"

    # Field-based status
    online_flag = (
        looks_online(rec.get("status"))
        or looks_online(rec.get("state"))
        or looks_online(rec.get("online"))
        or looks_online(rec.get("isOnline"))
    )

    # Age-based status (preferred if available)
    last_seen_raw = (
        rec.get("lastSeen")
        or rec.get("last_seen")
        or rec.get("lastOnline")
        or rec.get("last_online")
        or rec.get("lastActive")
        or rec.get("last_active")
        or rec.get("timestamp")
        or rec.get("time")
        or rec.get("dateTime")
        or rec.get("updatedAt")
        or rec.get("updated_at")
    )
    age_s = parse_last_seen_seconds(last_seen_raw)
    if age_s is not None:
        if age_s <= ONLINE_MAX_AGE_SECONDS:
            return True, f"lastSeen age {age_s}s"
        # If lastSeen is old, treat offline even if status flag is true
        return False, f"lastSeen too old ({age_s}s > {ONLINE_MAX_AGE_SECONDS}s)"

    if online_flag:
        return True, "status flag indicates online"
    return False, "no recent lastSeen and status flag not online"

def build_send_sms_url(base_url, device_id, auth_key):
    clean_base = base_url.rstrip("/")
    query = parse.urlencode({"auth": auth_key})
    return "{}/clients/{}/webhookEvent/sendSms.json?{}".format(
        clean_base, parse.quote(str(device_id).strip()), query
    )

def put_to_firebase(url, payload):
    req = request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    with request.urlopen(req, timeout=20) as response:
        body = response.read().decode("utf-8")
        return {"status": "ok", "firebase_response": json.loads(body) if body else None}

# Select specific device
device_id = "7a71327b0169a565"
sim_slot = 2  # SIM 2
to_number = "+916352478864"
message_text = "test sms"

print("Using device: {}, SIM: {}".format(device_id, sim_slot))

# Wait until hardcoded device is online (strict check: must appear in online registry scan)
deadline = time.time() + WAIT_FOR_DEVICE_ONLINE_SECONDS
listed_online, online_list = is_device_id_listed_online(firebase_url, auth_key, device_id)
while not listed_online and time.time() < deadline:
    remaining = int(deadline - time.time())
    print(
        "Waiting for device to come online... ({}s left, polling every {}s)".format(
            remaining, POLL_INTERVAL_SECONDS
        )
    )
    time.sleep(POLL_INTERVAL_SECONDS)
    listed_online, online_list = is_device_id_listed_online(firebase_url, auth_key, device_id)

if not listed_online:
    print("ERROR: Hardcoded device did not come online within timeout; SMS not queued.")
    if online_list:
        print("Online devices currently seen (showing up to 25):")
        for d in online_list[:25]:
            print(" - {}".format(d))
    else:
        print("No online devices found at all. Likely no device is connected / updating status in Firebase.")
    raise SystemExit(3)

print("OK: Device id is listed online in Firebase registry scan.")

# Verify device is online before queuing SMS
device_rec = get_device_record(firebase_url, auth_key, device_id)
online, online_reason = is_device_online(device_rec)
if not online:
    print("ERROR: Device is OFFLINE (or not found). Will not queue SMS.")
    print("Reason: {}".format(online_reason))
    if device_rec:
        # Print minimal hints without dumping secrets
        status = device_rec.get("status")
        last_seen = (
            device_rec.get("lastSeen")
            or device_rec.get("last_seen")
            or device_rec.get("timestamp")
            or device_rec.get("time")
            or device_rec.get("updatedAt")
        )
        print("Device status field: {}".format(status))
        print("Device lastSeen field: {}".format(last_seen))
    raise SystemExit(2)

print("OK: Device online: {}".format(online_reason))

# Payload
payload = {
    "from": int(sim_slot),
    "to": to_number,
    "message": message_text,
    "isSended": False,
    "createdAt": datetime.now(timezone.utc).isoformat(),
}

send_sms_url = build_send_sms_url(firebase_url, device_id, auth_key)
result = put_to_firebase(send_sms_url, payload)
print("SMS queued: {}".format(result))