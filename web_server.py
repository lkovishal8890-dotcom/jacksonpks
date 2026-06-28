#!/usr/bin/env python3
import asyncio
import base64
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import parse

import aiohttp
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Path(__file__).parent / "web_server.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("sms_hub")

app = FastAPI(title="SMS Forwarding Hub")

# Paths
BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "web_config.json"
LOG_PATH = BASE_DIR / "audit.log"
STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)

# Global Client Session
HTTP_SESSION: aiohttp.ClientSession | None = None
POLLING_TASK: asyncio.Task | None = None

# Default Configuration
DEFAULT_CONFIG = {
    "license_key": "",
    "firebase_url": "",
    "auth_key": "",
    "selected_device_id": "",
    "selected_sim_slot": 1,
    "poll_interval": 2,
    "last_timestamp": 0,
    "is_polling_active": False
}

def load_config() -> dict[str, Any]:
    if not CONFIG_PATH.exists():
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG.copy()
    try:
        data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        # Ensure all default keys exist
        for k, v in DEFAULT_CONFIG.items():
            data.setdefault(k, v)
        return data
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        return DEFAULT_CONFIG.copy()

def save_config(config: dict[str, Any]) -> None:
    try:
        CONFIG_PATH.write_text(json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        logger.error(f"Error saving config: {e}")

def append_audit_log(entry: dict[str, Any]) -> None:
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.error(f"Error writing to audit log: {e}")

# Firebase helper functions
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
                k in val for k in ("type", "mode", "isTest", "test", "isTestDevice")
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

async def firebase_get(url: str) -> Any:
    global HTTP_SESSION
    if not HTTP_SESSION:
        return None
    async with HTTP_SESSION.get(url) as resp:
        if resp.status == 200:
            body = await resp.text()
            return json.loads(body) if body else None
        return None

async def fetch_online_devices(firebase_url: str, auth_key: str) -> list[str]:
    if not firebase_url or not auth_key:
        return []
    clean_base = firebase_url.rstrip("/")
    query = parse.urlencode({"auth": auth_key})
    
    candidate_paths = ["devices", "device_registry", "clients", "device_status"]
    devices = set()
    for path in candidate_paths:
        url = f"{clean_base}/{path}.json?{query}"
        try:
            data = await firebase_get(url)
            if data:
                devices.update(normalize_devices(data))
        except Exception as e:
            logger.warning(f"Error fetching path {path}: {e}")
            continue
    return sorted(devices)

async def check_device_online_status(firebase_url: str, auth_key: str, device_id: str) -> bool:
    if not firebase_url or not auth_key or not device_id:
        return False
    online_ids = await fetch_online_devices(firebase_url, auth_key)
    return device_id in online_ids

def build_send_sms_url(base_url: str, device_id: str, auth_key: str) -> str:
    clean_base = base_url.rstrip("/")
    query = parse.urlencode({"auth": auth_key})
    return f"{clean_base}/clients/{parse.quote(device_id)}/webhookEvent/sendSms.json?{query}"

async def firebase_put_json(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    global HTTP_SESSION
    if not HTTP_SESSION:
        raise Exception("HTTP Session is not active")
    async with HTTP_SESSION.put(url, json=payload) as resp:
        resp.raise_for_status()
        body = await resp.text()
        return json.loads(body) if body else {}

async def send_sms_via_profex(config: dict[str, Any], to_number: str, message_text: str) -> dict[str, Any]:
    t0 = time.perf_counter()
    payload = {
        "from": int(config["selected_sim_slot"]),
        "to": to_number.strip(),
        "message": message_text.strip(),
        "isSended": False,
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    send_sms_url = build_send_sms_url(
        config["firebase_url"], config["selected_device_id"], config["auth_key"]
    )
    result = await firebase_put_json(send_sms_url, payload)
    elapsed_s = time.perf_counter() - t0
    
    append_audit_log(
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": "queued",
            "deviceId": config["selected_device_id"],
            "simSlot": config["selected_sim_slot"],
            "to": to_number,
            "message": message_text,
            "elapsedSeconds": round(elapsed_s, 4),
            "result": result,
        }
    )
    return result

# Vercel Polling Task
LAST_POLL_TIME: str = "Never"

async def vercel_polling_loop():
    global LAST_POLL_TIME
    logger.info("Vercel SMS Polling loop started")
    while True:
        try:
            config = load_config()
            if not config["is_polling_active"] or not config["license_key"]:
                await asyncio.sleep(2)
                continue

            license_key = config["license_key"]
            last_timestamp = config["last_timestamp"]
            
            # Form Vercel URL
            url = f"https://vercelsmsviewer.vercel.app/api/get-sms?key={parse.quote(license_key)}"
            if last_timestamp:
                url += f"&since={last_timestamp}"
                
            LAST_POLL_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            async with HTTP_SESSION.get(url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("success") and "records" in data:
                        records = data["records"]
                        if records:
                            # Vercel records are sorted newest first, so reverse to process oldest first
                            records_to_process = sorted(records, key=lambda r: r.get("timestamp", 0))
                            
                            # If this is the absolute first poll (last_timestamp is 0), do not forward
                            # historical records to prevent spamming, just advance the cursor to the latest message.
                            if last_timestamp == 0:
                                max_ts = max(r.get("timestamp", 0) for r in records_to_process)
                                config["last_timestamp"] = max_ts
                                save_config(config)
                                logger.info(f"Initialized Vercel cursor to timestamp {max_ts} without sending {len(records_to_process)} historical messages.")
                                continue
                            
                            logger.info(f"Fetched {len(records_to_process)} new records from Vercel")
                            for rec in records_to_process:
                                recipient = rec.get("recipient")
                                body = rec.get("body")
                                ts = rec.get("timestamp", 0)
                                
                                if recipient and body:
                                    logger.info(f"Forwarding SMS from Vercel: To={recipient}, Text={body}")
                                    try:
                                        # Forward
                                        if config["firebase_url"] and config["selected_device_id"]:
                                            await send_sms_via_profex(config, recipient, body)
                                            logger.info(f"Successfully forwarded SMS to Profex: To={recipient}")
                                        else:
                                            logger.warning("SMS fetched but Firebase URL or active device not configured.")
                                            append_audit_log({
                                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                                "status": "config_missing",
                                                "to": recipient,
                                                "message": body,
                                                "error": "Firebase URL or Device ID not set"
                                            })
                                    except Exception as ex:
                                        logger.error(f"Error forwarding SMS: {ex}")
                                        append_audit_log({
                                            "timestamp": datetime.now(timezone.utc).isoformat(),
                                            "status": "send_failed",
                                            "to": recipient,
                                            "message": body,
                                            "error": str(ex)
                                        })
                                
                                # Advance cursor
                                if ts > config["last_timestamp"]:
                                    config["last_timestamp"] = ts
                                    save_config(config)
                        else:
                            # No new records, cursor remains
                            pass
                else:
                    logger.warning(f"Vercel API returned status code {resp.status}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in Vercel polling loop: {e}")
            
        # Wait for the configured poll interval (default 2s)
        config = load_config()
        interval = max(1, config.get("poll_interval", 2))
        await asyncio.sleep(interval)

# FastAPI Events
@app.on_event("startup")
async def startup_event():
    global HTTP_SESSION, POLLING_TASK
    HTTP_SESSION = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10, connect=3))
    # Start background polling task
    POLLING_TASK = asyncio.create_task(vercel_polling_loop())
    logger.info("Application startup complete.")

@app.on_event("shutdown")
async def shutdown_event():
    global HTTP_SESSION, POLLING_TASK
    if POLLING_TASK:
        POLLING_TASK.cancel()
        try:
            await POLLING_TASK
        except asyncio.CancelledError:
            pass
    if HTTP_SESSION:
        await HTTP_SESSION.close()
    logger.info("Application shutdown complete.")

def parse_profex_link(link: str) -> tuple[str, str] | None:
    try:
        # Extract query parameter 's'
        parsed_url = parse.urlparse(link.strip())
        query_params = parse.parse_qs(parsed_url.query)
        s_val = query_params.get("s", [None])[0]
        if not s_val:
            # Maybe they passed the raw base64 string directly
            s_val = link.strip()
            
        # Base64 decode
        # Handle padding issues
        missing_padding = len(s_val) % 4
        if missing_padding:
            s_val += '=' * (4 - missing_padding)
            
        decoded = base64.b64decode(s_val).decode("utf-8")
        if "|||" in decoded:
            parts = decoded.split("|||")
            if len(parts) == 2:
                return parts[0].strip(), parts[1].strip()
    except Exception as e:
        logger.error(f"Error parsing profex link: {e}")
    return None

class LoginRequest(BaseModel):
    license_key: str
    profex_link: str

class ImportLinkRequest(BaseModel):
    link: str

class ConfigUpdateRequest(BaseModel):
    firebase_url: str
    auth_key: str
    selected_device_id: str
    selected_sim_slot: int
    poll_interval: int

class ManualSendRequest(BaseModel):
    to: str
    message: str

# API Endpoints
@app.post("/api/login")
async def api_login(req: LoginRequest):
    key = req.license_key.strip()
    link = req.profex_link.strip()
    if not key:
        raise HTTPException(status_code=400, detail="License key cannot be empty")
    if not link:
        raise HTTPException(status_code=400, detail="Profex Netlify link cannot be empty")
        
    # Decode the Profex Link first
    parsed = parse_profex_link(link)
    if not parsed:
        raise HTTPException(status_code=400, detail="Invalid Profex Link. Could not extract credentials.")
        
    firebase_url, auth_key = parsed
        
    # Verify the key against the Vercel API
    test_url = f"https://vercelsmsviewer.vercel.app/api/get-sms?key={parse.quote(key)}"
    try:
        async with HTTP_SESSION.get(test_url, timeout=8) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("success") == True:
                    # Key is valid! Save it, config, and activate polling
                    config = load_config()
                    config["license_key"] = key
                    config["firebase_url"] = firebase_url
                    config["auth_key"] = auth_key
                    config["is_polling_active"] = True
                    # If this is a new key, reset the cursor
                    if config["license_key"] != key:
                        config["last_timestamp"] = 0
                    save_config(config)
                    return {"success": True, "message": "Access granted"}
                else:
                    raise HTTPException(status_code=401, detail=data.get("error", "Invalid license key"))
            else:
                raise HTTPException(status_code=502, detail="Vercel API validation failed")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Login validation error: {e}")
        raise HTTPException(status_code=500, detail=f"Validation error: {str(e)}")

@app.post("/api/logout")
async def api_logout():
    config = load_config()
    config["is_polling_active"] = False
    config["license_key"] = ""
    config["last_timestamp"] = 0
    save_config(config)
    return {"success": True}

@app.post("/api/config/import-link")
async def api_import_link(req: ImportLinkRequest):
    parsed = parse_profex_link(req.link)
    if not parsed:
        raise HTTPException(status_code=400, detail="Invalid Profex Link. Could not extract credentials.")
    
    firebase_url, auth_key = parsed
    config = load_config()
    config["firebase_url"] = firebase_url
    config["auth_key"] = auth_key
    save_config(config)
    return {
        "success": True, 
        "firebase_url": firebase_url, 
        "auth_key": auth_key
    }

@app.get("/api/config")
async def api_get_config():
    config = load_config()
    # Return configuration details (hide license key slightly for safety, or return since it is local)
    return {
        "firebase_url": config["firebase_url"],
        "auth_key": config["auth_key"],
        "selected_device_id": config["selected_device_id"],
        "selected_sim_slot": config["selected_sim_slot"],
        "poll_interval": config["poll_interval"]
    }

@app.post("/api/config")
async def api_update_config(req: ConfigUpdateRequest):
    config = load_config()
    config["firebase_url"] = req.firebase_url.strip()
    config["auth_key"] = req.auth_key.strip()
    config["selected_device_id"] = req.selected_device_id.strip()
    config["selected_sim_slot"] = req.selected_sim_slot
    config["poll_interval"] = max(1, req.poll_interval)
    save_config(config)
    return {"success": True}

@app.get("/api/status")
async def api_get_status():
    config = load_config()
    license_key = config["license_key"]
    
    online_devices = []
    device_online = False
    firebase_ok = False
    
    if config["firebase_url"] and config["auth_key"]:
        try:
            online_devices = await fetch_online_devices(config["firebase_url"], config["auth_key"])
            firebase_ok = True
            if config["selected_device_id"]:
                device_online = config["selected_device_id"] in online_devices
        except Exception as e:
            logger.error(f"Status check Firebase error: {e}")
            
    return {
        "authenticated": bool(license_key),
        "firebase_configured": bool(config["firebase_url"]),
        "firebase_connected": firebase_ok,
        "online_devices": online_devices,
        "selected_device_online": device_online,
        "vercel_polling": config["is_polling_active"],
        "last_poll_time": LAST_POLL_TIME,
        "last_timestamp": config["last_timestamp"]
    }

@app.post("/api/send-test")
async def api_send_test(req: ManualSendRequest):
    config = load_config()
    if not config["firebase_url"] or not config["auth_key"] or not config["selected_device_id"]:
        raise HTTPException(status_code=400, detail="Firebase URL, Auth Key, or Selected Device is missing")
        
    try:
        result = await send_sms_via_profex(config, req.to, req.message)
        return {"success": True, "result": result}
    except Exception as e:
        logger.error(f"Manual send error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/logs")
async def api_get_logs(limit: int = 50):
    if not LOG_PATH.exists():
        return []
    try:
        lines = LOG_PATH.read_text(encoding="utf-8").strip().split("\n")
        parsed_logs = []
        for line in reversed(lines):
            if not line:
                continue
            try:
                parsed_logs.append(json.loads(line))
            except Exception:
                parsed_logs.append({"timestamp": datetime.now(timezone.utc).isoformat(), "message": line, "status": "raw"})
            if len(parsed_logs) >= limit:
                break
        return parsed_logs
    except Exception as e:
        logger.error(f"Error reading logs: {e}")
        return []

# Direct Webhook Endpoint
@app.post("/webhook")
async def api_webhook(request: Request):
    config = load_config()
    # Expect text or JSON
    content_type = request.headers.get("content-type", "")
    to_number = None
    message_text = None
    
    if "application/json" in content_type:
        try:
            data = await request.json()
            to_number = data.get("to") or data.get("phone") or data.get("recipient")
            message_text = data.get("message") or data.get("text") or data.get("body")
            
            # If a single content block is sent, try parsing it as template
            if not to_number and (data.get("content") or data.get("raw")):
                raw = data.get("content") or data.get("raw")
                # Try template parse
                from main import parse_template_block
                try:
                    parsed = parse_template_block(raw)
                    to_number = parsed.to
                    message_text = parsed.message
                except Exception:
                    pass
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")
    else:
        # Read raw text
        try:
            raw_text = (await request.body()).decode("utf-8")
            from main import parse_template_block
            parsed = parse_template_block(raw_text)
            to_number = parsed.to
            message_text = parsed.message
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Could not parse template block from text body: {e}")
            
    if not to_number or not message_text:
        raise HTTPException(status_code=400, detail="Missing 'to' or 'message' parameters or unparseable payload")
        
    if not config["firebase_url"] or not config["selected_device_id"]:
        raise HTTPException(status_code=503, detail="SMS gateway not configured in web settings")
        
    try:
        result = await send_sms_via_profex(config, to_number, message_text)
        return {"success": True, "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Fallback to serve static files
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        return index_path.read_text(encoding="utf-8")
    return """<html><body><h1>SMS Forwarding Hub</h1><p>Static index.html is missing inside /static folder.</p></body></html>"""

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
