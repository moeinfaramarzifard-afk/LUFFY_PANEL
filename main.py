import asyncio
import json
import os
import hashlib
import secrets
import time
import re
from datetime import datetime, timezone, timedelta
from urllib.parse import quote
from collections import deque, defaultdict

from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import Response, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import httpx
import logging
import psutil
import sqlite3

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Luffy-Gateway")

app = FastAPI(title="Luffy Panel", docs_url=None, redoc_url=None)

CONFIG = {
    "port": int(os.environ.get("PORT", 8000)),
    "secret": os.environ.get("SECRET_KEY", secrets.token_urlsafe(32)),
    "db_path": os.environ.get("DB_PATH", "luffy.db"),
    "tg_token": os.environ.get("TELEGRAM_BOT_TOKEN", ""),
    "tg_chat_id": os.environ.get("TELEGRAM_CHAT_ID", ""),
}

# NOTE: allow_origins=["*"] together with allow_credentials=True is not honored by
# browsers per the CORS spec (a wildcard origin cannot be combined with credentials),
# so this combination is effectively inert in real browsers. If you ever need
# cross-origin requests with cookies, replace "*" with an explicit list of trusted
# origins, e.g. allow_origins=["https://your-panel-domain.com"].
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# ── In-memory state ───────────────────────────────────────────────────────────
connections: dict = {}
connections_lock = asyncio.Lock()          # lock for connections
connection_sockets: dict = {}
link_ip_map: dict = defaultdict(set)
SHARE_TOKENS: dict = {}   # token -> {uid, created_at, expires_at, used}
stats = {"total_bytes": 0, "total_requests": 0, "total_errors": 0, "start_time": time.time()}
error_logs: deque = deque(maxlen=50)
hourly_traffic: dict = defaultdict(int)
daily_traffic: dict = defaultdict(int)
http_client: httpx.AsyncClient | None = None

LINKS: dict = {}
LINKS_LOCK = asyncio.Lock()
CUSTOM_ADDRESSES: list = ["www.speedtest.net"]
CUSTOM_ADDRESSES_LOCK = asyncio.Lock()

SESSION_COOKIE = "ren_session"
SESSION_TTL = 60 * 60 * 24 * 7
UNLIMITED_QUOTA_BYTES = 53687091200000

# ── Persistence (SQLite) ───────────────────────────────────────────────────────
DB_LOCK = asyncio.Lock()

def _db_conn():
    conn = sqlite3.connect(CONFIG["db_path"])
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def db_init():
    conn = _db_conn()
    conn.execute("""CREATE TABLE IF NOT EXISTS links (
        uid TEXT PRIMARY KEY, label TEXT, limit_bytes INTEGER, used_bytes INTEGER,
        max_connections INTEGER, created_at TEXT, active INTEGER, expires_at TEXT,
        notified_expiry INTEGER DEFAULT 0, notified_quota INTEGER DEFAULT 0
    )""")
    conn.execute("CREATE TABLE IF NOT EXISTS addresses (address TEXT PRIMARY KEY)")
    conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
    conn.commit()
    conn.close()

def db_load_all():
    conn = _db_conn()
    conn.row_factory = sqlite3.Row
    links = {}
    for row in conn.execute("SELECT * FROM links"):
        links[row["uid"]] = {
            "label": row["label"], "limit_bytes": row["limit_bytes"], "used_bytes": row["used_bytes"],
            "max_connections": row["max_connections"], "created_at": row["created_at"],
            "active": bool(row["active"]), "expires_at": row["expires_at"],
            "notified_expiry": bool(row["notified_expiry"]), "notified_quota": bool(row["notified_quota"]),
        }
    addresses = [r["address"] for r in conn.execute("SELECT address FROM addresses")]
    settings = {r["key"]: r["value"] for r in conn.execute("SELECT key, value FROM settings")}
    conn.close()
    return links, addresses, settings

async def db_save_link(uid: str, link: dict):
    async with DB_LOCK:
        conn = _db_conn()
        conn.execute(
            """INSERT INTO links (uid,label,limit_bytes,used_bytes,max_connections,created_at,active,expires_at,notified_expiry,notified_quota)
               VALUES (?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(uid) DO UPDATE SET label=excluded.label, limit_bytes=excluded.limit_bytes,
               used_bytes=excluded.used_bytes, max_connections=excluded.max_connections,
               active=excluded.active, expires_at=excluded.expires_at,
               notified_expiry=excluded.notified_expiry, notified_quota=excluded.notified_quota""",
            (uid, link["label"], link["limit_bytes"], link["used_bytes"], link.get("max_connections", 0),
             link["created_at"], int(link["active"]), link.get("expires_at"),
             int(link.get("notified_expiry", False)), int(link.get("notified_quota", False)))
        )
        conn.commit()
        conn.close()

async def db_delete_link(uid: str):
    async with DB_LOCK:
        conn = _db_conn()
        conn.execute("DELETE FROM links WHERE uid=?", (uid,))
        conn.commit()
        conn.close()

async def db_save_address(address: str):
    async with DB_LOCK:
        conn = _db_conn()
        conn.execute("INSERT OR IGNORE INTO addresses (address) VALUES (?)", (address,))
        conn.commit()
        conn.close()

async def db_delete_address(address: str):
    async with DB_LOCK:
        conn = _db_conn()
        conn.execute("DELETE FROM addresses WHERE address=?", (address,))
        conn.commit()
        conn.close()

async def db_save_setting(key: str, value: str):
    async with DB_LOCK:
        conn = _db_conn()
        conn.execute("INSERT INTO settings (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
        conn.commit()
        conn.close()

async def flush_usage_to_db():
    """Periodically persist in-memory traffic counters so restarts don't lose usage data."""
    while True:
        await asyncio.sleep(30)
        try:
            async with LINKS_LOCK:
                snapshot = {uid: dict(data) for uid, data in LINKS.items()}
            for uid, data in snapshot.items():
                await db_save_link(uid, data)
        except Exception:
            logger.exception("usage flush failed")

# ── Telegram notifications ─────────────────────────────────────────────────────
async def telegram_notify(text: str):
    token, chat_id = CONFIG["tg_token"], CONFIG["tg_chat_id"]
    if not token or not chat_id:
        return
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
            )
    except Exception:
        pass

async def quota_expiry_watcher():
    """Checks every 5 minutes for links nearing quota/expiry and sends a one-time Telegram alert."""
    while True:
        await asyncio.sleep(300)
        try:
            async with LINKS_LOCK:
                items = list(LINKS.items())
            for uid, link in items:
                changed = False
                if link["limit_bytes"] > 0:
                    pct = link["used_bytes"] / link["limit_bytes"] * 100
                    if pct >= 90 and not link.get("notified_quota"):
                        await telegram_notify(f"⚠️ <b>{link['label']}</b> reached {pct:.0f}% of its data quota.")
                        link["notified_quota"] = True
                        changed = True
                exp = parse_expires_at(link.get("expires_at"))
                if exp is not None:
                    remaining = (exp - datetime.now(timezone.utc)).total_seconds()
                    if 0 < remaining <= 86400 and not link.get("notified_expiry"):
                        await telegram_notify(f"⏳ <b>{link['label']}</b> expires in less than 24 hours.")
                        link["notified_expiry"] = True
                        changed = True
                if changed:
                    await db_save_link(uid, link)
        except Exception:
            logger.exception("quota/expiry watcher failed")

# ── Auth ─────────────────────────────────────────────────────────────────────
def hash_password(pw: str) -> str:
    return hashlib.sha256(f"{pw}{CONFIG['secret']}".encode()).hexdigest()

_admin_password = os.environ.get("ADMIN_PASSWORD")
if not _admin_password:
    raise RuntimeError(
        "ADMIN_PASSWORD environment variable is not set. "
        "Set it before starting the server (no default password is provided)."
    )
AUTH = {"password_hash": hash_password(_admin_password)}
SESSIONS: dict = {}
SESSIONS_LOCK = asyncio.Lock()

async def create_session() -> str:
    token = secrets.token_urlsafe(32)
    async with SESSIONS_LOCK:
        SESSIONS[token] = time.time() + SESSION_TTL
    return token

async def is_valid_session(token: str | None) -> bool:
    if not token:
        return False
    async with SESSIONS_LOCK:
        exp = SESSIONS.get(token)
        if exp is None or exp < time.time():
            SESSIONS.pop(token, None)
            return False
        return True

async def destroy_session(token: str | None):
    if token:
        async with SESSIONS_LOCK:
            SESSIONS.pop(token, None)

async def require_auth(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    if not await is_valid_session(token):
        raise HTTPException(status_code=401, detail="unauthorized")
    return token

# ── Keep-alive ────────────────────────────────────────────────────────────────
async def keep_alive():
    while True:
        await asyncio.sleep(600)
        try:
            domain = get_domain()
            if domain and domain != "localhost":
                async with httpx.AsyncClient(timeout=10.0) as client:
                    await client.get(f"https://{domain}/health")
        except Exception:
            pass

@app.on_event("startup")
async def startup():
    global http_client
    limits = httpx.Limits(max_connections=500, max_keepalive_connections=100)
    timeout = httpx.Timeout(30.0, connect=10.0)
    http_client = httpx.AsyncClient(limits=limits, timeout=timeout, follow_redirects=True)

    db_init()
    saved_links, saved_addresses, saved_settings = db_load_all()
    if saved_links:
        async with LINKS_LOCK:
            LINKS.update(saved_links)
    if saved_addresses:
        async with CUSTOM_ADDRESSES_LOCK:
            CUSTOM_ADDRESSES.clear()
            CUSTOM_ADDRESSES.extend(saved_addresses)
    if "password_hash" in saved_settings:
        AUTH["password_hash"] = saved_settings["password_hash"]
    elif os.environ.get("ADMIN_PASSWORD"):
        await db_save_setting("password_hash", AUTH["password_hash"])

    asyncio.create_task(keep_alive())
    asyncio.create_task(flush_usage_to_db())
    asyncio.create_task(quota_expiry_watcher())
    await ensure_default_link()

@app.on_event("shutdown")
async def shutdown():
    if http_client:
        await http_client.aclose()

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_domain() -> str:
    return (
        os.environ.get("RENDER_EXTERNAL_URL", os.environ.get("RAILWAY_PUBLIC_DOMAIN", "localhost"))
        .replace("https://", "").replace("http://", "")
    )

def generate_uuid(seed: str | None = None) -> str:
    if seed is None:
        return (
            str(secrets.token_hex(16))[:8] + "-" + secrets.token_hex(2) + "-" +
            secrets.token_hex(2) + "-" + secrets.token_hex(2) + "-" + secrets.token_hex(6)
        )
    h = hashlib.sha256(f"{seed}{CONFIG['secret']}".encode()).hexdigest()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"

def generate_vless_link(uuid: str, remark: str = "Luffy", address: str = None) -> str:
    domain = get_domain()
    addr = address if address else domain
    path = f"/ws/{uuid}"
    params = {
        "encryption": "none", "security": "tls", "type": "ws",
        "host": domain, "path": path, "sni": domain, "fp": "chrome", "alpn": "http/1.1"
    }
    query = "&".join(f"{k}={quote(str(v))}" for k, v in params.items())
    return f"vless://{uuid}@{addr}:443?{query}#{quote(remark)}"

def uptime() -> str:
    secs = int(time.time() - stats["start_time"])
    h, m, s = secs // 3600, (secs % 3600) // 60, secs % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def parse_size_to_bytes(value: float, unit: str) -> int:
    unit = unit.upper()
    if unit == "GB": return int(value * 1024 * 1024 * 1024)
    if unit == "MB": return int(value * 1024 * 1024)
    if unit == "KB": return int(value * 1024)
    return int(value)

def parse_expires_at(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        normalised = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalised)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def seconds_until_expiry(expires_at_str: str | None) -> int | None:
    exp = parse_expires_at(expires_at_str)
    if exp is None:
        return None
    remaining = (exp - datetime.now(timezone.utc)).total_seconds()
    return max(0, int(remaining))

async def ensure_default_link():
    created = False
    async with LINKS_LOCK:
        if not LINKS:
            LINKS["Default"] = {
                "label": "Default",
                "limit_bytes": 0,
                "used_bytes": 0,
                "max_connections": 0,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "active": True,
                "expires_at": None,
            }
            created = True
    if created:
        await db_save_link("Default", LINKS["Default"])

def get_client_ip(websocket: WebSocket) -> str:
    forwarded = websocket.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if websocket.client:
        return websocket.client.host
    return "unknown"

# lock used here
async def count_connections_for_link(uid: str) -> int:
    async with connections_lock:
        return sum(1 for info in connections.values() if info.get("uuid") == uid)

async def remove_ip_from_link(uid: str, ip: str):
    async with connections_lock:
        if uid in link_ip_map:
            link_ip_map[uid].discard(ip)
            if not link_ip_map[uid]:
                link_ip_map.pop(uid, None)

async def close_connections_for_link(uid: str):
    async with connections_lock:
        to_close = [cid for cid, info in connections.items() if info.get("uuid") == uid]
    for cid in to_close:
        ws = connection_sockets.get(cid)
        if ws:
            try:
                await ws.close(code=1000, reason="link deleted")
            except Exception:
                pass
        async with connections_lock:
            connections.pop(cid, None)
        connection_sockets.pop(cid, None)
    async with connections_lock:
        link_ip_map.pop(uid, None)

# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {"service": "Luffy Panel", "version": "1.0", "status": "active", "domain": get_domain()}

@app.get("/health")
async def health():
    async with connections_lock:
        conn_count = len(connections)
    return {"status": "ok", "connections": conn_count, "uptime": uptime()}

LOGIN_ATTEMPTS: dict = defaultdict(list)  # ip -> list of failed-attempt timestamps
LOGIN_MAX_ATTEMPTS = 5
LOGIN_WINDOW_SECONDS = 300  # 5 minutes

def _login_client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"

@app.post("/api/login")
async def api_login(request: Request):
    ip = _login_client_ip(request)
    now = time.time()
    attempts = [t for t in LOGIN_ATTEMPTS[ip] if now - t < LOGIN_WINDOW_SECONDS]
    LOGIN_ATTEMPTS[ip] = attempts
    if len(attempts) >= LOGIN_MAX_ATTEMPTS:
        retry_after = int(LOGIN_WINDOW_SECONDS - (now - attempts[0]))
        raise HTTPException(
            status_code=429,
            detail=f"Too many login attempts. Try again in {max(retry_after, 1)} seconds.",
        )
    body = await request.json()
    password = str(body.get("password") or "")
    if not secrets.compare_digest(hash_password(password), AUTH["password_hash"]):
        LOGIN_ATTEMPTS[ip].append(now)
        raise HTTPException(status_code=401, detail="Invalid password")
    LOGIN_ATTEMPTS.pop(ip, None)
    token = await create_session()
    resp = JSONResponse({"ok": True})
    is_secure = get_domain() != "localhost"
    resp.set_cookie(
        key=SESSION_COOKIE, value=token, max_age=SESSION_TTL,
        httponly=True, samesite="lax", path="/", secure=is_secure,
    )
    return resp

@app.post("/api/logout")
async def api_logout(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    await destroy_session(token)
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(SESSION_COOKIE, path="/")
    return resp

@app.get("/api/me")
async def api_me(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    return {"authenticated": await is_valid_session(token)}

@app.post("/api/change-password")
async def api_change_password(request: Request, _=Depends(require_auth)):
    body = await request.json()
    current = str(body.get("current_password") or "")
    new = str(body.get("new_password") or "")
    if not secrets.compare_digest(hash_password(current), AUTH["password_hash"]):
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    if len(new) < 4:
        raise HTTPException(status_code=400, detail="Password must be at least 4 characters")
    AUTH["password_hash"] = hash_password(new)
    await db_save_setting("password_hash", AUTH["password_hash"])
    current_token = request.cookies.get(SESSION_COOKIE)
    async with SESSIONS_LOCK:
        SESSIONS.clear()
        if current_token:
            SESSIONS[current_token] = time.time() + SESSION_TTL
    return {"ok": True}

@app.get("/stats")
async def get_stats(_=Depends(require_auth)):
    async with connections_lock:
        conn_count = len(connections)
    return {
        "active_connections": conn_count,
        "total_traffic_mb": round(stats["total_bytes"] / (1024 * 1024), 2),
        "total_requests": stats["total_requests"],
        "total_errors": stats["total_errors"],
        "uptime": uptime(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "recent_errors": list(error_logs)[-10:],
        "links_count": len(LINKS),
        "domain": get_domain(),
        "cpu_percent": psutil.cpu_percent(interval=0.1),
        "memory_percent": psutil.virtual_memory().percent,
        "hourly_traffic": dict(hourly_traffic),
    }

@app.post("/api/links")
async def create_link(request: Request, _=Depends(require_auth)):
    body = await request.json()
    label = (body.get("label") or "New Link").strip()[:60]
    if not re.match(r'^[a-zA-Z0-9\-_. ]+$', label):
        raise HTTPException(status_code=400, detail="Inbound name must contain only English letters, numbers, and characters: - _ . space")
    if not label:
        raise HTTPException(status_code=400, detail="Inbound name is required")
    async with LINKS_LOCK:
        if label in LINKS:
            raise HTTPException(status_code=400, detail="An inbound with this name already exists")
    limit_value = float(body.get("limit_value") or 0)
    limit_unit = body.get("limit_unit") or "GB"
    limit_bytes = 0 if limit_value <= 0 else parse_size_to_bytes(limit_value, limit_unit)
    max_conn = int(body.get("max_connections") or 0)
    if max_conn < 0:
        max_conn = 0
    days_valid = body.get("days_valid")
    expires_at: str | None = None
    if days_valid is not None:
        try:
            days_valid = int(days_valid)
            if days_valid > 0:
                expires_at = (datetime.now(timezone.utc) + timedelta(days=days_valid)).isoformat()
        except (ValueError, TypeError):
            pass
    uid = label
    async with LINKS_LOCK:
        LINKS[uid] = {
            "label": label,
            "limit_bytes": limit_bytes,
            "used_bytes": 0,
            "max_connections": max_conn,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "active": True,
            "expires_at": expires_at,
        }
    await db_save_link(uid, LINKS[uid])
    return {
        "uuid": uid, "label": label, "limit_bytes": limit_bytes, "used_bytes": 0,
        "max_connections": max_conn, "active": True, "created_at": LINKS[uid]["created_at"],
        "expires_at": expires_at, "vless_link": generate_vless_link(uid, remark=f"Luffy-{label}"),
    }

@app.post("/api/links/bulk")
async def create_links_bulk(request: Request, _=Depends(require_auth)):
    """Create multiple inbounds at once, e.g. {"prefix": "User", "count": 10, "limit_value": 10,
    "limit_unit": "GB", "max_connections": 2, "days_valid": 30}"""
    body = await request.json()
    prefix = (body.get("prefix") or "User").strip()[:40]
    if not re.match(r'^[a-zA-Z0-9\-_. ]+$', prefix):
        raise HTTPException(status_code=400, detail="Prefix must contain only English letters, numbers, and characters: - _ . space")
    count = int(body.get("count") or 0)
    if count <= 0 or count > 200:
        raise HTTPException(status_code=400, detail="Count must be between 1 and 200")
    limit_value = float(body.get("limit_value") or 0)
    limit_unit = body.get("limit_unit") or "GB"
    limit_bytes = 0 if limit_value <= 0 else parse_size_to_bytes(limit_value, limit_unit)
    max_conn = int(body.get("max_connections") or 0)
    if max_conn < 0:
        max_conn = 0
    days_valid = body.get("days_valid")
    expires_at: str | None = None
    if days_valid is not None:
        try:
            days_valid = int(days_valid)
            if days_valid > 0:
                expires_at = (datetime.now(timezone.utc) + timedelta(days=days_valid)).isoformat()
        except (ValueError, TypeError):
            pass

    created = []
    async with LINKS_LOCK:
        existing_max = 0
        for k in LINKS:
            m = re.match(rf'^{re.escape(prefix)}-(\d+)$', k)
            if m:
                existing_max = max(existing_max, int(m.group(1)))
        for i in range(1, count + 1):
            uid = f"{prefix}-{existing_max + i}"
            LINKS[uid] = {
                "label": uid, "limit_bytes": limit_bytes, "used_bytes": 0,
                "max_connections": max_conn, "created_at": datetime.now(timezone.utc).isoformat(),
                "active": True, "expires_at": expires_at,
            }
            created.append(uid)
    for uid in created:
        await db_save_link(uid, LINKS[uid])
    return {"ok": True, "created": created, "count": len(created)}

@app.get("/api/links")
async def list_links(_=Depends(require_auth)):
    result = []
    async with LINKS_LOCK:
        items = list(LINKS.items())
    for uid, data in items:
        result.append({
            "uuid": uid,
            "label": data["label"],
            "limit_bytes": data["limit_bytes"],
            "used_bytes": data["used_bytes"],
            "max_connections": data.get("max_connections", 0),
            "active": data["active"],
            "created_at": data["created_at"],
            "expires_at": data.get("expires_at"),
            "current_connections": await count_connections_for_link(uid),  # FIX: await
            "vless_link": generate_vless_link(uid, remark=f"Luffy-{data['label']}"),
        })
    result.sort(key=lambda x: x["created_at"], reverse=True)
    return {"links": result}

@app.patch("/api/links/{uid}")
async def toggle_link(uid: str, request: Request, _=Depends(require_auth)):
    body = await request.json()
    async with LINKS_LOCK:
        if uid not in LINKS:
            raise HTTPException(status_code=404, detail="link not found")
        if "active" in body:
            LINKS[uid]["active"] = bool(body["active"])
        if "limit_value" in body:
            limit_value = float(body.get("limit_value") or 0)
            limit_unit = body.get("limit_unit") or "GB"
            LINKS[uid]["limit_bytes"] = 0 if limit_value <= 0 else parse_size_to_bytes(limit_value, limit_unit)
        if "reset_usage" in body and body["reset_usage"]:
            LINKS[uid]["used_bytes"] = 0
            LINKS[uid]["notified_quota"] = False
        if "label" in body:
            LINKS[uid]["label"] = str(body["label"])[:60]
        if "max_connections" in body:
            mc = int(body["max_connections"] or 0)
            LINKS[uid]["max_connections"] = mc if mc >= 0 else 0
        if "days_valid" in body:
            try:
                dv = int(body["days_valid"])
                if dv > 0:
                    LINKS[uid]["expires_at"] = (datetime.now(timezone.utc) + timedelta(days=dv)).isoformat()
                    LINKS[uid]["notified_expiry"] = False
                else:
                    LINKS[uid]["expires_at"] = None
            except (ValueError, TypeError):
                pass
        link_copy = dict(LINKS[uid])
    await db_save_link(uid, link_copy)
    return {"ok": True}

@app.delete("/api/links/{uid}")
async def delete_link(uid: str, _=Depends(require_auth)):
    async with LINKS_LOCK:
        LINKS.pop(uid, None)
    await close_connections_for_link(uid)
    await db_delete_link(uid)
    return {"ok": True}

@app.get("/api/addresses")
async def list_addresses(_=Depends(require_auth)):
    async with CUSTOM_ADDRESSES_LOCK:
        return {"addresses": list(CUSTOM_ADDRESSES)}

@app.post("/api/addresses")
async def add_address(request: Request, _=Depends(require_auth)):
    body = await request.json()
    address = (body.get("address") or "").strip()
    if not address:
        raise HTTPException(status_code=400, detail="Address is required")
    if not re.match(r'^[a-zA-Z0-9\-_. ]+$', address):
        raise HTTPException(status_code=400, detail="Address must contain only English letters, numbers, and characters: - _ .")
    async with CUSTOM_ADDRESSES_LOCK:
        if address in CUSTOM_ADDRESSES:
            raise HTTPException(status_code=400, detail="Address already exists")
        CUSTOM_ADDRESSES.append(address)
    await db_save_address(address)
    return {"ok": True, "addresses": list(CUSTOM_ADDRESSES)}

@app.delete("/api/addresses/{index}")
async def delete_address(index: int, _=Depends(require_auth)):
    async with CUSTOM_ADDRESSES_LOCK:
        if 0 <= index < len(CUSTOM_ADDRESSES):
            removed = CUSTOM_ADDRESSES.pop(index)
        else:
            raise HTTPException(status_code=404, detail="Address not found")
    await db_delete_address(removed)
    return {"ok": True, "addresses": list(CUSTOM_ADDRESSES)}

PING_TIMEOUT_SECONDS = 8.0

async def _https_ping(address: str) -> dict:
    """Measure real-world HTTPS latency to an address: full DNS + TCP + TLS
    handshake + time-to-first-byte of an HTTP HEAD request. This is closer to
    what a client actually experiences than a bare TCP SYN/ACK, which can
    return misleadingly fast if something near the server (a load balancer,
    firewall, or edge node) answers the handshake without reaching the
    real backend."""
    url = f"https://{address}/"
    start = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=PING_TIMEOUT_SECONDS, verify=False) as client:
            await client.head(url, follow_redirects=True)
        elapsed_ms = round((time.monotonic() - start) * 1000)
        return {"address": address, "ok": True, "ms": elapsed_ms}
    except httpx.TimeoutException:
        return {"address": address, "ok": False, "error": "timeout"}
    except httpx.HTTPError as exc:
        return {"address": address, "ok": False, "error": str(exc) or "connection failed"}
    except Exception as exc:
        return {"address": address, "ok": False, "error": str(exc) or "failed"}

@app.post("/api/addresses/ping")
async def ping_addresses(_=Depends(require_auth)):
    """Test real HTTPS latency (DNS + TCP + TLS + TTFB) for every saved custom address, in parallel."""
    async with CUSTOM_ADDRESSES_LOCK:
        addresses = list(CUSTOM_ADDRESSES)
    if not addresses:
        return {"results": []}
    results = await asyncio.gather(*(_https_ping(addr) for addr in addresses))
    return {"results": results}

@app.get("/api/backup")
async def export_backup(_=Depends(require_auth)):
    """Full JSON export of inbounds + addresses, downloadable for safekeeping."""
    async with LINKS_LOCK:
        links_snapshot = {uid: dict(data) for uid, data in LINKS.items()}
    async with CUSTOM_ADDRESSES_LOCK:
        addresses_snapshot = list(CUSTOM_ADDRESSES)
    payload = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "links": links_snapshot,
        "addresses": addresses_snapshot,
    }
    headers = {"Content-Disposition": f'attachment; filename="luffy-backup-{int(time.time())}.json"'}
    return JSONResponse(content=payload, headers=headers)

@app.post("/api/restore")
async def restore_backup(request: Request, _=Depends(require_auth)):
    """Restore inbounds + addresses from a previously exported backup JSON. Replaces current data."""
    body = await request.json()
    links_in = body.get("links")
    addresses_in = body.get("addresses")
    if not isinstance(links_in, dict):
        raise HTTPException(status_code=400, detail="Invalid backup file: missing 'links'")
    restored = 0
    async with LINKS_LOCK:
        LINKS.clear()
        for uid, data in links_in.items():
            try:
                label = str(data.get("label", uid))[:60]
                if not re.match(r'^[a-zA-Z0-9\-_. ]+$', label):
                    continue
                LINKS[uid] = {
                    "label": label,
                    "limit_bytes": int(data.get("limit_bytes", 0)),
                    "used_bytes": int(data.get("used_bytes", 0)),
                    "max_connections": int(data.get("max_connections", 0)),
                    "created_at": data.get("created_at") or datetime.now(timezone.utc).isoformat(),
                    "active": bool(data.get("active", True)),
                    "expires_at": data.get("expires_at"),
                    "notified_expiry": bool(data.get("notified_expiry", False)),
                    "notified_quota": bool(data.get("notified_quota", False)),
                }
                restored += 1
            except Exception:
                continue
        links_snapshot = {uid: dict(d) for uid, d in LINKS.items()}
    if isinstance(addresses_in, list):
        async with CUSTOM_ADDRESSES_LOCK:
            CUSTOM_ADDRESSES.clear()
            CUSTOM_ADDRESSES.extend(str(a) for a in addresses_in)
        async with DB_LOCK:
            conn = _db_conn()
            conn.execute("DELETE FROM addresses")
            conn.executemany("INSERT OR IGNORE INTO addresses (address) VALUES (?)", [(a,) for a in CUSTOM_ADDRESSES])
            conn.commit()
            conn.close()
    async with DB_LOCK:
        conn = _db_conn()
        conn.execute("DELETE FROM links")
        conn.commit()
        conn.close()
    for uid, data in links_snapshot.items():
        await db_save_link(uid, data)
    return {"ok": True, "restored_links": restored, "restored_addresses": len(CUSTOM_ADDRESSES)}

@app.get("/api/links/{uid}/sub")
async def get_subscription(uid: str, _=Depends(require_auth)):
    import base64
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None:
            raise HTTPException(status_code=404, detail="link not found")
        link = dict(link)
    vless_link = generate_vless_link(uid, remark=f"Luffy-{link['label']}")
    used = link["used_bytes"]
    limit = link["limit_bytes"]
    used_mb = round(used / (1024 * 1024), 2)
    limit_mb = round(limit / (1024 * 1024), 2) if limit > 0 else 0
    pct = round((used / limit) * 100, 1) if limit > 0 else 0
    remaining_mb = round((limit - used) / (1024 * 1024), 2) if limit > 0 else 0
    sub_content = f"# Luffy Panel\n{vless_link}"
    encoded = base64.b64encode(sub_content.encode()).decode()
    return {
        "subscription_url": f"{get_domain()}/api/links/{uid}/sub",
        "config": vless_link, "label": link["label"],
        "used_bytes": used, "limit_bytes": limit,
        "used_mb": used_mb, "limit_mb": limit_mb,
        "remaining_mb": remaining_mb, "usage_percent": pct,
        "active": link["active"], "sub_base64": encoded, "sub_text": sub_content,
    }

def _fmt_bytes(b: int) -> str:
    if b >= 1_073_741_824: return f"{b / 1_073_741_824:.1f}GB"
    if b >= 1_048_576: return f"{b / 1_048_576:.1f}MB"
    return f"{b / 1024:.1f}KB"

def generate_subscription_content(link: dict, uid: str, addresses: list[str]) -> str:
    used = link["used_bytes"]
    limit = link["limit_bytes"]
    expires_at_str = link.get("expires_at")
    usage_str = f"{_fmt_bytes(used)} / ∞" if limit == 0 else f"{_fmt_bytes(used)} / {_fmt_bytes(limit)}"
    secs_left = seconds_until_expiry(expires_at_str)
    if secs_left is None:
        expiry_str = "∞"
    elif secs_left == 0:
        expiry_str = "Expired"
    else:
        expiry_str = f"{secs_left // 86400} Days Left"
    status_node = generate_vless_link(uid, remark=f"📊 {usage_str} | ⏳ {expiry_str}", address="0.0.0.0")
    links_out = [status_node, generate_vless_link(uid, remark=f"Luffy-{link['label']}-Server")]
    for i, addr in enumerate(addresses):
        links_out.append(generate_vless_link(uid, remark=f"Luffy-{link['label']}-IP{i+1}", address=addr))
    return "\n".join(links_out)

@app.get("/sub/{uid}")
async def subscription_endpoint(uid: str):
    import base64
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None:
            raise HTTPException(status_code=404, detail="link not found")
        link = dict(link)
    if not link["active"]:
        raise HTTPException(status_code=403, detail="link disabled")
    expires_at = parse_expires_at(link.get("expires_at"))
    if expires_at is not None and expires_at < datetime.now(timezone.utc):
        raise HTTPException(status_code=403, detail="link expired")
    async with CUSTOM_ADDRESSES_LOCK:
        addresses = list(CUSTOM_ADDRESSES)
    sub_content = generate_subscription_content(link, uid, addresses)
    encoded = base64.b64encode(sub_content.encode()).decode()
    total_bytes = link["limit_bytes"] if link["limit_bytes"] > 0 else UNLIMITED_QUOTA_BYTES
    expire_ts = 0
    if expires_at is not None:
        expire_ts = int(expires_at.timestamp())
    headers = {
        "Content-Type": "text/plain; charset=utf-8",
        "Content-Disposition": 'attachment; filename="sub.txt"',
        "profile-update-interval": "6",
        "subscription-userinfo": f"upload={link['used_bytes']}; download=0; total={total_bytes}; expire={expire_ts}",
    }
    return Response(content=encoded, headers=headers)

# ── Minimal standalone public page (shared style with the panel) ──────────────
def _public_page(title: str, body: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>{title}</title>
<link href="https://fonts.googleapis.com/css2?family=Sora:wght@600;700;800&family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@500;600&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
:root{{--bg:#0A0A0B;--surface:#19191C;--surface3:#212124;--border:rgba(255,255,255,.08);
--text:#F2F2F3;--text2:#A0A0A8;--text3:#65656E;--accent:#6366F1;--accent-dim:rgba(99,102,241,.14);
--green:#22C55E;--green-dim:rgba(34,197,94,.13);--red:#F43F5E;--red-dim:rgba(244,63,94,.13);
--yellow:#F5A524;}}
body{{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px}}
.box{{background:var(--surface);border:1px solid var(--border);border-radius:18px;padding:30px;width:100%;max-width:420px;box-shadow:0 10px 30px rgba(0,0,0,.35)}}
.mono{{width:36px;height:36px;border-radius:11px;background:linear-gradient(155deg,#6366F1,#4338CA);display:flex;align-items:center;justify-content:center;color:#fff;font-family:'Sora',sans-serif;font-weight:800;font-size:16px;margin-bottom:14px}}
h1{{font-family:'Sora',sans-serif;font-size:18px;font-weight:700;letter-spacing:-.01em;margin-bottom:4px}}
p{{color:var(--text3);font-size:12.5px;line-height:1.6}}
.pill-bar{{height:8px;background:var(--surface3);border-radius:4px;overflow:hidden;margin:14px 0 6px}}
.pill-fill{{height:100%;border-radius:4px}}
.row{{display:flex;justify-content:space-between;padding:10px 0;border-bottom:1px solid var(--border);font-size:12.5px}}
.row:last-child{{border-bottom:none}}
.row b{{font-family:'JetBrains Mono',monospace;font-weight:600}}
.tag{{display:inline-flex;padding:3px 9px;border-radius:6px;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.04em}}
.qr-box{{text-align:center;padding:18px;background:var(--surface3);border-radius:12px;margin-top:16px}}
.qr-box img{{max-width:200px;border-radius:8px}}
textarea{{width:100%;margin-top:14px;background:var(--surface3);border:1px solid var(--border);border-radius:10px;color:var(--text2);font-size:10.5px;font-family:'JetBrains Mono',monospace;padding:10px;resize:none}}
.copybtn{{width:100%;margin-top:10px;background:var(--accent);color:#fff;border:none;border-radius:9px;padding:11px;font-weight:600;font-size:13px;cursor:pointer;font-family:inherit}}
</style></head><body><div class="box">{body}</div>
<script>
function cp(){{const t=document.getElementById('cfgtxt');if(!t)return;navigator.clipboard.writeText(t.value).then(()=>{{const b=document.getElementById('cpbtn');if(b){{b.textContent='Copied ✓';setTimeout(()=>b.textContent='Copy Config',1500);}}}});}}
</script>
</body></html>"""

# ── One-time secure share links ────────────────────────────────────────────────
@app.post("/api/links/{uid}/share")
async def create_share_link(uid: str, _=Depends(require_auth)):
    async with LINKS_LOCK:
        if uid not in LINKS:
            raise HTTPException(status_code=404, detail="link not found")
    for tok, info in list(SHARE_TOKENS.items()):
        if info["expires_at"] < time.time():
            SHARE_TOKENS.pop(tok, None)
    token = secrets.token_urlsafe(24)
    SHARE_TOKENS[token] = {"uid": uid, "created_at": time.time(), "expires_at": time.time() + 86400, "used": False}
    return {"ok": True, "share_url": f"https://{get_domain()}/share/{token}"}

@app.get("/share/{token}", response_class=HTMLResponse)
async def view_share_link(token: str):
    info = SHARE_TOKENS.get(token)
    if info is None or info["expires_at"] < time.time():
        return HTMLResponse(_public_page("Link Expired", """
            <div class="mono">!</div><h1>This link is no longer valid</h1>
            <p>One-time share links expire after 24 hours or after being opened once. Ask the admin to generate a new one.</p>"""))
    if info["used"]:
        return HTMLResponse(_public_page("Already Viewed", """
            <div class="mono">✓</div><h1>This link was already opened</h1>
            <p>For security, one-time share links can only be viewed once. Ask the admin to generate a new one if you need the config again.</p>"""))
    info["used"] = True
    uid = info["uid"]
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None:
            return HTMLResponse(_public_page("Not Found", '<div class="mono">!</div><h1>Config not found</h1><p>This inbound no longer exists.</p>'))
        link = dict(link)
    vless_link = generate_vless_link(uid, remark=f"Luffy-{link['label']}")
    body = f"""
        <div class="mono">L</div>
        <h1>{link['label']}</h1>
        <p>Save this config now — this link cannot be opened again.</p>
        <div class="qr-box"><img src="https://api.qrserver.com/v1/create-qr-code/?size=240x240&data={quote(vless_link)}" alt="QR"></div>
        <textarea id="cfgtxt" rows="3" readonly>{vless_link}</textarea>
        <button class="copybtn" id="cpbtn" onclick="cp()">Copy Config</button>
    """
    return HTMLResponse(_public_page(f"{link['label']} · Config", body))

# ── Public client self-status page ─────────────────────────────────────────────
@app.get("/status/{uid}", response_class=HTMLResponse)
async def client_status_page(uid: str):
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None:
            return HTMLResponse(_public_page("Not Found", '<div class="mono">!</div><h1>Not found</h1><p>No such config exists.</p>'), status_code=404)
        link = dict(link)
    used, limit = link["used_bytes"], link["limit_bytes"]
    pct = min(100, round((used / limit) * 100, 1)) if limit > 0 else 0
    color = "var(--red)" if pct > 90 else ("var(--yellow)" if pct > 70 else "var(--accent)")
    used_str = _fmt_bytes(used)
    limit_str = "Unlimited" if limit == 0 else _fmt_bytes(limit)
    secs_left = seconds_until_expiry(link.get("expires_at"))
    if secs_left is None:
        expiry_str = "Never"
    elif secs_left == 0:
        expiry_str = "Expired"
    else:
        expiry_str = f"{secs_left // 86400}d {(secs_left % 86400) // 3600}h left"
    active = link["active"] and secs_left != 0
    status_tag = f'<span class="tag" style="background:var(--green-dim);color:var(--green)">Active</span>' if active else f'<span class="tag" style="background:var(--red-dim);color:var(--red)">Inactive</span>'
    bar = "" if limit == 0 else f'<div class="pill-bar"><div class="pill-fill" style="width:{pct}%;background:{color}"></div></div>'
    body = f"""
        <div class="mono">L</div>
        <h1>{link['label']}</h1>
        <p>Your personal usage status. This page only shows your own connection — nothing else.</p>
        <div class="row"><span>Status</span>{status_tag}</div>
        <div class="row"><span>Data used</span><b>{used_str} / {limit_str}</b></div>
        {bar}
        <div class="row"><span>Expires</span><b>{expiry_str}</b></div>
        <div class="row"><span>Max devices</span><b>{link.get('max_connections') or '∞'}</b></div>
    """
    return HTMLResponse(_public_page(f"{link['label']} · Status", body))

# ── WebSocket tunnel ──────────────────────────────────────────────────────────
RELAY_BUF = 64 * 1024

async def parse_vless_header(first_chunk: bytes):
    if len(first_chunk) < 24:
        raise ValueError("chunk too small")
    try:
        pos = 1 + 16
        addon_len = first_chunk[pos]
        pos += 1 + addon_len
        command = first_chunk[pos]
        pos += 1
        port = int.from_bytes(first_chunk[pos:pos + 2], "big")
        pos += 2
        addr_type = first_chunk[pos]
        pos += 1
        if addr_type == 1:
            addr_bytes = first_chunk[pos:pos + 4]
            if len(addr_bytes) < 4:
                raise ValueError("truncated IPv4 address")
            pos += 4
            address = ".".join(str(b) for b in addr_bytes)
        elif addr_type == 2:
            domain_len = first_chunk[pos]
            pos += 1
            address = first_chunk[pos:pos + domain_len].decode("utf-8", errors="ignore")
            pos += domain_len
        elif addr_type == 3:
            addr_bytes = first_chunk[pos:pos + 16]
            if len(addr_bytes) < 16:
                raise ValueError("truncated IPv6 address")
            pos += 16
            address = ":".join(f"{addr_bytes[i]:02x}{addr_bytes[i+1]:02x}" for i in range(0, 16, 2))
        else:
            raise ValueError(f"unknown address type: {addr_type}")
    except IndexError:
        raise ValueError("malformed VLESS header: chunk too short for declared fields")
    return command, address, port, first_chunk[pos:]

async def check_quota(uid: str, extra_bytes: int) -> bool:
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None or not link["active"]:
            return False
        expires_at = parse_expires_at(link.get("expires_at"))
        if expires_at is not None and expires_at < datetime.now(timezone.utc):
            return False
        if link["limit_bytes"] == 0:
            return True
        return (link["used_bytes"] + extra_bytes) <= link["limit_bytes"]

async def add_usage(uid: str, n: int):
    async with LINKS_LOCK:
        if uid in LINKS:
            LINKS[uid]["used_bytes"] += n

# drain wrapped in try/except, writer checked before use
async def ws_to_tcp(websocket, writer, conn_id, link_uid):
    try:
        while True:
            msg = await websocket.receive()
            if msg["type"] == "websocket.disconnect":
                break
            data = msg.get("bytes") or (msg.get("text") or "").encode()
            if not data:
                continue
            size = len(data)
            if not await check_quota(link_uid, size):
                await websocket.close(code=1008, reason="quota exceeded")
                break
            stats["total_bytes"] += size
            stats["total_requests"] += 1
            async with connections_lock:
                if conn_id in connections:
                    connections[conn_id]["bytes"] += size
            hourly_traffic[datetime.now(timezone.utc).strftime("%H:00")] += size
            daily_traffic[datetime.now(timezone.utc).strftime("%Y-%m-%d")] += size
            await add_usage(link_uid, size)
            try:
                writer.write(data)
                await writer.drain()   # drain inside try
            except Exception:
                break
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        # write_eof guarded for safety
        try:
            if not writer.is_closing():
                writer.write_eof()
        except Exception:
            pass

async def tcp_to_ws(websocket, reader, conn_id, link_uid):
    first = True
    try:
        while True:
            data = await reader.read(RELAY_BUF)
            if not data:
                break
            size = len(data)
            if not await check_quota(link_uid, size):
                await websocket.close(code=1008, reason="quota exceeded")
                break
            stats["total_bytes"] += size
            async with connections_lock:
                if conn_id in connections:
                    connections[conn_id]["bytes"] += size
            hourly_traffic[datetime.now(timezone.utc).strftime("%H:00")] += size
            daily_traffic[datetime.now(timezone.utc).strftime("%Y-%m-%d")] += size
            await add_usage(link_uid, size)
            try:
                await websocket.send_bytes((b"\x00\x00" + data) if first else data)
                first = False
            except Exception:
                break
    except Exception:
        pass

@app.websocket("/ws/{uuid}")
async def websocket_tunnel(websocket: WebSocket, uuid: str):
    await ensure_default_link()
    await websocket.accept()
    writer = None
    conn_id = None
    client_ip = get_client_ip(websocket)
    try:
        async with LINKS_LOCK:
            link_data = LINKS.get(uuid)
            if link_data is None or not link_data["active"]:
                await websocket.close(code=1008, reason="link not found or disabled")
                return
            max_conn = link_data.get("max_connections", 0)
            link_data_copy = dict(link_data)

        expires_at = parse_expires_at(link_data_copy.get("expires_at"))
        if expires_at is not None and expires_at < datetime.now(timezone.utc):
            await websocket.close(code=1008, reason="link expired")
            return

        # check connection limit under lock
        if max_conn > 0:
            current_conns = await count_connections_for_link(uuid)
            if current_conns >= max_conn:
                await websocket.close(code=1008, reason="connection limit reached")
                return

        first_msg = await asyncio.wait_for(websocket.receive(), timeout=15.0)
        if first_msg["type"] == "websocket.disconnect":
            return
        first_chunk = first_msg.get("bytes") or (first_msg.get("text") or "").encode()
        if not first_chunk:
            return

        try:
            command, address, port, initial_payload = await parse_vless_header(first_chunk)
        except ValueError as e:
            logger.warning(f"Invalid VLESS header: {e}")
            await websocket.close(code=1008, reason="invalid header")
            return

        conn_id = secrets.token_urlsafe(8)
        async with connections_lock:
            connections[conn_id] = {
                "uuid": uuid, "ip": client_ip,
                "connected_at": datetime.now(timezone.utc).isoformat(),
                "bytes": 0,
            }
            connection_sockets[conn_id] = websocket
            link_ip_map[uuid].add(client_ip)

        size = len(first_chunk)
        stats["total_bytes"] += size
        stats["total_requests"] += 1
        async with connections_lock:
            if conn_id in connections:
                connections[conn_id]["bytes"] += size
        hourly_traffic[datetime.now(timezone.utc).strftime("%H:00")] += size
        daily_traffic[datetime.now(timezone.utc).strftime("%Y-%m-%d")] += size
        await add_usage(uuid, size)

        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(address, port), timeout=10.0
        )

        if initial_payload:
            p_size = len(initial_payload)
            stats["total_bytes"] += p_size
            async with connections_lock:
                if conn_id in connections:
                    connections[conn_id]["bytes"] += p_size
            hourly_traffic[datetime.now(timezone.utc).strftime("%H:00")] += p_size
            daily_traffic[datetime.now(timezone.utc).strftime("%Y-%m-%d")] += p_size
            await add_usage(uuid, p_size)
            try:
                writer.write(initial_payload)
                await writer.drain()   # safe drain
            except Exception:
                pass

        task_up = asyncio.create_task(ws_to_tcp(websocket, writer, conn_id, uuid))
        task_down = asyncio.create_task(tcp_to_ws(websocket, reader, conn_id, uuid))
        done, pending = await asyncio.wait({task_up, task_down}, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        stats["total_errors"] += 1
        error_logs.append({"error": str(exc), "time": datetime.now(timezone.utc).isoformat()})
        logger.exception("WebSocket error")
    finally:
        if writer:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        if conn_id:
            async with connections_lock:
                info = connections.pop(conn_id, None)
                connection_sockets.pop(conn_id, None)
                if info:
                    uid = info.get("uuid")
                    ip = info.get("ip")
                    if uid and ip:
                        has_other = any(
                            c.get("uuid") == uid and c.get("ip") == ip
                            for c in connections.values()
                        )
                        if not has_other:
                            if uid in link_ip_map:
                                link_ip_map[uid].discard(ip)
                                if not link_ip_map[uid]:
                                    link_ip_map.pop(uid, None)

# ── HTML ──────────────────────────────────────────────────────────────────────
PANEL_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>Luffy Panel</title>
<link href="https://fonts.googleapis.com/css2?family=Sora:wght@500;600;700;800&family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@500;600;700&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{
  --bg:#0A0A0B;--surface:#121214;--surface2:#19191C;--surface3:#212124;
  --border:rgba(255,255,255,.08);--border2:rgba(255,255,255,.16);
  --text:#F2F2F3;--text2:#A0A0A8;--text3:#65656E;
  --accent:#6366F1;--accent-dim:rgba(99,102,241,.14);--accent-hover:#818CF8;--accent-glow:0 0 0 3px rgba(99,102,241,.16);
  --green:#22C55E;--green-dim:rgba(34,197,94,.13);
  --red:#F43F5E;--red-dim:rgba(244,63,94,.13);
  --yellow:#F5A524;--yellow-dim:rgba(245,165,36,.13);
  --purple:#A78BFA;--purple-dim:rgba(167,139,250,.13);
  --shadow-sm:0 1px 2px rgba(0,0,0,.4);--shadow-md:0 8px 24px rgba(0,0,0,.35);
  --nav-h:64px;--radius:12px;--radius-lg:16px;
}
body.light-mode{
  --bg:#FAFAFA;--surface:#FFFFFF;--surface2:#FFFFFF;--surface3:#F4F4F6;
  --border:rgba(15,15,20,.08);--border2:rgba(15,15,20,.14);
  --text:#16171B;--text2:#55565F;--text3:#9596A0;
  --accent:#4F46E5;--accent-dim:rgba(79,70,229,.08);--accent-hover:#6366F1;--accent-glow:0 0 0 3px rgba(79,70,229,.1);
  --shadow-sm:0 1px 2px rgba(15,15,20,.05);--shadow-md:0 12px 28px rgba(15,15,20,.07);
}
html,body{height:100%;background:var(--bg);transition:background .3s,color .3s}
body{font-family:'Inter',sans-serif;color:var(--text);display:flex;flex-direction:column;min-height:100vh}
::-webkit-scrollbar{width:5px;height:5px}::-webkit-scrollbar-thumb{background:var(--border2);border-radius:4px}
.bg-fixed{position:fixed;inset:0;z-index:0;pointer-events:none;background:radial-gradient(ellipse 60% 40% at 50% -5%,var(--accent-dim),transparent 65%)}
.grid-fixed{display:none}

/* Top navigation */
.sidebar{position:fixed;top:0;left:0;right:0;height:var(--nav-h);background:color-mix(in srgb,var(--surface) 88%,transparent);border-bottom:1px solid var(--border);display:flex;align-items:center;gap:6px;padding:0 22px;z-index:100;backdrop-filter:blur(18px);-webkit-backdrop-filter:blur(18px)}
.sb-brand{display:flex;align-items:center;gap:10px;margin-right:8px;flex-shrink:0}
.sb-hat{width:30px;height:30px;border-radius:9px;background:linear-gradient(155deg,var(--accent),#4338CA);display:flex;align-items:center;justify-content:center;color:#fff;font-family:'Sora',sans-serif;font-weight:800;font-size:14px;box-shadow:0 3px 10px rgba(99,102,241,.35);flex-shrink:0}
.sb-title{font-family:'Sora',sans-serif;font-size:14px;font-weight:700;color:var(--text);letter-spacing:-.01em;white-space:nowrap}
.sb-nav{flex:1;display:flex;flex-direction:row;align-items:center;gap:2px;height:100%}
.nav-item{display:flex;flex-direction:row;align-items:center;justify-content:center;gap:7px;padding:0 14px;height:38px;margin-top:1px;border-radius:9px;color:var(--text3);cursor:pointer;transition:all .18s ease;border:none;position:relative;text-decoration:none;background:none;font-family:'Inter',inherit}
.nav-item:hover{color:var(--text);background:var(--surface3)}
.nav-item.active{color:var(--accent);background:var(--accent-dim)}
.nav-icon{width:16px;height:16px;flex-shrink:0}
.nav-label{font-size:12.5px;font-weight:600;white-space:nowrap}
.nav-badge{background:var(--accent);color:#fff;font-size:10px;font-weight:700;min-width:16px;height:16px;border-radius:8px;display:flex;align-items:center;justify-content:center;padding:0 4px;margin-left:1px}
.nav-item.active .nav-badge{background:var(--accent)}
.sb-bottom{display:flex;align-items:center;gap:8px;flex-shrink:0}
.logout-btn{display:flex;align-items:center;justify-content:center;gap:5px;padding:7px 11px;border:1px solid var(--border);border-radius:8px;background:var(--surface3);color:var(--text2);cursor:pointer;transition:all .15s;font-size:11.5px;font-weight:600;font-family:inherit}
.logout-btn:hover{background:var(--red-dim);border-color:rgba(244,63,94,.25);color:var(--red)}
.theme-toggle{background:var(--surface3);border:1px solid var(--border);color:var(--text2);border-radius:8px;width:32px;height:32px;cursor:pointer;display:flex;align-items:center;justify-content:center;transition:all .15s;font-size:14px}
.theme-toggle:hover{color:var(--accent);border-color:var(--accent)}

.main{margin-top:var(--nav-h);flex:1;width:100%;max-width:1180px;margin-left:auto;margin-right:auto;padding:28px 28px 60px;position:relative;z-index:1}
.page{display:none;animation:pgIn .3s ease}
.page.active{display:block}
@keyframes pgIn{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}
.page-header{margin-bottom:22px;display:flex;align-items:flex-end;justify-content:space-between;flex-wrap:wrap;gap:10px}
.page-title{font-family:'Sora',sans-serif;font-size:21px;font-weight:700;color:var(--text);letter-spacing:-.02em}
.page-sub{font-size:12.5px;color:var(--text3);margin-top:4px}

.stats-row{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:14px}
.stat-card{background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-lg);padding:18px;position:relative;overflow:hidden;transition:all .2s;animation:cIn .4s ease both;box-shadow:var(--shadow-sm)}
.stat-card:hover{border-color:var(--border2);transform:translateY(-2px);box-shadow:var(--shadow-md)}
@keyframes cIn{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:none}}
.stat-label{font-size:10.5px;color:var(--text3);font-weight:600;text-transform:uppercase;letter-spacing:.07em;margin-bottom:10px}
.stat-val{font-family:'JetBrains Mono',monospace;font-size:21px;font-weight:600;color:var(--text);letter-spacing:-.02em}
.stat-unit{font-size:11.5px;font-weight:500;color:var(--text3);font-family:'Inter',sans-serif}
.card{background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-lg);padding:18px;margin-bottom:12px;position:relative;overflow:hidden;transition:all .2s;animation:cIn .4s ease both;box-shadow:var(--shadow-sm)}
.card-hd{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px}
.card-title{font-family:'Sora',sans-serif;font-size:13.5px;font-weight:600;color:var(--text);display:flex;align-items:center;gap:7px;letter-spacing:-.01em}
.chart-container{height:170px;width:100%}

.btn{font-family:inherit;font-size:12px;font-weight:600;border-radius:9px;padding:8px 15px;cursor:pointer;display:inline-flex;align-items:center;gap:6px;border:1px solid transparent;transition:all .15s}
.btn-gold{background:var(--accent);color:#fff;box-shadow:0 2px 8px rgba(99,102,241,.3)}
.btn-gold:hover{background:var(--accent-hover);transform:translateY(-1px);box-shadow:0 4px 14px rgba(99,102,241,.4)}
.btn-ghost{background:var(--surface3);color:var(--text);border-color:var(--border)}
.btn-ghost:hover{border-color:var(--border2)}
.btn-danger{background:var(--red-dim);color:var(--red);border-color:rgba(244,63,94,.2)}
.btn-sm{padding:5px 10px;font-size:11px}
.grid-2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.tbl-wrap{overflow-x:auto;border-radius:var(--radius);border:1px solid var(--border)}
.tbl{width:100%;border-collapse:collapse}
.tbl th{text-align:left;font-size:10px;font-weight:700;color:var(--text3);padding:11px 12px;text-transform:uppercase;letter-spacing:.06em;border-bottom:1px solid var(--border);background:var(--surface3)}
.tbl td{padding:11px 12px;border-bottom:1px solid var(--border);font-size:12.5px;vertical-align:middle}
.tbl tr:last-child td{border-bottom:none}
.tbl tr:hover td{background:var(--surface3)}
.tag{display:inline-flex;align-items:center;padding:3px 8px;border-radius:6px;font-size:9.5px;font-weight:700;letter-spacing:.04em;text-transform:uppercase}
.tag-vless{background:var(--accent-dim);color:var(--accent)}
.tag-on{background:var(--green-dim);color:var(--green)}
.tag-off{background:var(--red-dim);color:var(--red)}
.pill{display:flex;align-items:center;gap:8px;font-size:11px}
.pill-used{color:var(--text);font-weight:600;font-family:'JetBrains Mono',monospace;font-size:10.5px}
.pill-bar{flex:1;height:5px;background:var(--surface3);border-radius:3px;min-width:40px;overflow:hidden}
.pill-fill{height:100%;border-radius:3px;transition:width .4s}
.pill-lim{color:var(--text3);font-size:10px;font-family:'JetBrains Mono',monospace}
.toggle{width:34px;height:19px;border-radius:10px;background:var(--surface3);position:relative;cursor:pointer;transition:all .25s;border:1px solid var(--border);flex-shrink:0}
.toggle::after{content:'';position:absolute;width:13px;height:13px;border-radius:50%;background:var(--text3);top:2px;left:2px;transition:all .25s cubic-bezier(.4,0,.2,1)}
.toggle.on{background:var(--green);border-color:var(--green)}
.toggle.on::after{left:18px;background:#fff}
.sys-bar{height:7px;background:var(--surface3);border-radius:4px;overflow:hidden}
.sys-fill{height:100%;border-radius:4px;transition:width .4s}
.sl-item{display:flex;align-items:center;justify-content:space-between;padding:11px 0;border-bottom:1px solid var(--border)}
.sl-item:last-child{border-bottom:none}
.sl-k{color:var(--text3);font-size:12px}
.sl-v{color:var(--text);font-weight:600;font-size:12px;font-family:'JetBrains Mono',monospace}
.fg{display:flex;flex-direction:column;gap:5px;margin-bottom:12px}
.fl{font-size:10.5px;font-weight:600;color:var(--text2);text-transform:uppercase;letter-spacing:.06em}
.fi,.fs{padding:9px 12px;border-radius:9px;border:1px solid var(--border);font-family:inherit;font-size:13px;outline:none;color:var(--text);background:var(--surface);transition:all .15s}
.fi:focus,.fs:focus{border-color:var(--accent);box-shadow:var(--accent-glow)}
.fr{display:flex;gap:9px;flex-wrap:wrap;align-items:flex-end}
.fr .fg{margin-bottom:0;flex:1;min-width:90px}
.act-btn{font-family:inherit;font-size:10px;font-weight:600;border-radius:7px;padding:5px 9px;cursor:pointer;display:inline-flex;align-items:center;gap:3px;border:1px solid;transition:all .15s}
.act-copy{background:var(--accent-dim);color:var(--accent);border-color:transparent}
.act-sub{background:var(--green-dim);color:var(--green);border-color:transparent}
.act-qr{background:var(--purple-dim);color:var(--purple);border-color:transparent}
.act-edit{background:var(--yellow-dim);color:var(--yellow);border-color:transparent}
.act-del{background:var(--red-dim);color:var(--red);border-color:transparent}
.act-btn:hover{filter:brightness(1.15);transform:translateY(-1px)}
.toast{position:fixed;bottom:24px;left:50%;transform:translateX(-50%) translateY(16px);background:var(--surface2);color:var(--text);border:1px solid var(--border2);border-radius:10px;padding:12px 20px;font-size:13px;font-weight:500;opacity:0;transition:all .3s;z-index:999;backdrop-filter:blur(20px);box-shadow:var(--shadow-md)}
.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
.toast.err{border-color:rgba(244,63,94,.3)}
.mo{position:fixed;inset:0;background:rgba(8,8,10,.6);z-index:200;display:none;align-items:center;justify-content:center;backdrop-filter:blur(6px);padding:16px}
.mo.show{display:flex}
.mo-box{background:var(--surface2);border:1px solid var(--border2);border-radius:18px;padding:26px;width:100%;max-width:440px;position:relative;box-shadow:var(--shadow-md);transform:scale(.95) translateY(6px);opacity:0;transition:all .25s cubic-bezier(.16,1,.3,1)}
.mo.show .mo-box{transform:scale(1) translateY(0);opacity:1}
.mo-title{font-family:'Sora',sans-serif;font-size:15px;font-weight:700;margin-bottom:18px;color:var(--text);letter-spacing:-.01em}
.mo-close{position:absolute;top:14px;right:14px;background:var(--surface3);border:1px solid var(--border);color:var(--text3);width:28px;height:28px;border-radius:8px;cursor:pointer;display:flex;align-items:center;justify-content:center;font-size:13px}
.mo-close:hover{color:var(--text)}
.qr-box{text-align:center;padding:20px;background:var(--surface3);border-radius:var(--radius);border:1px solid var(--border);margin-top:12px}
.qr-box img{max-width:200px;border-radius:9px;border:1px solid var(--border)}
.tb{display:flex;align-items:center;gap:8px;margin-bottom:16px;flex-wrap:wrap}
.search-wrap{flex:1;min-width:160px;position:relative}
.search-wrap svg{position:absolute;left:12px;top:50%;transform:translateY(-50%);color:var(--text3)}
.search-wrap input{width:100%;padding:9px 12px 9px 34px;background:var(--surface2);border:1px solid var(--border);border-radius:9px;color:var(--text);font-size:13px;font-family:inherit;outline:none}
.search-wrap input:focus{border-color:var(--accent)}
.filter-chips{display:flex;gap:2px;padding:3px;background:var(--surface2);border:1px solid var(--border);border-radius:9px}
.chip{padding:6px 13px;border-radius:7px;font-size:11.5px;font-weight:600;color:var(--text3);cursor:pointer;border:none;background:none;transition:all .15s;font-family:inherit}
.chip.active{background:var(--accent);color:#fff}
.m-cards{display:none;flex-direction:column;gap:10px}
.m-card{border:1px solid var(--border);border-radius:var(--radius-lg);padding:16px;background:var(--surface2);box-shadow:var(--shadow-sm)}
.m-card-hd{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}
.m-card-acts{display:flex;gap:6px;flex-wrap:wrap;margin-top:12px}
.empty{text-align:center;padding:40px 20px;color:var(--text3);font-size:13px}
.mob-hd{display:none}
.mob-tl-group{display:flex;gap:10px;align-items:center}
.logout-mob{display:none}

/* Login page */
.login-wrap{display:flex;align-items:center;justify-content:center;min-height:100vh;width:100%;position:relative;z-index:1}
.login-box{background:var(--surface2);border:1px solid var(--border);border-radius:20px;padding:38px 34px;width:100%;max-width:380px;box-shadow:var(--shadow-md)}
.login-logo{text-align:center;margin-bottom:30px;display:flex;flex-direction:column;align-items:center}
.login-title{font-family:'Sora',sans-serif;font-size:19px;font-weight:700;color:var(--text);letter-spacing:-.02em;margin-top:14px}
.login-sub{font-size:12px;color:var(--text3);margin-top:6px}

@media(max-width:768px){
  .mob-hd{display:flex;height:60px;padding:0 18px;position:fixed;top:0;left:0;right:0;background:color-mix(in srgb,var(--surface) 90%,transparent);border-bottom:1px solid var(--border);z-index:90;align-items:center;justify-content:space-between;backdrop-filter:blur(18px)}
  .theme-toggle{font-size:15px}
  .sidebar{top:auto;bottom:0;height:74px;border-bottom:none;border-top:1px solid var(--border);background:color-mix(in srgb,var(--surface) 94%,transparent);padding:0;box-shadow:0 -8px 24px rgba(0,0,0,.25)}
  .sb-brand,.sb-bottom{display:none !important}
  .sb-nav{height:100%;padding:0 4px;gap:0}
  .nav-item{flex:1;flex-direction:column;gap:4px;height:100%;border-radius:0;padding:0}
  .nav-item.active{background:none}
  .nav-icon{width:21px;height:21px}
  .nav-label{font-size:10px}
  .nav-badge{position:absolute;top:8px;left:calc(50% + 10px);min-width:15px;height:15px;font-size:9px}
  .logout-mob{display:flex}
  .main{margin-top:60px;padding:18px 16px 100px}
  .page-title{font-size:22px}
  .page-sub{font-size:12.5px}
  .btn{font-size:13px;padding:10px 16px}
  .btn-sm{font-size:12px;padding:8px 13px}
  .stats-row{grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px}
  .stat-card{padding:18px;border-radius:var(--radius-lg)}
  .stat-val{font-size:22px}
  .grid-2{grid-template-columns:1fr;gap:12px;margin-bottom:12px}
  .card{padding:18px}
  .card-title{font-size:14.5px}
  .chart-container{height:210px}
  .sl-k,.sl-v{font-size:13px}
  .tbl-wrap{display:none}
  .m-cards{display:flex}
  .m-card-acts .act-btn{font-size:11.5px;padding:7px 12px}
  .mo-box{padding:26px 22px}
  .fi,.fs{font-size:16px;padding:11px 14px}
}
@media(max-width:460px){.stats-row{grid-template-columns:1fr;gap:12px}}

</style>
</head>
<body>
<div class="bg-fixed"></div>
<div class="grid-fixed"></div>
<div class="toast" id="toast"></div>

<!-- LOGIN PAGE (shown when not authenticated) -->
<div id="login-page" style="display:none;width:100%">
  <div class="login-wrap">
    <div class="login-box">
      <div class="login-logo">
        <div class="sb-hat" style="width:44px;height:44px;border-radius:13px;font-size:19px">L</div>
        <div class="login-title">Luffy Panel</div>
        <div class="login-sub">Enter your password to continue</div>
      </div>
      <div class="fg">
        <label class="fl">PASSWORD</label>
        <input class="fi" type="password" id="login-pw" placeholder="••••••••" onkeydown="if(event.key==='Enter')doLogin()">
      </div>
      <button class="btn btn-gold" onclick="doLogin()" style="width:100%;justify-content:center;padding:12px;margin-top:6px;">LOGIN</button>
      <div id="login-err" style="color:var(--red);font-size:12px;margin-top:10px;text-align:center;display:none">Invalid password</div>
    </div>
  </div>
</div>

<!-- DASHBOARD (shown when authenticated) -->
<div id="dashboard-page" style="display:none;width:100%">

  <!-- MOBILE HEADER -->
  <div class="mob-hd">
    <div class="mob-tl-group">
      <button class="theme-toggle" onclick="toggleTheme()" id="theme-btn-mob">🌙</button>
    </div>
    <span style="font-family:'Sora',sans-serif;font-size:15px;font-weight:700;color:var(--text);letter-spacing:-.01em;">Luffy Panel</span>
  </div>

  <!-- SIDEBAR / BOTTOM NAV -->
  <aside class="sidebar" id="sb">
    <div class="sb-brand">
      <div class="sb-hat">L</div>
      <div class="sb-title">Luffy Panel</div>
    </div>
    <nav class="sb-nav">
      <button class="nav-item active" data-page="dashboard">
        <svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>
        <span class="nav-label">Dashboard</span>
      </button>
      <button class="nav-item" data-page="inbounds">
        <svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="9" cy="7" r="4"/><line x1="23" y1="11" x2="17" y2="11"/><line x1="20" y1="8" x2="20" y2="14"/></svg>
        <span class="nav-label">Inbounds</span>
        <span class="nav-badge" id="nb">0</span>
      </button>
      <button class="nav-item" data-page="traffic">
        <svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
        <span class="nav-label">Traffic</span>
      </button>
      <button class="nav-item" data-page="addresses">
        <svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 014 10 15.3 15.3 0 01-4 10 15.3 15.3 0 01-4-10 15.3 15.3 0 014-10z"/></svg>
        <span class="nav-label">Clean IP</span>
      </button>
      <button class="nav-item" data-page="security">
        <svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0110 0v4"/></svg>
        <span class="nav-label">Security</span>
      </button>
      <button class="nav-item logout-mob" onclick="doLogout()">
        <svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M9 21H5a2 2 0 01-2-2V5a2 2 0 012-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>
        <span class="nav-label">Logout</span>
      </button>
    </nav>
    <div class="sb-bottom">
      <button class="theme-toggle" onclick="toggleTheme()" id="theme-btn-desk" style="margin-bottom:4px;font-size:12px">🌙 Theme</button>
      <button class="logout-btn" onclick="doLogout()" style="margin-top:2px">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M9 21H5a2 2 0 01-2-2V5a2 2 0 012-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>
        <span>Logout</span>
      </button>
    </div>
  </aside>

  <!-- MAIN CONTENT -->
  <main class="main">

    <!-- Dashboard -->
    <section class="page active" id="page-dashboard">
      <div class="page-header">
        <div>
          <div class="page-title">Dashboard</div>
          <div class="page-sub" id="last-up">–</div>
        </div>
        <div style="display:flex;gap:6px">
          <button class="btn btn-ghost btn-sm" onclick="qCreate(.5,'GB')">+ 0.5 GB</button>
          <button class="btn btn-gold btn-sm" onclick="qCreate(1,'GB')">+ 1 GB</button>
        </div>
      </div>
      <div class="stats-row">
        <div class="stat-card" style="animation-delay:.08s"><div class="stat-label">Traffic</div><div class="stat-val" id="sv-traffic">–<span class="stat-unit"> MB</span></div></div>
        <div class="stat-card" style="animation-delay:.16s"><div class="stat-label">Inbounds</div><div class="stat-val" id="sv-links">–</div></div>
        <div class="stat-card" style="animation-delay:.24s"><div class="stat-label">Uptime</div><div class="stat-val" id="sv-uptime" style="font-size:15px">–</div></div>
        <div class="stat-card" style="animation-delay:.32s"><div class="stat-label">Domain</div><div class="stat-val" id="sv-domain" style="font-size:10px;word-break:break-all;font-weight:500">–</div></div>
      </div>
      <div class="grid-2">
        <div class="card">
          <div class="card-hd"><div class="card-title">CPU</div><span id="cpu-v" style="font-size:17px;font-weight:700;color:var(--accent)">–%</span></div>
          <div class="sys-bar"><div class="sys-fill" id="cpu-b" style="background:var(--accent)"></div></div>
        </div>
        <div class="card">
          <div class="card-hd"><div class="card-title">Memory</div><span id="mem-v" style="font-size:17px;font-weight:700;color:var(--green)">–%</span></div>
          <div class="sys-bar"><div class="sys-fill" id="mem-b" style="background:var(--green)"></div></div>
        </div>
      </div>
      <div class="card">
        <div class="card-hd"><div class="card-title">Hourly Traffic</div></div>
        <div class="chart-container"><canvas id="tc"></canvas></div>
      </div>
    </section>

    <!-- Inbounds -->
    <section class="page" id="page-inbounds">
      <div class="page-header">
        <div>
          <div class="page-title">Inbounds</div>
          <div class="page-sub">VLESS over WebSocket · TLS</div>
        </div>
        <button class="btn btn-ghost" onclick="showBulkMo()">⚡ Bulk Create</button>
        <button class="btn btn-gold" onclick="showAddMo()">+ Add</button>
      </div>
      <div class="tb">
        <div class="search-wrap">
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
          <input id="srch" placeholder="Search name…" oninput="filterLinks()">
        </div>
        <div class="filter-chips">
          <button class="chip active" data-filter="all" onclick="setFilter('all',this)">All</button>
          <button class="chip" data-filter="active" onclick="setFilter('active',this)">Active</button>
          <button class="chip" data-filter="off" onclick="setFilter('off',this)">Off</button>
        </div>
      </div>
      <div class="card" style="padding:0;overflow:hidden">
        <div class="tbl-wrap">
          <table class="tbl">
            <thead><tr>
              <th>#</th>
              <th>Name</th>
              <th>Type</th>
              <th>Usage</th>
              <th>IPs</th>
              <th>Expiry</th>
              <th>Status</th>
              <th>Actions</th>
            </tr></thead>
            <tbody id="ltb"></tbody>
          </table>
        </div>
        <div class="m-cards" id="mcards"></div>
        <div class="empty" id="lempty" style="display:none">No inbounds found</div>
      </div>
    </section>


    <section class="page" id="page-traffic">
      <div class="page-header"><div><div class="page-title">Traffic</div><div class="page-sub">Statistics</div></div></div>
      <div class="card">
        <div class="sl-item"><span class="sl-k">Total Traffic</span><span class="sl-v" id="t-tr">–</span></div>
        <div class="sl-item"><span class="sl-k">Total Requests</span><span class="sl-v" id="t-rq">–</span></div>
        <div class="sl-item"><span class="sl-k">Uptime</span><span class="sl-v" id="t-up">–</span></div>
      </div>
    </section>

    <!-- Clean IP -->
    <section class="page" id="page-addresses">
      <div class="page-header">
        <div><div class="page-title">Clean IP</div><div class="page-sub">Subscription alternative addresses</div></div>
        <div style="display:flex;gap:8px">
          <button class="btn btn-ghost" id="ping-btn" onclick="pingAddrs()">⚡ Test Speed</button>
          <button class="btn btn-gold" onclick="showAddAddrMo()">+ Add</button>
        </div>
      </div>
      <div class="card">
        <div style="font-size:12px;color:var(--text3);margin-bottom:12px">Default: www.speedtest.net</div>
        <div id="addr-list"></div>
      </div>
    </section>

    <!-- Security -->
    <section class="page" id="page-security">
      <div class="page-header"><div><div class="page-title">Security</div><div class="page-sub">Change panel password</div></div></div>
      <div class="card" style="max-width:380px">
        <div class="fg"><label class="fl">Current Password</label><input class="fi" type="password" id="cpw" placeholder="Current password"></div>
        <div class="fg"><label class="fl">New Password</label><input class="fi" type="password" id="npw" placeholder="Min 4 chars"></div>
        <button class="btn btn-gold" onclick="chgPw()" style="margin-top:10px;width:100%;justify-content:center;">Update Password</button>
      </div>
      <div class="card" style="max-width:380px">
        <div class="card-hd"><div class="card-title">Backup &amp; Restore</div></div>
        <div style="font-size:11.5px;color:var(--text3);margin-bottom:12px">Inbound data is stored on disk, but exporting a backup protects you against accidental loss.</div>
        <button class="btn btn-ghost" onclick="downloadBackup()" style="width:100%;justify-content:center;margin-bottom:8px;">⬇ Export Backup</button>
        <input type="file" id="restore-file" accept="application/json" style="display:none" onchange="restoreBackup(this.files[0])">
        <button class="btn btn-ghost" onclick="document.getElementById('restore-file').click()" style="width:100%;justify-content:center;">⬆ Restore From File</button>
      </div>
    </section>

  </main>
</div><!-- /dashboard-page -->

<!-- Modals -->
<div class="mo" id="mo-add" onclick="if(event.target===this)this.classList.remove('show')">
  <div class="mo-box">
    <button class="mo-close" onclick="document.getElementById('mo-add').classList.remove('show')">✕</button>
    <div class="mo-title">ADD INBOUND</div>
    <div class="fg"><label class="fl">Remark</label><input class="fi" id="nl" placeholder="e.g. User 1"></div>
    <div class="fr">
      <div class="fg"><label class="fl">Traffic Limit</label><input class="fi" id="nv" type="number" min="0" step=".1" placeholder="0 = ∞"></div>
      <div class="fg" style="max-width:100px"><label class="fl">Unit</label><select class="fs" id="nu"><option>GB</option></select></div>
    </div>
    <div class="fg"><label class="fl">Max IPs</label><input class="fi" id="nc" type="number" min="0" placeholder="0 = ∞"></div>
    <div class="fg"><label class="fl">Days Valid</label><input class="fi" id="nd" type="number" min="0" placeholder="0 = No expiry"></div>
    <button class="btn btn-gold" onclick="createLink()" style="width:100%;justify-content:center;margin-top:12px;padding:12px;">CREATE</button>
  </div>
</div>

<div class="mo" id="mo-bulk" onclick="if(event.target===this)this.classList.remove('show')">
  <div class="mo-box">
    <button class="mo-close" onclick="document.getElementById('mo-bulk').classList.remove('show')">✕</button>
    <div class="mo-title">BULK CREATE</div>
    <div class="fr">
      <div class="fg"><label class="fl">Name Prefix</label><input class="fi" id="bp" placeholder="e.g. User" value="User"></div>
      <div class="fg" style="max-width:90px"><label class="fl">Count</label><input class="fi" id="bn" type="number" min="1" max="200" value="5"></div>
    </div>
    <div class="fr">
      <div class="fg"><label class="fl">Traffic Limit</label><input class="fi" id="bv" type="number" min="0" step=".1" placeholder="0 = ∞"></div>
      <div class="fg" style="max-width:100px"><label class="fl">Unit</label><select class="fs" id="bu"><option>GB</option></select></div>
    </div>
    <div class="fg"><label class="fl">Max IPs</label><input class="fi" id="bc" type="number" min="0" placeholder="0 = ∞"></div>
    <div class="fg"><label class="fl">Days Valid</label><input class="fi" id="bd" type="number" min="0" placeholder="0 = No expiry"></div>
    <button class="btn btn-gold" onclick="bulkCreate()" style="width:100%;justify-content:center;margin-top:12px;padding:12px;">CREATE ALL</button>
  </div>
</div>

<div class="mo" id="mo-edit" onclick="if(event.target===this)this.classList.remove('show')">
  <div class="mo-box">
    <button class="mo-close" onclick="document.getElementById('mo-edit').classList.remove('show')">✕</button>
    <div class="mo-title" id="et">EDIT INBOUND</div>
    <input type="hidden" id="eu">
    <div class="fg"><label class="fl">Name</label><input class="fi" id="en2" readonly style="opacity:.5;cursor:not-allowed"></div>
    <div class="fr">
      <div class="fg"><label class="fl">Traffic Limit</label><input class="fi" id="el" type="number" min="0" step=".1" placeholder="0 = ∞"></div>
      <div class="fg" style="max-width:100px"><label class="fl">Unit</label><select class="fs" id="eu2"><option>GB</option></select></div>
    </div>
    <div class="fg"><label class="fl">Max IPs</label><input class="fi" id="ec" type="number" min="0" placeholder="0 = ∞"></div>
    <div class="fg"><label class="fl">Extend Days</label><input class="fi" id="ed" type="number" min="0" placeholder="0 = no change"></div>
    <div style="display:flex;gap:10px;margin-top:16px">
      <button class="btn btn-gold" onclick="saveEdit()" style="flex:1;justify-content:center;padding:12px;">SAVE</button>
      <button class="btn btn-danger" onclick="resetTraf()" style="padding:12px;">Reset Traffic</button>
    </div>
  </div>
</div>

<div class="mo" id="mo-qr" onclick="if(event.target===this)this.classList.remove('show')">
  <div class="mo-box" style="max-width:340px">
    <button class="mo-close" onclick="document.getElementById('mo-qr').classList.remove('show')">✕</button>
    <div class="mo-title">QR CODE</div>
    <div class="qr-box"><img id="qr-img" src="" alt="QR"></div>
    <div style="display:flex;gap:10px;margin-top:16px;justify-content:center">
      <button class="btn btn-gold btn-sm" onclick="dlQR()" style="padding:10px 16px;">Download</button>
      <button class="btn btn-ghost btn-sm" onclick="document.getElementById('mo-qr').classList.remove('show')" style="padding:10px 16px;">Close</button>
    </div>
  </div>
</div>

<div class="mo" id="mo-addr" onclick="if(event.target===this)this.classList.remove('show')">
  <div class="mo-box">
    <button class="mo-close" onclick="document.getElementById('mo-addr').classList.remove('show')">✕</button>
    <div class="mo-title">ADD CLEAN IP</div>
    <div class="fg"><label class="fl">IPs / Domains (one per line)</label><textarea class="fi" id="na" rows="5" placeholder="8.8.8.8&#10;example.com" style="resize:vertical;font-family:monospace"></textarea></div>
    <button class="btn btn-gold" onclick="addAddrs()" style="width:100%;justify-content:center;margin-top:12px;padding:12px;">ADD ALL</button>
  </div>
</div>

<script>
function $(s){return document.querySelector(s);}
function $m(id){return document.getElementById(id);}
function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');}

let theme=localStorage.getItem('theme')||'dark';
let allLinks=[];
let cf='all';
let sData={};
let tChart=null;
let allAddrs=[];
let pingResults={};
let isAuthenticated=false;

// ── Theme ────────────────────────────────────────────────────────────────────
function setTheme(t){
  theme=t;
  if(t==='light')document.body.classList.add('light-mode');
  else document.body.classList.remove('light-mode');
  localStorage.setItem('theme',t);
  const icon=t==='light'?'☀️':'🌙';
  const mb=$m('theme-btn-mob');
  const db=$m('theme-btn-desk');
  if(mb)mb.innerHTML=icon;
  if(db)db.innerHTML=icon+' Theme';
  updChartColors();
}
function toggleTheme(){setTheme(theme==='dark'?'light':'dark');}

// ── Auth ─────────────────────────────────────────────────────────────────────
async function checkAuth(){
  try{
    const r=await fetch('/api/me');
    const d=await r.json();
    if(d.authenticated){
      showDashboard();
    } else {
      showLogin();
    }
  } catch(e){showLogin();}
}

function showLogin(){
  isAuthenticated=false;
  $m('login-page').style.display='';
  $m('dashboard-page').style.display='none';
}

function showDashboard(){
  isAuthenticated=true;
  $m('login-page').style.display='none';
  $m('dashboard-page').style.display='';
  initChart();
  loadStats();
  loadLinks();
  loadAddrs();
}

async function doLogin(){
  const pw=$m('login-pw').value;
  $m('login-err').style.display='none';
  try{
    const r=await fetch('/api/login',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({password:pw})
    });
    if(r.ok){
      $m('login-pw').value='';
      showDashboard();
    } else {
      $m('login-err').style.display='block';
    }
  } catch(e){$m('login-err').style.display='block';}
}

async function doLogout(){
  await fetch('/api/logout',{method:'POST'});
  showLogin();
}

// ── Navigation ───────────────────────────────────────────────────────────────
document.querySelectorAll('.nav-item[data-page]').forEach(el=>{
  el.addEventListener('click',()=>switchPage(el.dataset.page));
});

function switchPage(id){
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  const target=$m('page-'+id);
  if(target)target.classList.add('active');
  document.querySelectorAll('.nav-item').forEach(n=>n.classList.toggle('active',n.dataset.page===id));
}

// ── Toast ─────────────────────────────────────────────────────────────────────
function toast(msg,err=false){
  const t=$m('toast');
  t.textContent=msg;
  t.className='toast'+(err?' err':'')+' show';
  clearTimeout(t._hide);
  t._hide=setTimeout(()=>t.classList.remove('show'),3000);
}

// ── Format helpers ────────────────────────────────────────────────────────────
function fmtB(b){
  if(!b||b===0)return'0 B';
  return b>=1073741824?(b/1073741824).toFixed(2)+' GB':
         b>=1048576?(b/1048576).toFixed(2)+' MB':
         (b/1024).toFixed(1)+' KB';
}
function fmtLim(b){
  if(!b||b===0)return'∞';
  const g=b/1073741824;
  return(g%1===0?g.toFixed(0):g.toFixed(1))+' GB';
}
function fmtExp(ea){
  if(!ea||ea===0)return'∞';
  const d=new Date(ea)-new Date();
  if(d<=0)return'Expired';
  const days=Math.floor(d/86400000);
  if(days>0)return days+'d';
  const hours=Math.floor(d/3600000);
  if(hours>0)return hours+'h';
  return Math.floor(d/60000)+'m';
}

// ── Links ─────────────────────────────────────────────────────────────────────
function setFilter(filter,el){
  cf=filter;
  document.querySelectorAll('.chip').forEach(c=>c.classList.remove('active'));
  if(el)el.classList.add('active');
  filterLinks();
}

function filterLinks(){
  const q=($m('srch')?.value||'').toLowerCase();
  let r=allLinks;
  if(cf==='active')r=r.filter(l=>l.active);
  else if(cf==='off')r=r.filter(l=>!l.active);
  if(q)r=r.filter(l=>l.label.toLowerCase().includes(q)||l.uuid.toLowerCase().includes(q));
  renderLinks(r);
}

function renderLinks(links){
  const tb=$m('ltb');
  const em=$m('lempty');
  const mc=$m('mcards');
  if(!links||!links.length){
    tb.innerHTML='';
    mc.innerHTML='';
    em.style.display='block';
    em.textContent='No inbounds found';
    return;
  }
  em.style.display='none';
  let idx=links.length;
  const rows=links.map(l=>{
    const u=l.used_bytes||0;
    const lim=l.limit_bytes||0;
    const pct=lim>0?Math.min(100,(u/lim)*100):0;
    const col=pct>90?'var(--red)':pct>70?'var(--yellow)':'var(--accent)';
    const ex=fmtExp(l.expires_at);
    const ec=ex==='Expired'?'var(--red)':ex==='∞'?'var(--text3)':'var(--text2)';
    const i=idx--;
    const cc=l.current_connections||0;
    const mc2=l.max_connections||0;
    return{l,pct,col,ex,ec,i,cc,mc2,u,lim};
  });

  const editText='Edit';
  const copyText='Copy';
  const subText='Sub';
  const qrText='QR';
  const delText='Del';
  const shareText='Share';
  const statusText='Status';

  tb.innerHTML=rows.map(r=>`<tr>
    <td style="color:var(--text3);font-size:10.5px">${r.i}</td>
    <td style="font-weight:600">${esc(r.l.label)}</td>
    <td><span class="tag tag-vless">VLESS</span></td>
    <td><div class="pill"><span class="pill-used">${fmtB(r.u)}</span><div class="pill-bar"><div class="pill-fill" style="width:${r.pct}%;background:${r.col}"></div></div><span class="pill-lim">${fmtLim(r.lim)}</span></div></td>
    <td style="font-size:11px;font-weight:600;color:${r.mc2>0&&r.cc>=r.mc2?'var(--red)':'var(--text2)'}">${r.cc}/${r.mc2||'∞'}</td>
    <td style="font-size:10.5px;font-weight:700;color:${r.ec}">${r.ex}</td>
    <td><span class="tag ${r.l.active?'tag-on':'tag-off'}">${r.l.active?'On':'Off'}</span></td>
    <td><div style="display:flex;gap:3px;align-items:center;flex-wrap:wrap">
      <button class="toggle ${r.l.active?'on':''}" data-uid="${r.l.uuid}" onclick="togLink(this)"></button>
      <button class="act-btn act-edit" onclick="showEditMo('${r.l.uuid}')">${editText}</button>
      <button class="act-btn act-copy" onclick="cpLink('${esc(r.l.vless_link||'')}')">${copyText}</button>
      <button class="act-btn act-sub" onclick="cpSub('${r.l.uuid}')">${subText}</button>
      <button class="act-btn act-qr" onclick="showQR('${esc(r.l.vless_link||'')}')">${qrText}</button>
      <button class="act-btn act-sub" style="background:var(--purple-dim);color:var(--purple)" onclick="shareLink('${r.l.uuid}')">${shareText}</button>
      <button class="act-btn act-copy" onclick="cpStatus('${r.l.uuid}')">${statusText}</button>
      <button class="act-btn act-del" onclick="delLink('${r.l.uuid}')">${delText}</button>
    </div></td>
  </tr>`).join('');

  mc.innerHTML=rows.map(r=>`<div class="m-card">
    <div class="m-card-hd">
      <div style="display:flex;align-items:center;gap:7px">
        <span style="font-size:11px;color:var(--text3)">#${r.i}</span>
        <span style="font-weight:600;font-size:14px">${esc(r.l.label)}</span>
        <span class="tag tag-vless">VLESS</span>
      </div>
      <button class="toggle ${r.l.active?'on':''}" data-uid="${r.l.uuid}" onclick="togLink(this)"></button>
    </div>
    <div class="pill"><span class="pill-used">${fmtB(r.u)}</span><div class="pill-bar"><div class="pill-fill" style="width:${r.pct}%;background:${r.col}"></div></div><span class="pill-lim">${fmtLim(r.lim)}</span></div>
    <div style="font-size:11.5px;color:${r.ec};margin-top:6px;font-weight:600">⏳ ${r.ex} · ${r.cc}/${r.mc2||'∞'} IPs</div>
    <div class="m-card-acts">
      <button class="act-btn act-edit" onclick="showEditMo('${r.l.uuid}')">${editText}</button>
      <button class="act-btn act-copy" onclick="cpLink('${esc(r.l.vless_link||'')}')">${copyText}</button>
      <button class="act-btn act-sub" onclick="cpSub('${r.l.uuid}')">${subText}</button>
      <button class="act-btn act-qr" onclick="showQR('${esc(r.l.vless_link||'')}')">${qrText}</button>
      <button class="act-btn act-sub" style="background:var(--purple-dim);color:var(--purple)" onclick="shareLink('${r.l.uuid}')">${shareText}</button>
      <button class="act-btn act-copy" onclick="cpStatus('${r.l.uuid}')">${statusText}</button>
      <button class="act-btn act-del" onclick="delLink('${r.l.uuid}')">${delText}</button>
    </div>
  </div>`).join('');
}

async function togLink(el){
  const uid=el.dataset.uid;
  const l=allLinks.find(x=>x.uuid===uid);
  if(!l)return;
  const na=!l.active;
  try{
    const r=await fetch('/api/links/'+uid,{
      method:'PATCH',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({active:na})
    });
    if(!r.ok)throw new Error();
    l.active=na;
    filterLinks();
    loadStats();
  }catch(e){toast('Failed to toggle',true);}
}

async function qCreate(v,u){
  const ns=['Ali','Sara','Reza','Nima','Mina','Arash'];
  const n=ns[Math.floor(Math.random()*ns.length)]+'-'+Math.floor(Math.random()*100);
  try{
    const r=await fetch('/api/links',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({label:n,limit_value:v,limit_unit:u})
    });
    if(!r.ok)throw new Error();
    toast('Created: '+n);
    await loadLinks();
    await loadStats();
  }catch(e){toast('Error creating link',true);}
}

function showAddMo(){$m('mo-add').classList.add('show');}

async function createLink(){
  const label=$m('nl').value.trim()||'New Link';
  if(!/^[a-zA-Z0-9\-_. ]+$/.test(label)){toast('Only English letters allowed',true);return;}
  const v=parseFloat($m('nv').value)||0;
  const mc=parseInt($m('nc').value)||0;
  const days=parseInt($m('nd').value)||0;
  try{
    const r=await fetch('/api/links',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({label,limit_value:v,limit_unit:'GB',max_connections:mc,days_valid:days})
    });
    if(!r.ok)throw new Error();
    toast('Created');
    $m('nl').value='';$m('nv').value='';$m('nc').value='';$m('nd').value='';
    $m('mo-add').classList.remove('show');
    await loadLinks();
    await loadStats();
  }catch(e){toast('Error creating link',true);}
}

function showBulkMo(){$m('mo-bulk').classList.add('show');}

async function bulkCreate(){
  const prefix=$m('bp').value.trim()||'User';
  if(!/^[a-zA-Z0-9\-_. ]+$/.test(prefix)){toast('Only English letters allowed',true);return;}
  const count=parseInt($m('bn').value)||0;
  if(count<1||count>200){toast('Count must be 1-200',true);return;}
  const v=parseFloat($m('bv').value)||0;
  const mc=parseInt($m('bc').value)||0;
  const days=parseInt($m('bd').value)||0;
  try{
    const r=await fetch('/api/links/bulk',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({prefix,count,limit_value:v,limit_unit:'GB',max_connections:mc,days_valid:days})
    });
    if(!r.ok){const d=await r.json().catch(()=>({}));throw new Error(d.detail||'Error');}
    const d=await r.json();
    toast('Created '+d.count+' inbounds');
    $m('bn').value='5';$m('bv').value='';$m('bc').value='';$m('bd').value='';
    $m('mo-bulk').classList.remove('show');
    await loadLinks();
    await loadStats();
  }catch(e){toast(e.message||'Error bulk creating',true);}
}

async function downloadBackup(){
  try{
    const r=await fetch('/api/backup');
    if(!r.ok)throw new Error();
    const blob=await r.blob();
    const url=URL.createObjectURL(blob);
    const a=document.createElement('a');
    a.href=url;a.download='luffy-backup-'+Date.now()+'.json';
    a.click();
    URL.revokeObjectURL(url);
    toast('Backup exported');
  }catch(e){toast('Error exporting backup',true);}
}

async function restoreBackup(file){
  if(!file)return;
  if(!confirm('This will replace all current inbounds and addresses. Continue?'))return;
  try{
    const text=await file.text();
    const data=JSON.parse(text);
    const r=await fetch('/api/restore',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify(data)
    });
    if(!r.ok){const d=await r.json().catch(()=>({}));throw new Error(d.detail||'Error');}
    const d=await r.json();
    toast('Restored '+d.restored_links+' inbounds');
    await loadLinks();
    await loadAddrs();
    await loadStats();
  }catch(e){toast(e.message||'Invalid backup file',true);}
  $m('restore-file').value='';
}

function showEditMo(uid){
  const l=allLinks.find(x=>x.uuid===uid);
  if(!l)return;
  $m('eu').value=uid;
  $m('en2').value=l.label;
  $m('el').value=l.limit_bytes>0?(l.limit_bytes/1073741824):'';
  $m('ec').value=l.max_connections>0?l.max_connections:'';
  $m('ed').value='';
  $m('et').textContent='EDIT: '+l.label;
  $m('mo-edit').classList.add('show');
}

async function saveEdit(){
  const uid=$m('eu').value;
  const v=parseFloat($m('el').value)||0;
  const mc=parseInt($m('ec').value)||0;
  const days=parseInt($m('ed').value)||0;
  const body={limit_value:v,limit_unit:'GB',max_connections:mc};
  if(days>0)body.days_valid=days;
  try{
    const r=await fetch('/api/links/'+uid,{
      method:'PATCH',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify(body)
    });
    if(!r.ok)throw new Error();
    toast('Updated');
    $m('mo-edit').classList.remove('show');
    await loadLinks();
  }catch(e){toast('Error updating',true);}
}

async function resetTraf(){
  const uid=$m('eu').value;
  if(!confirm('Reset traffic for this inbound?'))return;
  try{
    const r=await fetch('/api/links/'+uid,{
      method:'PATCH',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({reset_usage:true})
    });
    if(!r.ok)throw new Error();
    toast('Traffic reset');
    await loadLinks();
  }catch(e){toast('Error resetting',true);}
}

async function delLink(uid){
  if(!confirm('Delete this inbound?'))return;
  try{
    const r=await fetch('/api/links/'+uid,{method:'DELETE'});
    if(!r.ok)throw new Error();
    toast('Deleted');
    await loadLinks();
    await loadStats();
  }catch(e){toast('Error deleting',true);}
}

function cpLink(txt){
  if(!txt){toast('No link to copy',true);return;}
  navigator.clipboard.writeText(txt).then(()=>toast('Copied!')).catch(()=>toast('Failed to copy',true));
}

async function cpSub(uid){
  try{
    await navigator.clipboard.writeText('https://'+location.host+'/sub/'+uid);
    toast('Sub URL copied!');
  }catch(e){toast('Failed to copy',true);}
}

async function shareLink(uid){
  try{
    const r=await fetch('/api/links/'+uid+'/share',{method:'POST'});
    if(!r.ok)throw new Error();
    const d=await r.json();
    await navigator.clipboard.writeText(d.share_url);
    toast('One-time link copied — valid for a single view!');
  }catch(e){toast('Error creating share link',true);}
}

async function cpStatus(uid){
  try{
    await navigator.clipboard.writeText('https://'+location.host+'/status/'+uid);
    toast('Status page link copied!');
  }catch(e){toast('Failed to copy',true);}
}

function showQR(txt){
  if(!txt){toast('No QR data',true);return;}
  $m('qr-img').src='https://api.qrserver.com/v1/create-qr-code/?size=280x280&data='+encodeURIComponent(txt);
  $m('mo-qr').classList.add('show');
}

function dlQR(){
  const a=document.createElement('a');
  a.href=$m('qr-img').src;
  a.download='luffy-qr.png';
  a.click();
}

// ── Stats ─────────────────────────────────────────────────────────────────────
async function loadStats(){
  try{
    const r=await fetch('/stats');
    if(r.status===401){showLogin();return;}
    if(!r.ok)throw new Error();
    sData=await r.json();
    $m('sv-traffic').innerHTML=(sData.total_traffic_mb||0)+'<span class="stat-unit"> MB</span>';
    $m('sv-links').textContent=sData.links_count||0;
    $m('sv-uptime').textContent=sData.uptime||'–';
    $m('sv-domain').textContent=sData.domain||'–';
    $m('nb').textContent=sData.links_count||0;
    $m('last-up').textContent='Updated '+new Date().toLocaleTimeString();
    if($m('t-tr'))$m('t-tr').textContent=(sData.total_traffic_mb||0)+' MB';
    if($m('t-rq'))$m('t-rq').textContent=(sData.total_requests||0).toLocaleString();
    if($m('t-up'))$m('t-up').textContent=sData.uptime||'–';
    if(sData.cpu_percent!==undefined){
      const c=sData.cpu_percent;
      const cc=c>80?'var(--red)':c>50?'var(--yellow)':'var(--accent)';
      $m('cpu-v').textContent=c.toFixed(1)+'%';
      $m('cpu-v').style.color=cc;
      $m('cpu-b').style.width=c+'%';
      $m('cpu-b').style.background=cc;
    }
    if(sData.memory_percent!==undefined){
      const m=sData.memory_percent;
      const mc=m>80?'var(--red)':m>50?'var(--yellow)':'var(--green)';
      $m('mem-v').textContent=m.toFixed(1)+'%';
      $m('mem-v').style.color=mc;
      $m('mem-b').style.width=m+'%';
      $m('mem-b').style.background=mc;
    }
    updChart();
  }catch(e){/* silent */}
}

async function loadLinks(){
  try{
    const r=await fetch('/api/links');
    if(r.status===401){showLogin();return;}
    if(!r.ok)throw new Error();
    const d=await r.json();
    allLinks=d.links||[];
    filterLinks();
  }catch(e){/* silent */}
}

async function chgPw(){
  const cur=$m('cpw').value;
  const nw=$m('npw').value;
  if(!cur||!nw){toast('Fill all fields',true);return;}
  if(nw.length<4){toast('Password must be at least 4 characters',true);return;}
  try{
    const r=await fetch('/api/change-password',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({current_password:cur,new_password:nw})
    });
    if(!r.ok){
      const d=await r.json().catch(()=>({}));
      throw new Error(d.detail||'Error changing password');
    }
    toast('Password updated successfully');
    $m('cpw').value='';$m('npw').value='';
  }catch(e){toast(e.message,true);}
}

// ── Chart ─────────────────────────────────────────────────────────────────────
function initChart(){
  const ctx=$m('tc');
  if(!ctx||tChart)return;
  tChart=new Chart(ctx,{
    type:'bar',
    data:{
      labels:[],
      datasets:[{label:'MB',data:[],backgroundColor:'rgba(99,102,241,0.55)',borderColor:'#6366F1',borderWidth:1,borderRadius:4}]
    },
    options:{
      responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false}},
      scales:{
        x:{grid:{display:false},ticks:{color:'rgba(255,215,0,0.3)',font:{size:10}}},
        y:{grid:{color:'rgba(255,255,255,0.04)'},ticks:{color:'rgba(255,215,0,0.3)',font:{size:10},callback:v=>v+' MB'},beginAtZero:true}
      }
    }
  });
  updChartColors();
}

function updChartColors(){
  if(!tChart)return;
  const col=theme==='light'?'rgba(0,0,0,0.5)':'rgba(255,215,0,0.4)';
  const gridCol=theme==='light'?'rgba(0,0,0,0.08)':'rgba(255,255,255,0.06)';
  tChart.options.scales.x.ticks.color=col;
  tChart.options.scales.y.ticks.color=col;
  tChart.options.scales.y.grid.color=gridCol;
  tChart.update();
}

function updChart(){
  if(!tChart||!sData.hourly_traffic)return;
  const entries=Object.entries(sData.hourly_traffic)
    .sort((a,b)=>a[0].localeCompare(b[0])).slice(-12);
  tChart.data.labels=entries.map(x=>x[0]);
  tChart.data.datasets[0].data=entries.map(x=>Math.round(x[1]/1048576));
  tChart.update();
}

// ── Addresses ─────────────────────────────────────────────────────────────────
async function loadAddrs(){
  try{
    const r=await fetch('/api/addresses');
    if(!r.ok)throw new Error();
    const d=await r.json();
    allAddrs=d.addresses||[];
    renderAddrs();
  }catch(e){/* silent */}
}

function renderAddrs(){
  const el=$m('addr-list');
  if(!el)return;
  if(!allAddrs||!allAddrs.length){
    el.innerHTML='<div style="color:var(--text3);font-size:12px">No addresses added</div>';
    return;
  }
  const indexed=allAddrs.map((a,i)=>({a,i}));
  indexed.sort((x,y)=>{
    const px=pingResults[x.a],py=pingResults[y.a];
    const okx=px&&px.ok, oky=py&&py.ok;
    if(okx&&oky)return px.ms-py.ms;
    if(okx&&!oky)return -1;
    if(!okx&&oky)return 1;
    return x.i-y.i;
  });
  el.innerHTML=indexed.map(({a,i})=>{
    const p=pingResults[a];
    let badge='';
    if(p){
      if(p.pending){
        badge='<span class="tag" style="background:var(--surface3);color:var(--text3)">⏳ Testing…</span>';
      }else if(p.ok){
        const col=p.ms<150?'var(--green)':(p.ms<400?'var(--yellow)':'var(--red)');
        const dim=p.ms<150?'var(--green-dim)':(p.ms<400?'var(--yellow-dim)':'var(--red-dim)');
        badge=`<span class="tag" style="background:${dim};color:${col}">${p.ms} ms</span>`;
      }else{
        badge='<span class="tag" style="background:var(--red-dim);color:var(--red)">Failed</span>';
      }
    }
    return `<div style="display:flex;align-items:center;justify-content:space-between;padding:12px 14px;background:var(--surface3);border:1px solid var(--border);border-radius:10px;margin-bottom:8px">
    <div style="display:flex;align-items:center;gap:10px">
      <span style="color:var(--accent);font-size:16px">🌐</span>
      <div><div style="font-size:14px;font-weight:600">${esc(a)}</div><div style="font-size:11px;color:var(--text3);margin-top:2px;">Address #${i+1}</div></div>
    </div>
    <div style="display:flex;align-items:center;gap:8px">
      ${badge}
      <button class="act-btn act-del" onclick="delAddr(${i})">Del</button>
    </div>
  </div>`;
  }).join('');
}

async function pingAddrs(){
  if(!allAddrs||!allAddrs.length){toast('No addresses to test',true);return;}
  const btn=$m('ping-btn');
  if(btn){btn.disabled=true;btn.textContent='⏳ Testing…';}
  pingResults={};
  allAddrs.forEach(a=>{pingResults[a]={pending:true};});
  renderAddrs();
  try{
    const r=await fetch('/api/addresses/ping',{method:'POST'});
    if(!r.ok)throw new Error();
    const d=await r.json();
    pingResults={};
    (d.results||[]).forEach(res=>{pingResults[res.address]=res;});
    renderAddrs();
    const okCount=(d.results||[]).filter(x=>x.ok).length;
    toast(`Tested ${d.results.length} addresses · ${okCount} reachable`);
  }catch(e){
    toast('Speed test failed',true);
    pingResults={};
    renderAddrs();
  }finally{
    if(btn){btn.disabled=false;btn.textContent='⚡ Test Speed';}
  }
}

function showAddAddrMo(){$m('na').value='';$m('mo-addr').classList.add('show');}

async function addAddrs(){
  const lines=($m('na').value||'').trim().split('\n').map(l=>l.trim()).filter(l=>l);
  let ok=0,fail=0;
  for(const a of lines){
    if(!/^[a-zA-Z0-9\-_. ]+$/.test(a)){fail++;continue;}
    try{
      const r=await fetch('/api/addresses',{
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({address:a})
      });
      if(r.ok)ok++;else fail++;
    }catch(e){fail++;}
  }
  if(ok)toast('Added '+ok);
  if(fail)toast(fail+' failed',true);
  if(ok){$m('mo-addr').classList.remove('show');await loadAddrs();}
}

async function delAddr(i){
  if(!confirm('Delete this address?'))return;
  try{
    const r=await fetch('/api/addresses/'+i,{method:'DELETE'});
    if(!r.ok)throw new Error();
    toast('Deleted');
    await loadAddrs();
  }catch(e){toast('Error deleting',true);}
}

// ── Init ──────────────────────────────────────────────────────────────────────
setTheme(theme);
checkAuth();
let statsInterval=null;
function startPolling(){
  if(statsInterval)clearInterval(statsInterval);
  statsInterval=setInterval(()=>{if(isAuthenticated){loadStats();loadLinks();}},12000);
}
startPolling();
</script>
</body>
</html>"""

# Both routes serve PANEL_HTML
# login and dashboard are a single page; auth is handled client-side

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return HTMLResponse(content=PANEL_HTML)

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    return HTMLResponse(content=PANEL_HTML)

# Root panel route redirects to the same dashboard
@app.get("/panel", response_class=HTMLResponse)
async def panel_page(request: Request):
    return HTMLResponse(content=PANEL_HTML)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=CONFIG["port"])
