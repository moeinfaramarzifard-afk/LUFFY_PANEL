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

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

connections = {}
connections_lock = asyncio.Lock()
connection_sockets = {}
link_ip_map = defaultdict(set)
SHARE_TOKENS = {}
stats = {"total_bytes": 0, "total_requests": 0, "total_errors": 0, "start_time": time.time()}
error_logs = deque(maxlen=50)
hourly_traffic = defaultdict(int)
daily_traffic = defaultdict(int)
http_client = None

LINKS = {}
LINKS_LOCK = asyncio.Lock()
CUSTOM_ADDRESSES = ["www.speedtest.net"]
CUSTOM_ADDRESSES_LOCK = asyncio.Lock()

SESSION_COOKIE = "ren_session"
SESSION_TTL = 60 * 60 * 24 * 7
UNLIMITED_QUOTA_BYTES = 53687091200000

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
    while True:
        await asyncio.sleep(30)
        try:
            async with LINKS_LOCK:
                snapshot = {uid: dict(data) for uid, data in LINKS.items()}
            for uid, data in snapshot.items():
                await db_save_link(uid, data)
        except Exception:
            logger.exception("usage flush failed")

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

def hash_password(pw: str) -> str:
    return hashlib.sha256(f"{pw}{CONFIG['secret']}".encode()).hexdigest()

AUTH = {"password_hash": hash_password(os.environ.get("ADMIN_PASSWORD", "admin"))}
SESSIONS = {}
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

@app.get("/")
async def root():
    return {"service": "Luffy Panel", "version": "1.0", "status": "active", "domain": get_domain()}

@app.get("/health")
async def health():
    async with connections_lock:
        conn_count = len(connections)
    return {"status": "ok", "connections": conn_count, "uptime": uptime()}

@app.post("/api/login")
async def api_login(request: Request):
    body = await request.json()
    password = str(body.get("password") or "")
    if hash_password(password) != AUTH["password_hash"]:
        raise HTTPException(status_code=401, detail="Invalid password")
    token = await create_session()
    resp = JSONResponse({"ok": True})
    resp.set_cookie(key=SESSION_COOKIE, value=token, max_age=SESSION_TTL, httponly=True, samesite="lax", path="/")
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
    if hash_password(current) != AUTH["password_hash"]:
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
        raise HTTPException(status_code=400, detail="Inbound name error")
    async with LINKS_LOCK:
        if label in LINKS:
            raise HTTPException(status_code=400, detail="Already exists")
    limit_value = float(body.get("limit_value") or 0)
    limit_unit = body.get("limit_unit") or "GB"
    limit_bytes = 0 if limit_value <= 0 else parse_size_to_bytes(limit_value, limit_unit)
    max_conn = int(body.get("max_connections") or 0)
    days_valid = body.get("days_valid")
    expires_at = None
    if days_valid is not None:
        try:
            expires_at = (datetime.now(timezone.utc) + timedelta(days=int(days_valid))).isoformat()
        except Exception: pass
    uid = label
    async with LINKS_LOCK:
        LINKS[uid] = {
            "label": label, "limit_bytes": limit_bytes, "used_bytes": 0, "max_connections": max_conn,
            "created_at": datetime.now(timezone.utc).isoformat(), "active": True, "expires_at": expires_at
        }
    await db_save_link(uid, LINKS[uid])
    return {"uuid": uid, "label": label}

@app.post("/api/links/bulk")
async def create_links_bulk(request: Request, _=Depends(require_auth)):
    body = await request.json()
    prefix = (body.get("prefix") or "User").strip()[:40]
    count = int(body.get("count") or 0)
    limit_value = float(body.get("limit_value") or 0)
    limit_unit = body.get("limit_unit") or "GB"
    limit_bytes = 0 if limit_value <= 0 else parse_size_to_bytes(limit_value, limit_unit)
    max_conn = int(body.get("max_connections") or 0)
    days_valid = body.get("days_valid")
    expires_at = None
    if days_valid is not None:
        try: expires_at = (datetime.now(timezone.utc) + timedelta(days=int(days_valid))).isoformat()
        except Exception: pass
    created = []
    async with LINKS_LOCK:
        existing_max = 0
        for k in LINKS:
            m = re.match(rf'^{re.escape(prefix)}-(\d+)$', k)
            if m: existing_max = max(existing_max, int(m.group(1)))
        for i in range(1, count + 1):
            uid = f"{prefix}-{existing_max + i}"
            LINKS[uid] = {
                "label": uid, "limit_bytes": limit_bytes, "used_bytes": 0, "max_connections": max_conn,
                "created_at": datetime.now(timezone.utc).isoformat(), "active": True, "expires_at": expires_at
            }
            created.append(uid)
    for uid in created:
        await db_save_link(uid, LINKS[uid])
    return {"ok": True, "count": len(created)}

@app.get("/api/links")
async def list_links(_=Depends(require_auth)):
    result = []
    async with LINKS_LOCK:
        items = list(LINKS.items())
    for uid, data in items:
        result.append({
            "uuid": uid, "label": data["label"], "limit_bytes": data["limit_bytes"], "used_bytes": data["used_bytes"],
            "max_connections": data.get("max_connections", 0), "active": data["active"],
            "created_at": data["created_at"], "expires_at": data.get("expires_at"),
            "current_connections": await count_connections_for_link(uid),
            "vless_link": generate_vless_link(uid, remark=f"Luffy-{data['label']}"),
        })
    result.sort(key=lambda x: x["created_at"], reverse=True)
    return {"links": result}

@app.patch("/api/links/{uid}")
async def toggle_link(uid: str, request: Request, _=Depends(require_auth)):
    body = await request.json()
    async with LINKS_LOCK:
        if uid not in LINKS: raise HTTPException(status_code=404)
        if "active" in body: LINKS[uid]["active"] = bool(body["active"])
        if "reset_usage" in body and body["reset_usage"]:
            LINKS[uid]["used_bytes"] = 0
            LINKS[uid]["notified_quota"] = False
        if "label" in body: LINKS[uid]["label"] = str(body["label"])[:60]
        if "max_connections" in body: LINKS[uid]["max_connections"] = max(0, int(body["max_connections"] or 0))
        link_copy = dict(LINKS[uid])
    await db_save_link(uid, link_copy)
    return {"ok": True}

@app.delete("/api/links/{uid}")
async def delete_link(uid: str, _=Depends(require_auth)):
    async with LINKS_LOCK: LINKS.pop(uid, None)
    await close_connections_for_link(uid)
    await db_delete_link(uid)
    return {"ok": True}

@app.get("/api/addresses")
async def list_addresses(_=Depends(require_auth)):
    async with CUSTOM_ADDRESSES_LOCK: return {"addresses": list(CUSTOM_ADDRESSES)}

@app.post("/api/addresses")
async def add_address(request: Request, _=Depends(require_auth)):
    body = await request.json()
    address = (body.get("address") or "").strip()
    async with CUSTOM_ADDRESSES_LOCK:
        if address not in CUSTOM_ADDRESSES: CUSTOM_ADDRESSES.append(address)
    await db_save_address(address)
    return {"ok": True}

@app.delete("/api/addresses/{index}")
async def delete_address(index: int, _=Depends(require_auth)):
    async with CUSTOM_ADDRESSES_LOCK:
        if 0 <= index < len(CUSTOM_ADDRESSES): removed = CUSTOM_ADDRESSES.pop(index)
        else: raise HTTPException(status_code=404)
    await db_delete_address(removed)
    return {"ok": True}

@app.get("/sub/{uid}")
async def subscription_endpoint(uid: str):
    import base64
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None: raise HTTPException(status_code=404)
        link = dict(link)
    if not link["active"]: raise HTTPException(status_code=403)
    async with CUSTOM_ADDRESSES_LOCK: addresses = list(CUSTOM_ADDRESSES)
    
    used = link["used_bytes"]
    limit = link["limit_bytes"]
    usage_str = f"{used} / Unlimited" if limit == 0 else f"{used} / {limit}"
    status_node = generate_vless_link(uid, remark=f"📊 {usage_str}", address="0.0.0.0")
    links_out = [status_node, generate_vless_link(uid, remark=f"Luffy-{link['label']}-Server")]
    for addr in addresses:
        links_out.append(generate_vless_link(uid, remark=f"Luffy-{link['label']}-{addr}", address=addr))
        
    sub_content = "\n".join(links_out)
    encoded = base64.b64encode(sub_content.encode()).decode()
    return Response(content=encoded, headers={"Content-Type": "text/plain; charset=utf-8"})

def _public_page(title: str, body: str) -> str:
    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><title>{title}</title>
<style>body{{background:#0A0A0B;color:#F2F2F3;font-family:sans-serif;padding:50px;text-align:center;}}
.box{{background:#19191C;border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:30px;display:inline-block;max-width:400px;}}
textarea{{width:100%;background:#212124;color:#A0A0A8;border:1px solid rgba(255,255,255,0.08);padding:10px;resize:none;margin-top:15px;}}
button{{background:#6366F1;color:white;border:none;padding:10px 20px;border-radius:6px;cursor:pointer;margin-top:10px;width:100%;}}
</style></head><body><div class="box">{body}</div></body></html>"""

@app.post("/api/links/{uid}/share")
async def create_share_link(uid: str, _=Depends(require_auth)):
    token = secrets.token_urlsafe(24)
    SHARE_TOKENS[token] = {"uid": uid, "expires_at": time.time() + 86400, "used": False}
    return {"ok": True, "share_url": f"https://{get_domain()}/share/{token}"}

@app.get("/share/{token}", response_class=HTMLResponse)
async def view_share_link(token: str):
    info = SHARE_TOKENS.get(token)
    if info is None or info["expires_at"] < time.time() or info["used"]:
        return HTMLResponse(_public_page("Expired", "<h1>Link Expired or Already Viewed</h1>"))
    info["used"] = True
    uid = info["uid"]
    async with LINKS_LOCK:
        link = LINKS.get(uid)
    if not link: return HTMLResponse(_public_page("Error", "<h1>Config not found</h1>"))
    vless_link = generate_vless_link(uid, remark=f"Luffy-{link['label']}")
    body = f'<h1>{link["label"]}</h1><textarea rows="5" readonly>{vless_link}</textarea>'
    return HTMLResponse(_public_page("Share Link", body))

PANEL_HTML = """<!DOCTYPE html><html><head><title>Luffy Panel</title></head><body><h1>Luffy Panel</h1></body></html>"""

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request): return HTMLResponse(content=PANEL_HTML)

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request): return HTMLResponse(content=PANEL_HTML)

@app.websocket("/ws/{uid}")
async def websocket_endpoint(websocket: WebSocket, uid: str):
    async with LINKS_LOCK:
        link = LINKS.get(uid)
    if not link or not link["active"]:
        await websocket.close(code=1008)
        return
    await websocket.accept()
    client_ip = get_client_ip(websocket)
    cid = f"{uid}-{secrets.token_hex(4)}"
    async with connections_lock:
        connections[cid] = {"uuid": uid, "ip": client_ip}
    try:
        while True:
            data = await websocket.receive_bytes()
            async with LINKS_LOCK:
                if uid in LINKS: LINKS[uid]["used_bytes"] += len(data)
            stats["total_bytes"] += len(data)
            await websocket.send_bytes(data)
    except WebSocketDisconnect: pass
    finally:
        async with connections_lock: connections.pop(cid, None)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=CONFIG["port"])
