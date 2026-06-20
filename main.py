import asyncio
import json
import os
import hashlib
import secrets
import time
import re
import base64
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

# ── در-حافظه ────────────────────────────────────────────────────────────────
connections: dict = {}
connections_lock = asyncio.Lock()
connection_sockets: dict = {}
link_ip_map: dict = defaultdict(set)
SHARE_TOKENS: dict = {}
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

async def count_connections_for_link(uid: str) -> int:
    async with connections_lock:
        return sum(1 for info in connections.values() if info.get("uuid") == uid)

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
    return RedirectResponse(url="/login")

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
        "links_count": len(LINKS),
        "domain": get_domain(),
        "cpu_percent": psutil.cpu_percent(interval=0.1),
        "memory_percent": psutil.virtual_memory().percent,
    }

@app.post("/api/links")
async def create_link(request: Request, _=Depends(require_auth)):
    body = await request.json()
    label = (body.get("label") or "New Link").strip()[:60]
    if not re.match(r'^[a-zA-Z0-9\-_. ]+$', label):
        raise HTTPException(status_code=400, detail="Invalid character in label")
    async with LINKS_LOCK:
        if label in LINKS:
            raise HTTPException(status_code=400, detail="An inbound with this name already exists")
    limit_value = float(body.get("limit_value") or 0)
    limit_unit = body.get("limit_unit") or "GB"
    limit_bytes = 0 if limit_value <= 0 else parse_size_to_bytes(limit_value, limit_unit)
    max_conn = int(body.get("max_connections") or 0)
    days_valid = body.get("days_valid")
    expires_at = None
    if days_valid:
        expires_at = (datetime.now(timezone.utc) + timedelta(days=int(days_valid))).isoformat()
    uid = label
    async with LINKS_LOCK:
        LINKS[uid] = {
            "label": label, "limit_bytes": limit_bytes, "used_bytes": 0,
            "max_connections": max_conn, "created_at": datetime.now(timezone.utc).isoformat(),
            "active": True, "expires_at": expires_at
        }
    await db_save_link(uid, LINKS[uid])
    return {"ok": True}

@app.get("/api/links")
async def list_links(_=Depends(require_auth)):
    result = []
    async with LINKS_LOCK:
        items = list(LINKS.items())
    for uid, data in items:
        result.append({
            "uuid": uid, "label": data["label"], "limit_bytes": data["limit_bytes"],
            "used_bytes": data["used_bytes"], "max_connections": data.get("max_connections", 0),
            "active": data["active"], "created_at": data["created_at"], "expires_at": data.get("expires_at"),
            "current_connections": await count_connections_for_link(uid),
            "vless_link": generate_vless_link(uid, remark=f"Luffy-{data['label']}"),
        })
    return {"links": result}

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
    addr = str(body.get("address") or "").strip()
    if addr:
        async with CUSTOM_ADDRESSES_LOCK:
            if addr not in CUSTOM_ADDRESSES:
                CUSTOM_ADDRESSES.append(addr)
        await db_save_address(addr)
    return {"ok": True}

@app.delete("/api/addresses/{addr}")
async def delete_address(addr: str, _=Depends(require_auth)):
    async with CUSTOM_ADDRESSES_LOCK:
        if addr in CUSTOM_ADDRESSES:
            CUSTOM_ADDRESSES.remove(addr)
    await db_delete_address(addr)
    return {"ok": True}

@app.get("/sub/{uid}")
async def subscription_endpoint(uid: str):
    async with LINKS_LOCK:
        link = LINKS.get(uid)
    if not link or not link["active"]:
        raise HTTPException(status_code=404, detail="Link not found or inactive")
    vless = generate_vless_link(uid, remark=f"Luffy-{link['label']}")
    encoded = base64.b64encode(vless.encode()).decode()
    return Response(content=encoded, headers={"Content-Type": "text/plain"})

def _public_page(title: str, body: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>{title}</title>
<link href="https://fonts.googleapis.com/css2?family=Sora:wght@600;700;800&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
*:not(b){{margin:0;padding:0;box-sizing:border-box}}
body{{background:#0A0A0B;color:#F2F2F3;font-family:'Inter',sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;}}
.box{{background:#19191C;border:1px solid rgba(255,255,255,.08);border-radius:18px;padding:30px;width:100%;max-width:420px;text-align:center;}}
.mono{{width:36px;height:36px;border-radius:11px;background:linear-gradient(155deg,#6366F1,#4338CA);display:flex;align-items:center;justify-content:center;margin:0 auto 14px;font-weight:800;}}
h1{{font-family:'Sora',sans-serif;font-size:18px;margin-bottom:8px;}}
p{{color:#A0A0A8;font-size:13px;line-height:1.6;margin-bottom:20px;}}
textarea{{width:100%;background:#212124;border:1px solid rgba(255,255,255,.08);color:#A0A0A8;padding:10px;border-radius:10px;resize:none;font-family:monospace;font-size:11px;}}
.copybtn{{width:100%;margin-top:10px;background:#6366F1;color:#fff;border:none;padding:12px;border-radius:9px;font-weight:600;cursor:pointer;}}
</style></head><body><div class="box">{body}</div>
<script>
function cp(){{const t=document.getElementById('cfgtxt');t.select();document.execCommand('copy');alert('Copied!');}}
</script></body></html>"""

@app.get("/share/{token}", response_class=HTMLResponse)
async def view_share_link(token: str):
    info = SHARE_TOKENS.get(token)
    if info is None or info["expires_at"] < time.time() or info["used"]:
        return HTMLResponse(_public_page("Expired", "<h1>Link Expired or Already Opened</h1>"))
    info["used"] = True
    uid = info["uid"]
    async with LINKS_LOCK:
        link = LINKS.get(uid)
    if not link:
        return HTMLResponse(_public_page("Not Found", "<h1>Config not found</h1>"))
    vless_link = generate_vless_link(uid, remark=f"Luffy-{link['label']}")
    body = f"""<div class="mono">L</div><h1>{link['label']}</h1><p>Save your configuration link now.</p>
    <textarea id="cfgtxt" rows="4" readonly>{vless_link}</textarea>
    <button class="copybtn" onclick="cp()">Copy Config</button>"""
    return HTMLResponse(_public_page("Config", body))

# ── PANEL_HTML (المان‌ها و رابط کاربری گرافیکی) ──────────────────────────────
PANEL_HTML = """<!DOCTYPE html>
<html lang="en" data-theme="dark" data-lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Luffy Server Gateway Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Sora:wght@600;700;800&family=Inter:wght@400;500;600;700&family=Vazirmatn:wght@400;700&display=swap" rel="stylesheet">
<style>
:root[data-theme="dark"] {
  --bg-main: #0A0A0B; --bg-card: #131316; --bg-input: #1C1C21;
  --text-main: #F2F2F3; --text-muted: #8F8F94; --border: rgba(255,255,255,0.06);
  --accent: #6366F1; --accent-hover: #4F46E5; --danger: #EF4444;
}
:root[data-theme="light"] {
  --bg-main: #FAFAFA; --bg-card: #FFFFFF; --bg-input: #F3F4F6;
  --text-main: #111827; --text-muted: #6B7280; --border: rgba(0,0,0,0.08);
  --accent: #4F46E5; --accent-hover: #4338CA; --danger: #DC2626;
}
* { margin:0; padding:0; box-sizing:border-box; font-family:'Inter','Vazirmatn',sans-serif; transition: background 0.2s, border 0.2s; }
body { background: var(--bg-main); color: var(--text-main); min-height: 100vh; padding: 20px; }
.container { max-width: 1100px; margin: 0 auto; }
header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 30px; }
.logo-area { display: flex; align-items: center; gap: 10px; }
.logo-icon { background: linear-gradient(135deg, #6366F1, #4338CA); width: 40px; height: 40px; border-radius: 12px; display: flex; align-items: center; justify-content: center; color: white; font-weight: 800; font-family: 'Sora'; }
.logo-title { font-family: 'Sora'; font-size: 20px; }
.ctrls { display: flex; gap: 10px; }
button.btn-sec { background: var(--bg-card); border: 1px solid var(--border); color: var(--text-main); padding: 8px 14px; border-radius: 8px; cursor: pointer; font-size: 13px; font-weight: 500; }
button.btn-pri { background: var(--accent); border: none; color: white; padding: 10px 18px; border-radius: 8px; cursor: pointer; font-size: 13px; font-weight: 600; }
button.btn-pri:hover { background: var(--accent-hover); }
.grid-stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin-bottom: 30px; }
.card-stat { background: var(--bg-card); border: 1px solid var(--border); padding: 20px; border-radius: 14px; }
.card-stat .lbl { font-size: 12px; color: var(--text-muted); margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.5px; }
.card-stat .val { font-size: 22px; font-weight: 700; font-family: 'Sora', sans-serif; }
.main-section { background: var(--bg-card); border: 1px solid var(--border); border-radius: 16px; padding: 24px; margin-bottom: 24px; }
.sec-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
.sec-title { font-family: 'Sora'; font-size: 16px; }
.link-row { display: flex; justify-content: space-between; align-items: center; padding: 14px 0; border-bottom: 1px solid var(--border); }
.link-row:last-child { border-bottom: none; }
.link-meta h4 { font-size: 14px; margin-bottom: 4px; }
.link-meta p { font-size: 12px; color: var(--text-muted); }
.link-actions { display: flex; gap: 8px; }
.modal { position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.6); display:none; align-items:center; justify-content:center; padding:16px; z-index:100; }
.modal.show { display: flex; }
.modal-content { background: var(--bg-card); border: 1px solid var(--border); padding: 24px; border-radius: 16px; width: 100%; max-width: 440px; }
.form-group { margin-bottom: 16px; }
.form-group label { display:block; font-size:12px; color:var(--text-muted); margin-bottom:6px; }
.form-group input, .form-group select { width:100%; background:var(--bg-input); border:1px solid var(--border); color:var(--text-main); padding:10px; border-radius:8px; font-size:14px; }
#login-screen { position:fixed; top:0; left:0; width:100%; height:100%; background:var(--bg-main); z-index:999; display:flex; align-items:center; justify-content:center; padding:20px; }
.login-box { background:var(--bg-card); border:1px solid var(--border); padding:32px; border-radius:20px; width:100%; max-width:380px; text-align:center; }
.toast { position:fixed; bottom:20px; right:20px; background:#10B981; color:white; padding:12px 20px; border-radius:8px; font-size:13px; font-weight:600; display:none; z-index:1000; box-shadow:0 10px 15px -3px rgba(0,0,0,0.3); }
.toast.err { background:#EF4444; }
[data-lang="fa"] { direction: rtl; text-align: right; }
[data-lang="fa"] .ctrls, [data-lang="fa"] .link-actions { gap: 8px; }
</style>
</head>
<body>

<div id="login-screen">
  <div class="login-box">
    <div class="logo-icon" style="margin:0 auto 16px;">L</div>
    <h3 style="font-family:'Sora'; margin-bottom:6px;">Luffy Gateway</h3>
    <p style="color:var(--text-muted); font-size:13px; margin-bottom:24px;">Enter administrative credentials</p>
    <div class="form-group" style="text-align:left;">
      <input type="password" id="login-password" placeholder="Password" style="text-align:center;">
    </div>
    <button class="btn-pri" style="width:100%; padding:12px;" onclick="doLogin()">Authenticate</button>
  </div>
</div>

<div class="container">
  <header>
    <div class="logo-area">
      <div class="logo-icon">L</div>
      <div class="logo-title">Luffy Panel</div>
    </div>
    <div class="ctrls">
      <button class="btn-sec" onclick="toggleLang()">EN / FA</button>
      <button class="btn-sec" onclick="toggleTheme()">☀️/🌙</button>
      <button class="btn-sec" onclick="doLogout()" data-i18n="logout">Logout</button>
    </div>
  </header>

  <div class="grid-stats">
    <div class="card-stat"><div class="lbl" data-i18n="st-conn">Active Connections</div><div class="val" id="st-conn-v">-</div></div>
    <div class="card-stat"><div class="lbl" data-i18n="st-traffic">Total Traffic</div><div class="val" id="st-traffic-v">-</div></div>
    <div class="card-stat"><div class="lbl" data-i18n="st-cpu">CPU Load</div><div class="val" id="st-cpu-v">-</div></div>
    <div class="card-stat"><div class="lbl" data-i18n="st-mem">Memory Usage</div><div class="val" id="st-mem-v">-</div></div>
  </div>

  <div class="main-section">
    <div class="sec-head">
      <div class="sec-title" data-i18n="links-title">Client Inbounds & Metrics</div>
      <button class="btn-pri" onclick="$m('mo-add').classList.add('show')" data-i18n="add-btn">+ Create Link</button>
    </div>
    <div id="links-container"></div>
  </div>

  <div class="main-section">
    <div class="sec-head">
      <div class="sec-title" data-i18n="addr-title">Custom Routing / SNI Addresses</div>
      <button class="btn-pri" onclick="$m('mo-addr').classList.add('show')" data-i18n="addr-add">+ Add Address</button>
    </div>
    <div id="addr-container" style="display:flex; flex-wrap:wrap; gap:8px;"></div>
  </div>
</div>

<div class="modal" id="mo-add">
  <div class="modal-content">
    <h3 style="font-family:'Sora'; margin-bottom:16px;" data-i18n="mo-add-t">Create New Access Inbound</h3>
    <div class="form-group">
      <label data-i18n="mo-lbl-name">Inbound Name / Remark</label>
      <input type="text" id="mo-in-label" placeholder="e.g. John-Remote">
    </div>
    <div class="form-group" style="display:grid; grid-template-columns: 2fr 1fr; gap:8px;">
      <div>
        <label data-i18n="mo-lbl-quota">Data Quota</label>
        <input type="number" id="mo-in-quota" value="0">
      </div>
      <div>
        <label>&nbsp;</label>
        <select id="mo-in-unit"><option>GB</option><option>MB</option></select>
      </div>
    </div>
    <div class="form-group">
      <label data-i18n="mo-lbl-conn">Max Concurrent Connections (0 = Unlim)</label>
      <input type="number" id="mo-in-conn" value="0">
    </div>
    <div class="form-group">
      <label data-i18n="mo-lbl-days">Validity Period (Days, optional)</label>
      <input type="number" id="mo-in-days" placeholder="Unlimited">
    </div>
    <div class="link-actions" style="justify-content:flex-end; margin-top:20px;">
      <button class="btn-sec" onclick="$m('mo-add').classList.remove('show')" data-i18n="cancel">Cancel</button>
      <button class="btn-pri" onclick="saveLink()" data-i18n="save">Save</button>
    </div>
  </div>
</div>

<div class="modal" id="mo-addr">
  <div class="modal-content">
    <h3 style="font-family:'Sora'; margin-bottom:16px;" data-i18n="mo-addr-t">Add Custom Routing Address</h3>
    <div class="form-group">
      <label data-i18n="mo-addr-lbl">Domain / Host Address</label>
      <input type="text" id="mo-in-addr" placeholder="e.g. speedtest.net">
    </div>
    <div class="link-actions" style="justify-content:flex-end; margin-top:20px;">
      <button class="btn-sec" onclick="$m('mo-addr').classList.remove('show')" data-i18n="cancel">Cancel</button>
      <button class="btn-pri" onclick="saveAddr()" data-i18n="add-btn">Add</button>
    </div>
  </div>
</div>

<div class="toast" id="ts">Action Successful</div>

<script>
const I18N = {
  en: {
    logout: "Logout", "st-conn": "Active Connections", "st-traffic": "Total Traffic",
    "st-cpu": "CPU Load", "st-mem": "Memory Usage", "links-title": "Client Inbounds & Metrics",
    "add-btn": "Create Link", "addr-title": "Custom Routing / SNI Addresses", "addr-add": "Add Address",
    "mo-add-t": "Create New Access Inbound", "mo-lbl-name": "Inbound Name / Remark", "mo-lbl-quota": "Data Quota",
    "mo-lbl-conn": "Max Concurrent Connections", "mo-lbl-days": "Validity Period (Days)", "cancel": "Cancel", "save": "Save",
    "mo-addr-t": "Add Custom Routing Address", "mo-addr-lbl": "Domain / Host Address"
  },
  fa: {
    logout: "خروج", "st-conn": "اتصالات فعال", "st-traffic": "ترافیک کل",
    "st-cpu": "بار پردازنده", "st-mem": "مصرف حافظه", "links-title": "تنظیمات کلاینت و ترافیک",
    "add-btn": "ایجاد لینک", "addr-title": "آدرس‌های مسیریابی اختصاصی (SNI)", "addr-add": "افزودن آدرس",
    "mo-add-t": "ایجاد اتصال ورودی جدید", "mo-lbl-name": "نام / عنوان لینک", "mo-lbl-quota": "حجم مجاز ترافیک",
    "mo-lbl-conn": "حداکثر اتصالات همزمان", "mo-lbl-days": "مدت اعتبار (روز)", "cancel": "لغو", "save": "ذخیره",
    "mo-addr-t": "افزودن آدرس مسیریابی سفارشی", "mo-addr-lbl": "دامنه / آدرس هاست"
  }
};

let theme = localStorage.getItem('theme') || 'dark';
let lang = localStorage.getItem('lang') || 'en';
let isAuthenticated = false;

const $m = id => document.getElementById(id);

function setTheme(t) {
  theme = t; document.documentElement.setAttribute('data-theme', t); localStorage.setItem('theme', t);
}
function toggleTheme() { setTheme(theme === 'dark' ? 'light' : 'dark'); }

function setLang(l) {
  lang = l; document.documentElement.setAttribute('data-lang', l); localStorage.setItem('lang', l);
  document.querySelectorAll('[data-i18n]').forEach(el => {
    const k = el.getAttribute('data-i18n'); if(I18N[l][k]) el.textContent = I18N[l][k];
  });
}
function toggleLang() { setLang(lang === 'en' ? 'fa' : 'en'); }

function toast(msg, isErr=false) {
  const t = $m('ts'); t.textContent = msg; t.className = 'toast' + (isErr?' err':'');
  t.style.display='block'; setTimeout(()=>t.style.display='none',3000);
}

async function checkAuth() {
  try {
    const r = await fetch('/api/me'); const d = await r.json();
    if(d.authenticated) {
      isAuthenticated = true; $m('login-screen').style.display='none'; loadStats(); loadLinks(); loadAddrs();
    }
  } catch(e){}
}

async function doLogin() {
  const pw = $m('login-password').value;
  try {
    const r = await fetch('/api/login', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({password:pw})});
    if(r.ok) {
      isAuthenticated = true; $m('login-screen').style.display='none'; toast('Authenticated'); loadStats(); loadLinks(); loadAddrs();
    } else { toast('Invalid Password', true); }
  } catch(e){ toast('Connection error', true); }
}

async function doLogout() {
  await fetch('/api/logout', {method:'POST'}); location.reload();
}

async function loadStats() {
  if(!isAuthenticated) return;
  try {
    const r = await fetch('/stats'); const d = await r.json();
    $m('st-conn-v').textContent = d.active_connections;
    $m('st-traffic-v').textContent = d.total_traffic_mb + ' MB';
    $m('st-cpu-v').textContent = d.cpu_percent + '%';
    $m('st-mem-v').textContent = d.memory_percent + '%';
  } catch(e){}
}

async function loadLinks() {
  if(!isAuthenticated) return;
  try {
    const r = await fetch('/api/links'); const d = await r.json();
    const container = $m('links-container'); container.innerHTML = '';
    d.links.forEach(l => {
      const row = document.createElement('div'); row.className = 'link-row';
      const useMb = (l.used_bytes / (1024*1024)).toFixed(1);
      const limMb = l.limit_bytes ? (l.limit_bytes / (1024*1024)).toFixed(0) + ' MB' : '∞';
      row.innerHTML = `<div class="link-meta">
        <h4>${l.label}</h4>
        <p>Traffic: ${useMb} / ${limMb} | Conns: ${l.current_connections}/${l.max_connections||'∞'}</p>
      </div>
      <div class="link-actions">
        <button class="btn-sec" onclick="navigator.clipboard.writeText('${l.vless_link}'); toast('Config Copied');">Copy</button>
        <button class="btn-sec" style="color:var(--danger);" onclick="delLink('${l.uuid}')">Delete</button>
      </div>`;
      container.appendChild(row);
    });
  } catch(e){}
}

async function loadAddrs() {
  if(!isAuthenticated) return;
  try {
    const r = await fetch('/api/addresses'); const d = await r.json();
    const container = $m('addr-container'); container.innerHTML = '';
    d.addresses.forEach(a => {
      const badge = document.createElement('span');
      badge.style = "background:var(--bg-input); padding:6px 12px; border-radius:20px; font-size:12px; display:inline-flex; align-items:center; gap:8px; border:1px solid var(--border);";
      badge.innerHTML = `${a} <b style="cursor:pointer; color:var(--danger);" onclick="delAddr('${a}')">×</b>`;
      container.appendChild(badge);
    });
  } catch(e){}
}

async function saveLink() {
  const lbl = $m('mo-in-label').value;
  const qVal = $m('mo-in-quota').value;
  const qUnit = $m('mo-in-unit').value;
  const conn = $m('mo-in-conn').value;
  const days = $m('mo-in-days').value;
  try {
    const r = await fetch('/api/links', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({label:lbl, limit_value:qVal, limit_unit:qUnit, max_connections:conn, days_valid:days})
    });
    if(!r.ok) { const err = await r.json(); throw new Error(err.detail || 'Failed'); }
    toast('Inbound Created'); $m('mo-add').classList.remove('show'); loadLinks();
  } catch(e){ toast(e.message, true); }
}

async function delLink(uid) {
  if(!confirm('Delete Inbound?')) return;
  await fetch('/api/links/'+uid, {method:'DELETE'}); toast('Inbound Deleted'); loadLinks();
}

async function saveAddr() {
  const addr = $m('mo-in-addr').value;
  await fetch('/api/addresses', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({address:addr})});
  toast('Address Added'); $m('mo-addr').classList.remove('show'); loadAddrs();
}

async function delAddr(addr) {
  await fetch('/api/addresses/'+addr, {method:'DELETE'}); toast('Address Removed'); loadAddrs();
}

setTheme(theme); setLang(lang); checkAuth();
setInterval(()=>{ if(isAuthenticated){ loadStats(); loadLinks(); } }, 10000);
</script>
</body>
</html>"""

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return HTMLResponse(content=PANEL_HTML)

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    return HTMLResponse(content=PANEL_HTML)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=CONFIG["port"], reload=False)
