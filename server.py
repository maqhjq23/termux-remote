#!/usr/bin/env python3
"""Termux Remote - Railway Server (Ultra-stable)"""
import os, sys, json, time, asyncio, uuid, traceback
from aiohttp import web

# Railway sets PORT automatically. Don't hardcode.
PORT = int(os.environ.get("PORT", 2010))
AUTH_TOKEN = os.environ.get("AUTH_TOKEN", "termux-remote-2024")

# In-memory stores
_browsers = {}  # id -> ws
_termux = {}    # id -> ws
_meta = {}      # id -> info dict

log = lambda m: print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

# ---------- Routes ----------
async def index(req):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "public", "index.html")
    return web.FileResponse(path)

async def health(req):
    n = len([w for w in _termux.values() if not w.closed])
    return web.json_response({"status": "ok", "port": PORT, "devices": n, "auth_set": bool(AUTH_TOKEN)})

async def ws_browser(req):
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    cid = str(uuid.uuid4())[:8]
    _browsers[cid] = ws
    log(f"Browser {cid} open")

    # send id
    await ws.send(json.dumps({"type": "init", "client_id": cid}))
    # send existing devices
    devs = [{"id": t, "info": _meta.get(t, {}).get("device_info", "?")} for t, w in list(_termux.items()) if not w.closed]
    await ws.send(json.dumps({"type": "devices", "devices": devs}))

    try:
        async for raw in ws:
            if raw.type == 1:  # TEXT
                try:
                    data = json.loads(raw.data)
                except:
                    continue
                act = data.get("action")
                tgt = data.get("target")

                if act == "list_devices":
                    devs = [{"id": t, "info": _meta.get(t, {}).get("device_info", "?")} for t, w in list(_termux.items()) if not w.closed]
                    await ws.send(json.dumps({"type": "devices", "devices": devs}))

                elif act == "ping":
                    await ws.send(json.dumps({"type": "pong"}))

                elif tgt in _termux and not _termux[tgt].closed:
                    tw = _termux[tgt]
                    if act == "input":
                        await tw.send(json.dumps({"type": "input", "data": data.get("data", "")}))
                    elif act == "resize":
                        await tw.send(json.dumps({"type": "resize", "cols": data.get("cols", 80), "rows": data.get("rows", 24)}))
                    elif act == "file_req":
                        await tw.send(json.dumps({"type": "file_req", "path": data.get("path", "."), "mode": data.get("mode", "list"), "query": data.get("query", "*"), "req_id": data.get("req_id", "")}))
                    elif act == "file_upload":
                        await tw.send(json.dumps({"type": "file_upload", "path": data.get("path", ""), "content": data.get("content", ""), "req_id": data.get("req_id", "")}))
                    elif act == "file_download":
                        await tw.send(json.dumps({"type": "file_download", "path": data.get("path", ""), "req_id": data.get("req_id", "")}))

            elif raw.type in (8, 256):  # ERROR, CLOSE
                break
    except Exception as e:
        log(f"Browser {cid} err: {e}")
    finally:
        _browsers.pop(cid, None)
        log(f"Browser {cid} closed")
    return ws

async def ws_termux(req):
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    token = req.query.get("token", "")
    if token != AUTH_TOKEN:
        log("Termux auth FAILED")
        await ws.send(json.dumps({"type": "error", "msg": "bad token"}))
        await ws.close()
        return ws
    cid = str(uuid.uuid4())[:8]
    _termux[cid] = ws
    info = req.query.get("device", "Termux")
    _meta[cid] = {"type": "termux", "device_info": info, "ts": time.time()}
    log(f"Termux {cid} connected ({info})")
    await ws.send(json.dumps({"type": "init", "client_id": cid}))
    # notify browsers
    payload = json.dumps({"type": "device_connected", "device": {"id": cid, "info": info}})
    for bw in list(_browsers.values()):
        if not bw.closed:
            try: await bw.send(payload)
            except: pass
    try:
        async for raw in ws:
            if raw.type == 1:
                try:
                    data = json.loads(raw.data)
                except:
                    continue
                mt = data.get("type")
                if mt in ("output", "result", "file_data"):
                    out = json.dumps({"type": mt, "data": data.get("data", {}), "source": cid, "req_id": data.get("req_id", "")})
                    for bw in list(_browsers.values()):
                        if not bw.closed:
                            try: await bw.send(out)
                            except: pass
                elif mt == "ping":
                    await ws.send(json.dumps({"type": "pong"}))
            elif raw.type in (8, 256):
                break
    except Exception as e:
        log(f"Termux {cid} err: {e}")
    finally:
        _termux.pop(cid, None)
        _meta.pop(cid, None)
        log(f"Termux {cid} disconnected")
        disc = json.dumps({"type": "device_disconnected", "device_id": cid})
        for bw in list(_browsers.values()):
            if not bw.closed:
                try: await bw.send(disc)
                except: pass
    return ws

# ---------- App ----------
app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/api/health", health)
app.router.add_get("/ws/browser", ws_browser)
app.router.add_get("/ws/termux", ws_termux)

if __name__ == "__main__":
    log(f"=== START port={PORT} token={AUTH_TOKEN} ===")
    web.run_app(app, host="0.0.0.0", port=PORT, print=None)
    log("Server stopped")
