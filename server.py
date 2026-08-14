#!/usr/bin/env python3
"""Termux Remote Controller - Server v6
Clean rewrite. Tested before deploy."""
import os, json, time, asyncio, traceback as tb
from aiohttp import web, WSMsgType

PORT = int(os.environ.get("PORT", 2010))
TOKEN = os.environ.get("AUTH_TOKEN", "termux-remote-2024")

# Storage
brokers = {}   # cid -> ws  (browser clients)
devices = {}   # cid -> ws  (termux clients)
dev_info = {}  # cid -> device name string

log = lambda *a: print(f"[{time.strftime('%H:%M:%S')}]", *a, flush=True)

# ---------- HTTP ----------

async def serve_index(req):
    fp = os.path.join(os.path.dirname(os.path.abspath(__file__)), "public", "index.html")
    return web.FileResponse(fp)

async def serve_health(req):
    nd = sum(1 for w in devices.values() if not w.closed)
    return web.json_response({"ok": True, "devices": nd})

# ---------- Helpers ----------

async def safe_send(ws, obj):
    """Send JSON to a websocket, catch errors."""
    try:
        await ws.send_str(json.dumps(obj))
        return True
    except:
        return False

async def broadcast_browsers(obj):
    """Send JSON to all connected browsers."""
    dead = []
    for cid, ws in brokers.items():
        if ws.closed:
            dead.append(cid)
            continue
        if not await safe_send(ws, obj):
            dead.append(cid)
    for cid in dead:
        brokers.pop(cid, None)

def get_device_list():
    return [{"id": c, "name": dev_info.get(c, "Termux")}
           for c, ws in devices.items() if not ws.closed]

# ---------- Browser WebSocket ----------

async def handle_browser(req):
    ws = web.WebSocketResponse()
    try:
        await ws.prepare(req)
    except Exception as e:
        log("browser prepare error:", e)
        return ws

    cid = str(int(time.time() * 1000) % 1000000)
    brokers[cid] = ws
    log(f"browser-{cid} connected")

    try:
        # 1) Send client ID
        await safe_send(ws, {"t": "hi", "id": cid})
        # 2) Send current device list
        await safe_send(ws, {"t": "devs", "list": get_device_list()})

        # 3) Message loop
        while not ws.closed:
            msg = await ws.receive()

            if msg.type == WSMsgType.TEXT:
                try:
                    d = json.loads(msg.data)
                except:
                    continue

                kind = d.get("a")  # action

                if kind == "ls":
                    await safe_send(ws, {"t": "devs", "list": get_device_list()})

                elif kind == "i":  # input
                    target = d.get("to")
                    if target in devices and not devices[target].closed:
                        await safe_send(devices[target], {"t": "i", "d": d.get("d", "")})

                elif kind == "r":  # resize
                    target = d.get("to")
                    if target in devices and not devices[target].closed:
                        await safe_send(devices[target], {"t": "r", "c": d.get("c", 80), "rows": d.get("rows", 24)})

                elif kind == "fr":  # file_req
                    target = d.get("to")
                    if target in devices and not devices[target].closed:
                        await safe_send(devices[target], {"t": "fr", "p": d.get("p", "."), "m": d.get("m", "list"), "q": d.get("q", "*"), "rid": d.get("rid")})

                elif kind == "fu":  # file_upload
                    target = d.get("to")
                    if target in devices and not devices[target].closed:
                        await safe_send(devices[target], {"t": "fu", "p": d.get("p", ""), "c": d.get("c", ""), "rid": d.get("rid")})

                elif kind == "fd":  # file_download
                    target = d.get("to")
                    if target in devices and not devices[target].closed:
                        await safe_send(devices[target], {"t": "fd", "p": d.get("p", ""), "rid": d.get("rid")})

            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED, WSMsgType.ERROR):
                break

    except asyncio.CancelledError:
        pass
    except Exception as e:
        log(f"browser-{cid} error: {e}")
        log(tb.format_exc())
    finally:
        brokers.pop(cid, None)
        log(f"browser-{cid} disconnected")
    return ws

# ---------- Termux WebSocket ----------

async def handle_termux(req):
    ws = web.WebSocketResponse()
    try:
        await ws.prepare(req)
    except Exception as e:
        log("termux prepare error:", e)
        return ws

    # Auth
    if req.query.get("token") != TOKEN:
        log("termux auth rejected")
        await safe_send(ws, {"t": "err", "msg": "bad token"})
        await ws.close()
        return ws

    cid = str(int(time.time() * 1000) % 1000000)
    name = req.query.get("device", "Termux")
    devices[cid] = ws
    dev_info[cid] = name
    log(f"termux-{cid} connected ({name})")

    try:
        await safe_send(ws, {"t": "hi", "id": cid})
        await broadcast_browsers({"t": "dev_up", "id": cid, "name": name})

        while not ws.closed:
            msg = await ws.receive()

            if msg.type == WSMsgType.TEXT:
                try:
                    d = json.loads(msg.data)
                except:
                    continue

                kind = d.get("t")

                if kind in ("o", "res", "fd"):  # output, result, file_data
                    await broadcast_browsers({"t": kind, "d": d.get("d", {}), "src": cid, "rid": d.get("rid")})

                elif kind == "ping":
                    await safe_send(ws, {"t": "pong"})

            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED, WSMsgType.ERROR):
                break

    except asyncio.CancelledError:
        pass
    except Exception as e:
        log(f"termux-{cid} error: {e}")
    finally:
        devices.pop(cid, None)
        dev_info.pop(cid, None)
        log(f"termux-{cid} disconnected")
        await broadcast_browsers({"t": "dev_down", "id": cid})
    return ws

# ---------- App ----------

app = web.Application()
app.router.add_get("/", serve_index)
app.router.add_get("/api/health", serve_health)
app.router.add_get("/ws/b", handle_browser)      # browser ws
app.router.add_get("/ws/t", handle_termux)       # termux ws

if __name__ == "__main__":
    log(f"START port={PORT} token={TOKEN}")
    web.run_app(app, host="0.0.0.0", port=PORT, print=None, access_log=None)
