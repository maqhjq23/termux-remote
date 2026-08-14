#!/usr/bin/env python3
"""Termux Remote Controller - Server v7
Railway Hikari WebSocket fix.
- heartbeat to keep WS alive through proxy
- explicit error logging on startup
- graceful shutdown handling
"""
import os, sys, json, time, asyncio, traceback as tb
from aiohttp import web, WSMsgType

PORT = int(os.environ.get("PORT", 2010))
TOKEN = os.environ.get("AUTH_TOKEN", "termux-remote-2024")

# Storage
brokers = {}   # cid -> ws  (browser clients)
devices = {}   # cid -> ws  (termux clients)
dev_info = {}  # cid -> device name string

def log(*a):
    print(f"[{time.strftime('%H:%M:%S')}]", *a, flush=True)

# ---------- HTTP ----------

async def serve_index(req):
    fp = os.path.join(os.path.dirname(os.path.abspath(__file__)), "public", "index.html")
    return web.FileResponse(fp)

async def serve_health(req):
    nd = sum(1 for w in devices.values() if not w.closed)
    return web.json_response({"ok": True, "devices": nd})

async def serve_debug(req):
    """Debug endpoint to verify app is running and show routes."""
    routes = [{'path': r.path, 'method': r.method} for r in app.router.routes()]
    return web.json_response({
        "ok": True,
        "port": PORT,
        "python": sys.version,
        "routes": routes,
        "devices": len(devices),
        "brokers": len(brokers),
    })

# ---------- Helpers ----------

async def safe_send(ws, obj):
    try:
        if ws.closed:
            return False
        await ws.send_str(json.dumps(obj))
        return True
    except Exception as e:
        return False

async def broadcast_browsers(obj):
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
    log(f"browser WS upgrade from {req.remote}")
    ws = web.WebSocketResponse(
        heartbeat=25,
        autoping=True,
        timeout=30,
        max_msg_size=4*1024*1024,
    )
    try:
        await ws.prepare(req)
        log(f"browser WS upgrade success")
    except Exception as e:
        log(f"browser prepare error: {e}")
        log(tb.format_exc())
        return ws

    cid = str(int(time.time() * 1000) % 1000000)
    brokers[cid] = ws
    log(f"browser-{cid} connected")

    try:
        await safe_send(ws, {"t": "hi", "id": cid})
        await safe_send(ws, {"t": "devs", "list": get_device_list()})

        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    d = json.loads(msg.data)
                except:
                    continue

                kind = d.get("a")

                if kind == "ls":
                    await safe_send(ws, {"t": "devs", "list": get_device_list()})

                elif kind == "i":
                    target = d.get("to")
                    if target in devices and not devices[target].closed:
                        await safe_send(devices[target], {"t": "i", "d": d.get("d", "")})

                elif kind == "r":
                    target = d.get("to")
                    if target in devices and not devices[target].closed:
                        await safe_send(devices[target], {"t": "r", "c": d.get("c", 80), "rows": d.get("rows", 24)})

                elif kind == "fr":
                    target = d.get("to")
                    if target in devices and not devices[target].closed:
                        await safe_send(devices[target], {"t": "fr", "p": d.get("p", "."), "m": d.get("m", "list"), "q": d.get("q", "*"), "rid": d.get("rid")})

                elif kind == "fu":
                    target = d.get("to")
                    if target in devices and not devices[target].closed:
                        await safe_send(devices[target], {"t": "fu", "p": d.get("p", ""), "c": d.get("c", ""), "rid": d.get("rid")})

                elif kind == "fd":
                    target = d.get("to")
                    if target in devices and not devices[target].closed:
                        await safe_send(devices[target], {"t": "fd", "p": d.get("p", ""), "rid": d.get("rid")})

            elif msg.type == WSMsgType.ERROR:
                log(f"browser-{cid} ws error: {ws.exception()}")
                break
            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED):
                break
            elif msg.type == WSMsgType.PING:
                log(f"browser-{cid} ping received")

    except asyncio.CancelledError:
        pass
    except Exception as e:
        log(f"browser-{cid} loop error: {e}")
        log(tb.format_exc())
    finally:
        brokers.pop(cid, None)
        try:
            await ws.close()
        except:
            pass
        log(f"browser-{cid} disconnected")
    return ws

# ---------- Termux WebSocket ----------

async def handle_termux(req):
    log(f"termux WS upgrade from {req.remote}")
    ws = web.WebSocketResponse(
        heartbeat=25,
        autoping=True,
        timeout=30,
        max_msg_size=4*1024*1024,
    )
    try:
        await ws.prepare(req)
        log(f"termux WS upgrade success")
    except Exception as e:
        log(f"termux prepare error: {e}")
        log(tb.format_exc())
        return ws

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

        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    d = json.loads(msg.data)
                except:
                    continue

                kind = d.get("t")

                if kind in ("o", "res", "fd"):
                    await broadcast_browsers({"t": kind, "d": d.get("d", {}), "src": cid, "rid": d.get("rid")})

                elif kind == "ping":
                    await safe_send(ws, {"t": "pong"})

            elif msg.type == WSMsgType.ERROR:
                log(f"termux-{cid} ws error: {ws.exception()}")
                break
            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED):
                break

    except asyncio.CancelledError:
        pass
    except Exception as e:
        log(f"termux-{cid} loop error: {e}")
        log(tb.format_exc())
    finally:
        devices.pop(cid, None)
        dev_info.pop(cid, None)
        try:
            await ws.close()
        except:
            pass
        log(f"termux-{cid} disconnected")
        await broadcast_browsers({"t": "dev_down", "id": cid})
    return ws

# ---------- Minimal WS Test ----------

async def handle_test(req):
    ws = web.WebSocketResponse(autoping=True)
    try:
        await ws.prepare(req)
    except Exception as e:
        log(f"test ws prepare error: {e}")
        return ws
    log("test WS connected")
    try:
        await ws.send_str(json.dumps({"ok": True, "msg": "ws works"}))
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                await ws.send_str(json.dumps({"echo": msg.data}))
            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED, WSMsgType.ERROR):
                break
    except Exception as e:
        log(f"test ws error: {e}")
    finally:
        log("test WS disconnected")
    return ws

# ---------- App ----------

app = web.Application()
app.router.add_get("/", serve_index)
app.router.add_get("/api/health", serve_health)
app.router.add_get("/api/debug", serve_debug)
app.router.add_get("/ws/test", handle_test)
app.router.add_get("/ws/b", handle_browser)
app.router.add_get("/ws/t", handle_termux)

async def on_startup(app):
    log(f"=== SERVER STARTED port={PORT} token={TOKEN} ===")
    log(f"Routes: /, /api/health, /api/debug, /ws/test, /ws/b, /ws/t")

app.on_startup.append(on_startup)

if __name__ == "__main__":
    log(f"Booting on port {PORT}...")
    web.run_app(app, host="0.0.0.0", port=PORT, print=None, access_log=None)
