#!/usr/bin/env python3
"""Termux Remote Controller - Server v8
Starlette + Uvicorn (aiohttp WS broken on Railway Hikari proxy).
"""
import os, sys, json, time, asyncio, traceback as tb
from starlette.applications import Starlette
from starlette.routing import Route, WebSocketRoute
from starlette.responses import JSONResponse, FileResponse
from starlette.websockets import WebSocket, WebSocketDisconnect
from starlette.middleware.cors import CORSMiddleware
import uvicorn

PORT = int(os.environ.get("PORT", 2010))
TOKEN = os.environ.get("AUTH_TOKEN", "termux-remote-2024")

brokers = {}   # cid -> ws

devices = {}   # cid -> ws

dev_info = {}  # cid -> name

def log(*a):
    print(f"[{time.strftime('%H:%M:%S')}]", *a, flush=True)

# ---------- HTTP ----------

async def serve_index(request):
    fp = os.path.join(os.path.dirname(os.path.abspath(__file__)), "public", "index.html")
    return FileResponse(fp)

async def serve_health(request):
    nd = sum(1 for w in devices.values())
    return JSONResponse({"ok": True, "devices": nd})

async def serve_debug(request):
    return JSONResponse({
        "ok": True,
        "port": PORT,
        "python": sys.version.split()[0],
        "framework": "starlette+uvicorn",
        "devices": len(devices),
        "brokers": len(brokers),
    })

# ---------- Helpers ----------

async def safe_send(ws, obj):
    try:
        await ws.send_json(obj)
        return True
    except:
        return False

async def broadcast_browsers(obj):
    dead = []
    for cid, ws in brokers.items():
        if not await safe_send(ws, obj):
            dead.append(cid)
    for cid in dead:
        brokers.pop(cid, None)

def get_device_list():
    return [{"id": c, "name": dev_info.get(c, "Termux")}
           for c in devices]

# ---------- Browser WebSocket ----------

async def handle_browser(ws: WebSocket):
    await ws.accept()
    log(f"browser WS accepted")

    cid = str(int(time.time() * 1000) % 1000000)
    brokers[cid] = ws
    log(f"browser-{cid} connected")

    try:
        await safe_send(ws, {"t": "hi", "id": cid})
        await safe_send(ws, {"t": "devs", "list": get_device_list()})

        while True:
            data = await ws.receive_text()
            try:
                d = json.loads(data)
            except:
                continue

            kind = d.get("a")

            if kind == "ls":
                await safe_send(ws, {"t": "devs", "list": get_device_list()})

            elif kind == "i":
                target = d.get("to")
                if target in devices:
                    await safe_send(devices[target], {"t": "i", "d": d.get("d", "")})

            elif kind == "r":
                target = d.get("to")
                if target in devices:
                    await safe_send(devices[target], {"t": "r", "c": d.get("c", 80), "rows": d.get("rows", 24)})

            elif kind == "fr":
                target = d.get("to")
                if target in devices:
                    await safe_send(devices[target], {"t": "fr", "p": d.get("p", "."), "m": d.get("m", "list"), "q": d.get("q", "*"), "rid": d.get("rid")})

            elif kind == "fu":
                target = d.get("to")
                if target in devices:
                    await safe_send(devices[target], {"t": "fu", "p": d.get("p", ""), "c": d.get("c", ""), "rid": d.get("rid")})

            elif kind == "fd":
                target = d.get("to")
                if target in devices:
                    await safe_send(devices[target], {"t": "fd", "p": d.get("p", ""), "rid": d.get("rid")})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        log(f"browser-{cid} error: {e}")
        log(tb.format_exc())
    finally:
        brokers.pop(cid, None)
        log(f"browser-{cid} disconnected")

# ---------- Termux WebSocket ----------

async def handle_termux(ws: WebSocket):
    await ws.accept()
    log(f"termux WS accepted")

    token = ws.query_params.get("token", "")
    if token != TOKEN:
        log("termux auth rejected")
        await safe_send(ws, {"t": "err", "msg": "bad token"})
        await ws.close()
        return

    cid = str(int(time.time() * 1000) % 1000000)
    name = ws.query_params.get("device", "Termux")
    devices[cid] = ws
    dev_info[cid] = name
    log(f"termux-{cid} connected ({name})")

    try:
        await safe_send(ws, {"t": "hi", "id": cid})
        await broadcast_browsers({"t": "dev_up", "id": cid, "name": name})

        while True:
            data = await ws.receive_text()
            try:
                d = json.loads(data)
            except:
                continue

            kind = d.get("t")

            if kind in ("o", "res", "fd"):
                await broadcast_browsers({"t": kind, "d": d.get("d", {}), "src": cid, "rid": d.get("rid")})

            elif kind == "ping":
                await safe_send(ws, {"t": "pong"})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        log(f"termux-{cid} error: {e}")
        log(tb.format_exc())
    finally:
        devices.pop(cid, None)
        dev_info.pop(cid, None)
        log(f"termux-{cid} disconnected")
        await broadcast_browsers({"t": "dev_down", "id": cid})

# ---------- Test WS ----------

async def handle_test(ws: WebSocket):
    await ws.accept()
    log("test WS connected")
    try:
        await ws.send_json({"ok": True, "msg": "ws works"})
        while True:
            data = await ws.receive_text()
            await ws.send_json({"echo": data})
    except WebSocketDisconnect:
        pass
    finally:
        log("test WS disconnected")

# ---------- App ----------

routes = [
    Route("/", serve_index),
    Route("/api/health", serve_health),
    Route("/api/debug", serve_debug),
    WebSocketRoute("/ws/test", handle_test),
    WebSocketRoute("/ws/b", handle_browser),
    WebSocketRoute("/ws/t", handle_termux),
]

app = Starlette(routes=routes)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
async def on_startup():
    log(f"=== SERVER STARTED port={PORT} token={TOKEN} ===")
    log(f"Framework: starlette + uvicorn")

if __name__ == "__main__":
    log(f"Booting on port {PORT}...")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")
