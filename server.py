#!/usr/bin/env python3
"""
Termux Remote v9 — Clean rebuild
Starlette + uvicorn. Minimal relay: Browser ↔ Server ↔ Termux.
"""
import os, sys, json, time, asyncio, traceback as tb
from starlette.applications import Starlette
from starlette.routing import Route, WebSocketRoute
from starlette.responses import JSONResponse, FileResponse
from starlette.websockets import WebSocket, WebSocketDisconnect
import uvicorn

PORT = int(os.environ.get("PORT", 2010))
TOKEN = os.environ.get("AUTH_TOKEN", "termux-remote-2024")

device_ws = None   # single connected Termux device
browser_ws = []   # all connected browsers
device_name = ""

def log(*a):
    print(f"[{time.strftime('%H:%M:%S')}]", *a, flush=True)

# ── HTTP ──────────────────────────────────────────

async def index(req):
    fp = os.path.join(os.path.dirname(os.path.abspath(__file__)), "public", "index.html")
    return FileResponse(fp)

async def health(req):
 return JSONResponse({"ok": True, "device": device_name or None})

# ── Browser WebSocket ─────────────────────────────

async def ws_browser(ws: WebSocket):
    global browser_ws
    await ws.accept()
    browser_ws.append(ws)
    log(f"browser+ ({len(browser_ws)} total)")

    # Send current device status immediately
    if device_ws and not device_ws[0].closed:
        await ws.send_json({"t": "connected", "name": device_name})
    else:
        await ws.send_json({"t": "waiting"})

    try:
        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)
            kind = msg.get("t")

            # Forward input to Termux
            if kind == "i" and device_ws and not device_ws[0].closed:
                await device_ws[0].send_json(msg)

            # Forward resize to Termux
            elif kind == "r" and device_ws and not device_ws[0].closed:
                await device_ws[0].send_json(msg)

            # File operations
            elif kind in ("fr", "fu", "fd") and device_ws and not device_ws[0].closed:
                await device_ws[0].send_json(msg)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        log(f"browser error: {e}")
    finally:
        if ws in browser_ws:
            browser_ws.remove(ws)
        log(f"browser- ({len(browser_ws)} total)")

# ── Termux WebSocket ──────────────────────────────

async def ws_termux(ws: WebSocket):
    global device_ws, device_name
    await ws.accept()

    # Auth
    token = ws.query_params.get("token", "")
    if token != TOKEN:
        log("termux auth FAILED")
        await ws.send_json({"t": "err", "msg": "bad token"})
        await ws.close()
        return

    device_ws = [ws]
    device_name = ws.query_params.get("device", "Termux")
    log(f"termux connected: {device_name}")

    # Notify all browsers
    for bws in browser_ws:
        if not bws.closed:
            try:
                await bws.send_json({"t": "connected", "name": device_name})
            except:
                pass

    try:
        async for raw in ws:
            msg = json.loads(raw)
            kind = msg.get("t")

            # Broadcast terminal output + file results to all browsers
            if kind in ("o", "res", "fd"):
                dead = []
                for bws in browser_ws:
                    if bws.closed:
                        dead.append(bws)
                        continue
                    try:
                        await bws.send_json(msg)
                    except:
                        dead.append(bws)
                for d in dead:
                    if d in browser_ws:
                        browser_ws.remove(d)

            elif kind == "ping":
                await ws.send_json({"t": "pong"})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        log(f"termux error: {e}")
    finally:
        device_ws = None
        device_name = ""
        log("termux disconnected")
        # Notify browsers
        for bws in browser_ws:
            if not bws.closed:
                try:
                    await bws.send_json({"t": "disconnected"})
                except:
                    pass

# ── App ────────────────────────────────────────────

routes = [
    Route("/", index),
    Route("/api/health", health),
    WebSocketRoute("/ws/b", ws_browser),
    WebSocketRoute("/ws/t", ws_termux),
]

app = Starlette(routes=routes)

@app.on_event("startup")
async def _start():
    log(f"v9 ready port={PORT}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")
