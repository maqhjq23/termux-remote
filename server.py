#!/usr/bin/env python3
"""
Termux Remote Controller - Railway Server
WebSocket relay + HTML frontend server
Port: 2010
"""

import os
import json
import asyncio
import uuid
import time
from aiohttp import web
from aiohttp import WSMsgType

# --- Config ---
PORT = int(os.environ.get("PORT", 2010))
AUTH_TOKEN = os.environ.get("AUTH_TOKEN", "termux-remote-2024")
MAX_MSG_SIZE = 4 * 1024 * 1024  # 4MB
HEARTBEAT = 25  # seconds - ping to keep Railway proxy alive

# --- Store ---
browser_sockets = {}   # client_id -> WebSocket
termux_sockets = {}   # client_id -> WebSocket
client_meta = {}       # client_id -> dict


def log(msg):
    ts = time.strftime('%H:%M:%S')
    print(f"[{ts}] {msg}", flush=True)


async def index(request):
    return web.FileResponse(os.path.join(os.path.dirname(__file__), "public", "index.html"))


async def ws_browser(request):
    """WebSocket for browser clients."""
    ws = web.WebSocketResponse(max_msg_size=MAX_MSG_SIZE, heartbeat=HEARTBEAT)
    try:
        await ws.prepare(request)
    except Exception as e:
        log(f"Browser WS prepare failed: {e}")
        return ws

    client_id = str(uuid.uuid4())[:8]
    browser_sockets[client_id] = ws
    client_meta[client_id] = {"type": "browser", "connected_at": time.time()}
    log(f"Browser +{client_id} connected (total: {len(browser_sockets)})")

    # Send client ID immediately
    try:
        await ws.send_json({"type": "init", "client_id": client_id})
    except Exception as e:
        log(f"Browser {client_id} send init failed: {e}")
        _cleanup_browser(client_id)
        return ws

    # Notify about already-connected Termux devices
    await _send_device_list(ws)

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue

                action = data.get("action")
                target = data.get("target")

                # Auth check for sensitive actions
                if action in ("resize", "file_req", "file_upload", "file_download"):
                    if data.get("token") != AUTH_TOKEN:
                        try:
                            await ws.send_json({"type": "error", "msg": "Unauthorized"})
                        except:
                            pass
                        continue

                # List devices doesn't need a target
                if action == "list_devices":
                    await _send_device_list(ws)
                    continue

                # All other actions need a target Termux device
                tx_ws = termux_sockets.get(target)
                if not tx_ws or tx_ws.closed:
                    try:
                        await ws.send_json({"type": "error", "msg": "Device not connected"})
                    except:
                        pass
                    continue

                if action == "input":
                    try:
                        await tx_ws.send_json({"type": "input", "data": data.get("data", "")})
                    except:
                        pass

                elif action == "resize":
                    try:
                        await tx_ws.send_json({"type": "resize", "cols": data.get("cols", 80), "rows": data.get("rows", 24)})
                    except:
                        pass

                elif action == "command":
                    try:
                        await tx_ws.send_json({"type": "command", "cmd": data.get("cmd", ""), "req_id": data.get("req_id", "")})
                    except:
                        pass

                elif action == "file_req":
                    try:
                        await tx_ws.send_json({"type": "file_req", "path": data.get("path", "."), "mode": data.get("mode", "list"), "query": data.get("query", "*"), "req_id": data.get("req_id", "")})
                    except:
                        pass

                elif action == "file_upload":
                    try:
                        await tx_ws.send_json({"type": "file_upload", "path": data.get("path", ""), "content": data.get("content", ""), "req_id": data.get("req_id", "")})
                    except:
                        pass

                elif action == "file_download":
                    try:
                        await tx_ws.send_json({"type": "file_download", "path": data.get("path", ""), "req_id": data.get("req_id", "")})
                    except:
                        pass

            elif msg.type == WSMsgType.PING:
                await ws.pong()

            elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
                break

    except Exception as e:
        log(f"Browser {client_id} loop error: {e}")
    finally:
        _cleanup_browser(client_id)

    return ws


async def ws_termux(request):
    """WebSocket for Termux device clients."""
    ws = web.WebSocketResponse(max_msg_size=MAX_MSG_SIZE, heartbeat=HEARTBEAT)
    try:
        await ws.prepare(request)
    except Exception as e:
        log(f"Termux WS prepare failed: {e}")
        return ws

    # Auth check
    token = request.query.get("token", "")
    if token != AUTH_TOKEN:
        log("Termux auth rejected (bad token)")
        try:
            await ws.send_json({"type": "error", "msg": "Invalid token"})
            await ws.close(code=4003, message=b"Bad token")
        except:
            pass
        return ws

    client_id = str(uuid.uuid4())[:8]
    termux_sockets[client_id] = ws
    device_info = request.query.get("device", "Termux Device")
    client_meta[client_id] = {"type": "termux", "device_info": device_info, "connected_at": time.time()}
    log(f"Termux +{client_id} connected: {device_info}")

    # Tell the device its ID
    try:
        await ws.send_json({"type": "init", "client_id": client_id})
    except:
        pass

    # Notify all browsers about new device
    await _broadcast_browsers({"type": "device_connected", "device": {"id": client_id, "info": device_info}})

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue

                msg_type = data.get("type")

                if msg_type == "output":
                    payload = {"type": "output", "data": data.get("data", ""), "source": client_id}
                    await _broadcast_browsers(payload)

                elif msg_type == "result":
                    payload = {"type": "result", "data": data.get("data", {}), "source": client_id, "req_id": data.get("req_id", "")}
                    await _broadcast_browsers(payload)

                elif msg_type == "file_data":
                    payload = {"type": "file_data", "data": data.get("data", {}), "source": client_id, "req_id": data.get("req_id", "")}
                    await _broadcast_browsers(payload)

                elif msg_type == "ping":
                    try:
                        await ws.send_json({"type": "pong"})
                    except:
                        pass

            elif msg.type == WSMsgType.PING:
                await ws.pong()

            elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
                break

    except Exception as e:
        log(f"Termux {client_id} loop error: {e}")
    finally:
        del termux_sockets.get(client_id, termux_sockets).__class__  # no-op check
        if client_id in termux_sockets:
            del termux_sockets[client_id]
        client_meta.pop(client_id, None)
        log(f"Termux -{client_id} disconnected")
        await _broadcast_browsers({"type": "device_disconnected", "device_id": client_id})

    return ws


async def status(request):
    devices = []
    for tid, tws in termux_sockets.items():
        if not tws.closed:
            meta = client_meta.get(tid, {})
            devices.append({"id": tid, "info": meta.get("device_info", "Unknown"), "connected_at": meta.get("connected_at", 0)})
    browsers = len([b for b in browser_sockets.values() if not b.closed])
    return web.json_response({"termux_devices": devices, "browser_clients": browsers, "uptime": time.time()})


async def health(request):
    return web.json_response({"status": "ok", "devices": len([t for t in termux_sockets.values() if not t.closed])})


# ========== Helpers ==========

async def _send_device_list(ws):
    """Send current device list to a specific browser WS."""
    devices = []
    for tid, tws in list(termux_sockets.items()):
        if not tws.closed:
            meta = client_meta.get(tid, {})
            devices.append({"id": tid, "info": meta.get("device_info", "Unknown")})
    try:
        await ws.send_json({"type": "devices", "devices": devices})
    except Exception as e:
        log(f"Failed to send device list: {e}")


async def _broadcast_browsers(payload):
    """Send a message to all connected browsers."""
    dead = []
    for bid, bws in list(browser_sockets.items()):
        if bws.closed:
            dead.append(bid)
            continue
        try:
            await bws.send_json(payload)
        except:
            dead.append(bid)
    for bid in dead:
        _cleanup_browser(bid)


def _cleanup_browser(client_id):
    if client_id in browser_sockets:
        try:
            browser_sockets[client_id].close()
        except:
            pass
        del browser_sockets[client_id]
    client_meta.pop(client_id, None)
    log(f"Browser -{client_id} cleaned up")


def create_app():
    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/ws/browser", ws_browser)
    app.router.add_get("/ws/termux", ws_termux)
    app.router.add_get("/api/status", status)
    app.router.add_get("/api/health", health)
    return app


if __name__ == "__main__":
    app = create_app()
    log(f"=== Termux Remote Controller ===")
    log(f"Port: {PORT}")
    log(f"Token: {AUTH_TOKEN}")
    log(f"Heartbeat: {HEARTBEAT}s")
    web.run_app(app, host="0.0.0.0", port=PORT)
