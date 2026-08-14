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
MAX_MSG_SIZE = 4 * 1024 * 1024  # 4MB for file transfer

# --- Store ---
# client_id -> WebSocket (browser)
browser_sockets = {}
# client_id -> WebSocket (termux)
termux_sockets = {}
# client_id -> metadata
client_meta = {}
# File transfer chunks
file_transfers = {}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")


async def index(request):
    return web.FileResponse(os.path.join(os.path.dirname(__file__), "public", "index.html"))


async def ws_browser(request):
    """WebSocket for browser (xterm.js) clients"""
    ws = web.WebSocketResponse(max_msg_size=MAX_MSG_SIZE)
    await ws.prepare(request)

    client_id = str(uuid.uuid4())[:8]
    browser_sockets[client_id] = ws
    client_meta[client_id] = {"type": "browser", "connected_at": time.time()}
    log(f"Browser connected: {client_id}")

    # Send client ID
    await ws.send_json({"type": "init", "client_id": client_id})

    # Notify about connected Termux devices
    tx_ids = list(termux_sockets.keys())
    if tx_ids:
        devices = []
        for tid in tx_ids:
            meta = client_meta.get(tid, {})
            devices.append({"id": tid, "info": meta.get("device_info", "Unknown")})
        await ws.send_json({"type": "devices", "devices": devices})

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue

                target = data.get("target")
                action = data.get("action")

                # Check auth for control actions
                if action in ("resize", "file_req", "file_upload", "file_download"):
                    token = data.get("token", "")
                    if token != AUTH_TOKEN:
                        await ws.send_json({"type": "error", "msg": "Unauthorized"})
                        continue

                # Find target Termux device
                tx_ws = termux_sockets.get(target)
                if not tx_ws and action != "list_devices":
                    await ws.send_json({"type": "error", "msg": "Device not connected"})
                    continue

                if action == "input":
                    # Send keystroke/input to Termux PTY
                    if tx_ws and not tx_ws.closed:
                        await tx_ws.send_json({
                            "type": "input",
                            "data": data.get("data", "")
                        })

                elif action == "resize":
                    # Resize terminal
                    if tx_ws and not tx_ws.closed:
                        await tx_ws.send_json({
                            "type": "resize",
                            "cols": data.get("cols", 80),
                            "rows": data.get("rows", 24)
                        })

                elif action == "command":
                    # Execute a command directly
                    if tx_ws and not tx_ws.closed:
                        await tx_ws.send_json({
                            "type": "command",
                            "cmd": data.get("cmd", ""),
                            "req_id": data.get("req_id", "")
                        })

                elif action == "file_req":
                    # Request file listing or content from Termux
                    if tx_ws and not tx_ws.closed:
                        await tx_ws.send_json({
                            "type": "file_req",
                            "path": data.get("path", "."),
                            "mode": data.get("mode", "list"),  # list, read, find
                            "req_id": data.get("req_id", "")
                        })

                elif action == "file_upload":
                    # Upload file to Termux
                    if tx_ws and not tx_ws.closed:
                        await tx_ws.send_json({
                            "type": "file_upload",
                            "path": data.get("path", ""),
                            "content": data.get("content", ""),
                            "req_id": data.get("req_id", "")
                        })

                elif action == "file_download":
                    # Request file download from Termux
                    if tx_ws and not tx_ws.closed:
                        await tx_ws.send_json({
                            "type": "file_download",
                            "path": data.get("path", ""),
                            "req_id": data.get("req_id", "")
                        })

                elif action == "list_devices":
                    devices = []
                    for tid, tws in termux_sockets.items():
                        if not tws.closed:
                            meta = client_meta.get(tid, {})
                            devices.append({
                                "id": tid,
                                "info": meta.get("device_info", "Unknown")
                            })
                    await ws.send_json({"type": "devices", "devices": devices})

            elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
                break

    except Exception as e:
        log(f"Browser {client_id} error: {e}")
    finally:
        del browser_sockets[client_id]
        client_meta.pop(client_id, None)
        log(f"Browser disconnected: {client_id}")

    return ws


async def ws_termux(request):
    """WebSocket for Termux device clients"""
    ws = web.WebSocketResponse(max_msg_size=MAX_MSG_SIZE)
    await ws.prepare(request)

    # Auth check
    token = request.query.get("token", "")
    if token != AUTH_TOKEN:
        await ws.send_json({"type": "error", "msg": "Invalid token"})
        await ws.close()
        return ws

    client_id = str(uuid.uuid4())[:8]
    termux_sockets[client_id] = ws

    # Get device info
    device_info = request.query.get("device", "Termux Device")
    client_meta[client_id] = {
        "type": "termux",
        "device_info": device_info,
        "connected_at": time.time()
    }

    log(f"Termux device connected: {client_id} ({device_info})")

    # Notify all browsers
    notification = {
        "type": "device_connected",
        "device": {"id": client_id, "info": device_info}
    }
    for bid, bws in list(browser_sockets.items()):
        if not bws.closed:
            try:
                await bws.send_json(notification)
            except:
                pass

    # Send init to termux
    await ws.send_json({"type": "init", "client_id": client_id})

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue

                msg_type = data.get("type")

                if msg_type == "output":
                    # Relay terminal output to all browsers
                    for bid, bws in list(browser_sockets.items()):
                        if not bws.closed:
                            try:
                                await bws.send_json({
                                    "type": "output",
                                    "data": data.get("data", ""),
                                    "source": client_id
                                })
                            except:
                                pass

                elif msg_type == "result":
                    # Relay command/file result to all browsers
                    for bid, bws in list(browser_sockets.items()):
                        if not bws.closed:
                            try:
                                await bws.send_json({
                                    "type": "result",
                                    "data": data.get("data", {}),
                                    "source": client_id,
                                    "req_id": data.get("req_id", "")
                                })
                            except:
                                pass

                elif msg_type == "file_data":
                    # Relay file data (download) to browsers
                    for bid, bws in list(browser_sockets.items()):
                        if not bws.closed:
                            try:
                                await bws.send_json({
                                    "type": "file_data",
                                    "data": data.get("data", {}),
                                    "source": client_id,
                                    "req_id": data.get("req_id", "")
                                })
                            except:
                                pass

                elif msg_type == "ping":
                    await ws.send_json({"type": "pong"})

            elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
                break

    except Exception as e:
        log(f"Termux {client_id} error: {e}")
    finally:
        del termux_sockets[client_id]
        client_meta.pop(client_id, None)
        log(f"Termux disconnected: {client_id}")

        # Notify browsers
        for bid, bws in list(browser_sockets.items()):
            if not bws.closed:
                try:
                    await bws.send_json({
                        "type": "device_disconnected",
                        "device_id": client_id
                    })
                except:
                    pass

    return ws


async def status(request):
    """API: Get connected devices status"""
    devices = []
    for tid, tws in termux_sockets.items():
        if not tws.closed:
            meta = client_meta.get(tid, {})
            devices.append({
                "id": tid,
                "info": meta.get("device_info", "Unknown"),
                "connected_at": meta.get("connected_at", 0)
            })
    browsers = len([b for b in browser_sockets.values() if not b.closed])
    return web.json_response({
        "termux_devices": devices,
        "browser_clients": browsers,
        "uptime": time.time()
    })


async def health(request):
    return web.json_response({"status": "ok"})


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
    log(f"Starting Termux Remote Controller on port {PORT}")
    log(f"Auth Token: {AUTH_TOKEN}")
    web.run_app(app, host="0.0.0.0", port=PORT)
