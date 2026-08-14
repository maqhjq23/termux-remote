#!/usr/bin/env python3
"""Termux Remote - v5 debug"""
import os, json, time, asyncio, sys
from aiohttp import web, WSMsgType

PORT = int(os.environ.get("PORT", 2010))
AUTH_TOKEN = os.environ.get("AUTH_TOKEN", "termux-remote-2024")

_log = lambda m: print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

termux_devs = {}

class WSRelay:
    """Manages all WebSocket connections."""
    def __init__(self):
        self.browser_conns = {}  # id -> ws
        self.termux_conns = {}  # id -> ws
        self.termux_meta = {}

    def add_browser(self, ws, cid):
        self.browser_conns[cid] = ws

    def add_termux(self, ws, cid, info):
        self.termux_conns[cid] = ws
        self.termux_meta[cid] = info

    def remove_browser(self, cid):
        self.browser_conns.pop(cid, None)

    def remove_termux(self, cid):
        self.termux_conns.pop(cid, None)
        self.termux_meta.pop(cid, None)

    def get_devices(self):
        return [{"id": t, "info": self.termux_meta.get(t, "?")} for t, w in self.termux_conns.items() if not w.closed]

relay = WSRelay()

async def index(req):
    return web.FileResponse(os.path.join(os.path.dirname(os.path.abspath(__file__)), "public", "index.html"))

async def health(req):
    return web.json_response({"status": "ok", "port": PORT, "devices": len(relay.get_devices())})

async def ws_browser_handler(req):
    ws = web.WebSocketResponse()
    try:
        await ws.prepare(req)
    except Exception as e:
        _log(f"prepare err: {e}")
        return ws

    cid = str(int(time.time() * 1000))[-6:]
    relay.add_browser(ws, cid)
    _log(f"browser {cid} open (total browsers: {len(relay.browser_conns)})")

    try:
        # Send init
        msg = json.dumps({"type": "init", "client_id": cid})
        await ws.send_str(msg)
        _log(f"browser {cid} -> init sent")

        # Send device list
        devs = relay.get_devices()
        msg = json.dumps({"type": "devices", "devices": devs})
        await ws.send_str(msg)
        _log(f"browser {cid} -> devices sent ({len(devs)})")

        # Message loop
        while not ws.closed:
            msg = await ws.receive()

            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue

                act = data.get("action")
                tgt = data.get("target")

                if act == "list_devices":
                    devs = relay.get_devices()
                    await ws.send_str(json.dumps({"type": "devices", "devices": devs}))

                elif act == "ping":
                    await ws.send_str(json.dumps({"type": "pong"}))

                elif tgt and tgt in relay.termux_conns and not relay.termux_conns[tgt].closed:
                    tw = relay.termux_conns[tgt]
                    try:
                        if act == "input":
                            await tw.send_str(json.dumps({"type": "input", "data": data.get("data", "")}))
                        elif act == "resize":
                            await tw.send_str(json.dumps({"type": "resize", "cols": data.get("cols", 80), "rows": data.get("rows", 24)}))
                        elif act == "file_req":
                            await tw.send_str(json.dumps({"type": "file_req", "path": data.get("path", "."), "mode": data.get("mode", "list"), "query": data.get("query", "*"), "req_id": data.get("req_id", "")}))
                        elif act == "file_upload":
                            await tw.send_str(json.dumps({"type": "file_upload", "path": data.get("path", ""), "content": data.get("content", ""), "req_id": data.get("req_id", "")}))
                        elif act == "file_download":
                            await tw.send_str(json.dumps({"type": "file_download", "path": data.get("path", ""), "req_id": data.get("req_id", "")}))
                    except Exception as e:
                        _log(f"relay err: {e}")

            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED, WSMsgType.ERROR):
                _log(f"browser {cid} got close/error type={msg.type}")
                break

    except asyncio.CancelledError:
        _log(f"browser {cid} cancelled")
    except Exception as e:
        _log(f"browser {cid} EXCEPTION: {type(e).__name__}: {e}")
        import traceback; _log(traceback.format_exc())
    finally:
        relay.remove_browser(cid)
        _log(f"browser {cid} closed")
    return ws

async def ws_termux_handler(req):
    ws = web.WebSocketResponse()
    try:
        await ws.prepare(req)
    except Exception as e:
        _log(f"termux prepare err: {e}")
        return ws

    token = req.query.get("token", "")
    if token != AUTH_TOKEN:
        _log("termux auth FAIL")
        await ws.send_str(json.dumps({"type": "error", "msg": "bad token"}))
        await ws.close()
        return ws

    cid = str(int(time.time() * 1000))[-6:]
    relay.add_termux(ws, cid, req.query.get("device", "Termux"))
    _log(f"termux {cid} open")

    try:
        await ws.send_str(json.dumps({"type": "init", "client_id": cid}))

        # notify browsers
        info = relay.termux_meta.get(cid, "Termux")
        notif = json.dumps({"type": "device_connected", "device": {"id": cid, "info": info}})
        for bw in list(relay.browser_conns.values()):
            if not bw.closed:
                try: await bw.send_str(notif)
                except: pass

        while not ws.closed:
            msg = await ws.receive()
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except: continue
                mt = data.get("type")
                if mt in ("output", "result", "file_data"):
                    out = json.dumps({"type": mt, "data": data.get("data", {}), "source": cid, "req_id": data.get("req_id", "")})
                    for bw in list(relay.browser_conns.values()):
                        if not bw.closed:
                            try: await bw.send_str(out)
                            except: pass
                elif mt == "ping":
                    await ws.send_str(json.dumps({"type": "pong"}))
            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED, WSMsgType.ERROR):
                break

    except asyncio.CancelledError:
        pass
    except Exception as e:
        _log(f"termux {cid} EXCEPTION: {type(e).__name__}: {e}")
    finally:
        relay.remove_termux(cid)
        _log(f"termux {cid} closed")
        disc = json.dumps({"type": "device_disconnected", "device_id": cid})
        for bw in list(relay.browser_conns.values()):
            if not bw.closed:
                try: await bw.send_str(disc)
                except: pass
    return ws

app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/api/health", health)
app.router.add_get("/ws/browser", ws_browser_handler)
app.router.add_get("/ws/termux", ws_termux_handler)

if __name__ == "__main__":
    _log(f"=== START port={PORT} ===")
    web.run_app(app, host="0.0.0.0", port=PORT, print=None, access_log=None)
