#!/usr/bin/env python3
"""Termux Remote - Railway Server v4"""
import os, sys, json, time, traceback
from aiohttp import web, WSMsgType

PORT = int(os.environ.get("PORT", 2010))
AUTH_TOKEN = os.environ.get("AUTH_TOKEN", "termux-remote-2024")

_browsers = {}
_termux = {}
_meta = {}

_log = lambda m: print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

async def index(req):
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "public", "index.html")
    return web.FileResponse(p)

async def health(req):
    n = len([w for w in _termux.values() if not w.closed])
    return web.json_response({"status": "ok", "port": PORT, "devices": n})

async def ws_browser(req):
    ws = web.WebSocketResponse()
    try:
        await ws.prepare(req)
    except Exception as e:
        _log(f"WS prepare fail: {e}")
        return ws

    cid = str(int(time.time()*1000))[-6:]
    _browsers[cid] = ws
    _log(f"Browser {cid} OPEN")

    try:
        # Send init - inside try/except!
        await ws.send_str(json.dumps({"type": "init", "client_id": cid}))
        _log(f"Browser {cid} sent init")

        devs = [{"id": t, "info": _meta.get(t, {}).get("device_info", "?")}
                for t, w in list(_termux.items()) if not w.closed]
        await ws.send_str(json.dumps({"type": "devices", "devices": devs}))
        _log(f"Browser {cid} sent devices ({len(devs)})")

        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except:
                    continue
                act = data.get("action")
                tgt = data.get("target")

                if act == "list_devices":
                    devs = [{"id": t, "info": _meta.get(t, {}).get("device_info", "?")}
                            for t, w in list(_termux.items()) if not w.closed]
                    await ws.send_str(json.dumps({"type": "devices", "devices": devs}))

                elif act == "ping":
                    await ws.send_str(json.dumps({"type": "pong"}))

                elif tgt and tgt in _termux and not _termux[tgt].closed:
                    tw = _termux[tgt]
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
                        _log(f"Relay to {tgt} err: {e}")

            elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
                break

    except Exception as e:
        _log(f"Browser {cid} ERR: {e}\n{traceback.format_exc()}")
    finally:
        _browsers.pop(cid, None)
        _log(f"Browser {cid} CLOSED")
    return ws


async def ws_termux(req):
    ws = web.WebSocketResponse()
    try:
        await ws.prepare(req)
    except Exception as e:
        _log(f"Termux WS prepare fail: {e}")
        return ws

    token = req.query.get("token", "")
    if token != AUTH_TOKEN:
        _log("Termux auth FAIL")
        await ws.send_str(json.dumps({"type": "error", "msg": "bad token"}))
        await ws.close()
        return ws

    cid = str(int(time.time()*1000))[-6:]
    _termux[cid] = ws
    info = req.query.get("device", "Termux")
    _meta[cid] = {"device_info": info, "ts": time.time()}
    _log(f"Termux {cid} OPEN ({info})")

    try:
        await ws.send_str(json.dumps({"type": "init", "client_id": cid}))
        # notify browsers
        payload = json.dumps({"type": "device_connected", "device": {"id": cid, "info": info}})
        for bw in list(_browsers.values()):
            if not bw.closed:
                try: await bw.send_str(payload)
                except: pass

        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except:
                    continue
                mt = data.get("type")
                if mt in ("output", "result", "file_data"):
                    out = json.dumps({"type": mt, "data": data.get("data", {}), "source": cid, "req_id": data.get("req_id", "")})
                    for bw in list(_browsers.values()):
                        if not bw.closed:
                            try: await bw.send_str(out)
                            except: pass
                elif mt == "ping":
                    await ws.send_str(json.dumps({"type": "pong"}))

            elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
                break

    except Exception as e:
        _log(f"Termux {cid} ERR: {e}\n{traceback.format_exc()}")
    finally:
        _termux.pop(cid, None)
        _meta.pop(cid, None)
        _log(f"Termux {cid} CLOSED")
        disc = json.dumps({"type": "device_disconnected", "device_id": cid})
        for bw in list(_browsers.values()):
            if not bw.closed:
                try: await bw.send_str(disc)
                except: pass
    return ws


app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/api/health", health)
app.router.add_get("/ws/browser", ws_browser)
app.router.add_get("/ws/termux", ws_termux)

if __name__ == "__main__":
    _log(f"=== START port={PORT} ===")
    web.run_app(app, host="0.0.0.0", port=PORT, print=None)
