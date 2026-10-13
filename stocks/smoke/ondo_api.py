"""A stand-in for Ondo's GM API, for run.sh: the three endpoints the `ondo` pricer reads,
answering one asset at a fixed price stamped with the current time. Standard library only.

    python3 ondo_api.py PORT API_KEY SYMBOL TOKEN_PRICE STOCK_PRICE [EVENTS_FILE]

The asset events are EVENTS_FILE's JSON, re-read on every request so the caller can change
them mid-run; none without it. Any other key gets a 401, any other path a 404, the way the
real API refuses them.
"""

import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT, KEY, SYMBOL, TOKEN_PRICE, STOCK_PRICE = sys.argv[1:6]
EVENTS = sys.argv[6] if len(sys.argv) > 6 else None


def events():
    if EVENTS is None:
        return []
    with open(EVENTS) as f:
        return json.load(f)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.headers.get("x-api-key") != KEY:
            return self.reply(401, {"error": "UNAUTHORIZED"})
        if self.path == f"/v1/assets/{SYMBOL}/prices/latest":
            return self.reply(200, {
                "primaryMarket": {"symbol": SYMBOL, "price": TOKEN_PRICE},
                "underlyingMarket": {"ticker": SYMBOL.removesuffix("on"), "price": STOCK_PRICE},
                "timestamp": int(time.time() * 1000),
            })
        if self.path == "/v1/status/market":
            return self.reply(200, {"isOpen": True, "marketStatus": "regular"})
        if self.path == "/v1/status/assets":
            return self.reply(200, events())
        return self.reply(404, {"error": "NOT_FOUND"})

    def reply(self, status, body):
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


server = ThreadingHTTPServer(("127.0.0.1", int(PORT)), Handler)
print("READY", flush=True)
server.serve_forever()
