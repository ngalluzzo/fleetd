#!/usr/bin/env python3
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


if "--version" in sys.argv:
    print("llama.cpp mock-1.0")
    raise SystemExit(0)


def value(flag):
    return sys.argv[sys.argv.index(flag) + 1]


port = int(value("--port"))
model_id = value("--alias")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
            return
        if self.path == "/v1/models":
            body = json.dumps({"data": [{"id": model_id}]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path == "/metrics":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"llamacpp:requests_processing 0\n")
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, *_args):
        return


ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
