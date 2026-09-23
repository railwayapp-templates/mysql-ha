"""Test-only proxy: emulate a wrapper predating the GTID block-size advert."""
import json
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class LegacyPeer(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            with urllib.request.urlopen("http://127.0.0.1:8081" + self.path, timeout=2) as response:
                body = response.read()
                status = response.status
        except urllib.error.HTTPError as error:
            body, status = error.read(), error.code
        except OSError:
            self.send_error(503)
            return
        if self.path == "/gr/state" and status == 200:
            state = json.loads(body)
            state.pop("gtid_assignment_block_size", None)
            body = json.dumps(state).encode()
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


ThreadingHTTPServer(("0.0.0.0", 8080), LegacyPeer).serve_forever()
