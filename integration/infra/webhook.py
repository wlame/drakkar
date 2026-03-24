"""Minimal webhook sink — logs one line per request, responds 200 OK."""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length) if length else b''
        ct = self.headers.get('Content-Type', '')
        summary = ''
        if 'json' in ct:
            try:
                data = json.loads(body)
                if isinstance(data, dict):
                    keys = ', '.join(list(data.keys())[:5])
                    summary = f' keys=[{keys}]'
            except (json.JSONDecodeError, ValueError):
                pass
        print(f'POST {self.path} {length}b{summary}', flush=True)
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def do_GET(self):
        print(f'GET {self.path}', flush=True)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'ok')

    def log_message(self, format, *args):
        pass  # suppress default access logs


if __name__ == '__main__':
    server = HTTPServer(('0.0.0.0', 8080), Handler)
    print('webhook listening on :8080', flush=True)
    server.serve_forever()
