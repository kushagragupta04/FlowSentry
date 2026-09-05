"""
app.py — Local Alertmanager webhook receiver (Slack stub).

Receives POST requests from Alertmanager and logs them to stdout
in a readable format. This replaces a real Slack webhook for local
development and testing, while still exercising the full alert routing
pipeline: Alertmanager rule fires → webhook POST → logged output.

In production: replace the ALERTMANAGER_WEBHOOK_URL env var with
a real Slack incoming webhook URL.
"""

from __future__ import annotations

import json
import time
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler


class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)

        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"raw": body.decode("utf-8", errors="replace")}

        ts = datetime.utcnow().isoformat()
        print(f"\n{'='*60}")
        print(f"ALERT RECEIVED @ {ts}")
        print(f"{'='*60}")

        for alert in payload.get("alerts", [payload]):
            status    = alert.get("status", "unknown").upper()
            name      = alert.get("labels", {}).get("alertname", "unnamed")
            summary   = alert.get("annotations", {}).get("summary", "")
            severity  = alert.get("labels", {}).get("severity", "")
            print(f"  [{status}] {name} ({severity})")
            if summary:
                print(f"  Summary: {summary}")

        print(f"{'='*60}\n")

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def log_message(self, fmt, *args):  # suppress default access log
        pass


if __name__ == "__main__":
    port = 5001
    print(f"Webhook receiver listening on port {port}")
    HTTPServer(("0.0.0.0", port), WebhookHandler).serve_forever()
