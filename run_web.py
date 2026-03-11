#!/usr/bin/env python3
"""
Start the Flask web UI.

Usage:
  python run_web.py
  python run_web.py --port 8080

The PORT environment variable takes precedence over --port when set
(required for Railway and similar platforms).
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from database import init_db
from web.app import app

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PT Clinic Web UI")
    parser.add_argument("--port", type=int, default=5050)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    port = int(os.environ.get("PORT", args.port))
    host = args.host

    init_db()
    print(f"Starting web UI at http://{host}:{port}")
    app.run(host=host, port=port, debug=args.debug, use_reloader=False)
