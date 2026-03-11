#!/usr/bin/env python3
"""
Start the Flask web UI on port 5050.

Usage:
  python run_web.py
  python run_web.py --port 8080
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
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    init_db()
    print(f"Starting web UI at http://{args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=args.debug, use_reloader=False)
