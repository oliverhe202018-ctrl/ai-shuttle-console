"""
Run script for Local AI Shuttle Control Center.
Binds strictly to 127.0.0.1.
"""
import argparse
import os
from pathlib import Path
import uvicorn

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, help="Explicit JSON configuration; isolated state database")
    parser.add_argument("--canary", action="store_true", help="Require strict canary configuration")
    parser.add_argument("--check", action="store_true", help="Read-only identity/capability preflight; no server or transfers")
    args = parser.parse_args()
    if args.canary and not args.config:
        parser.error("--canary requires --config; no default configuration fallback")
    if args.check and not args.canary:
        parser.error("--check requires --canary --config")
    if args.config:
        from app.canary import load_explicit_config
        load_explicit_config(args.config, canary=args.canary)
        os.environ["SHUTTLE_CONFIG"] = str(args.config.resolve(strict=True))
        os.environ["SHUTTLE_CANARY"] = "1" if args.canary else "0"
    if args.check:
        import asyncio
        from app.main import check_canary_targets
        config = load_explicit_config(args.config, canary=True)
        print(asyncio.run(check_canary_targets(config)))
        return
    print("Starting Local AI Shuttle Control Center on http://127.0.0.1:8765 ...")
    uvicorn.run("app.main:app", host="127.0.0.1", port=8765, reload=False, log_level="info")


if __name__ == "__main__":
    main()
