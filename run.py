"""
Run script for Local AI Shuttle Control Center.
Binds strictly to 127.0.0.1.
"""
import sys
import uvicorn

if __name__ == "__main__":
    print("Starting Local AI Shuttle Control Center on http://127.0.0.1:8765 ...")
    uvicorn.run("app.main:app", host="127.0.0.1", port=8765, reload=False, log_level="info")
