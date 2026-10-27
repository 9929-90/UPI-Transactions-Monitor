from __future__ import annotations
import json, os, threading
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
WEB = ROOT / "dashboard" / "web"
HOST = os.getenv("UPI_DASHBOARD_HOST", "127.0.0.1")
PORT = int(os.getenv("UPI_DASHBOARD_PORT", "8765"))
threshold = float(os.getenv("UPI_HIGH_VALUE_THRESHOLD", "50000"))
threshold_lock = threading.Lock()
DATASETS = {
    "normal": DATA / "processed" / "transactions_v4" / "normal",
    "flagged": DATA / "processed" / "transactions_v4" / "flagged",
    "velocity": DATA / "processed" / "transactions_v4" / "velocity_flags_v2",
    "quarantine": DATA / "quarantine" / "transactions_v4",
}

def safe(value):
    if isinstance(value, dict): return {str(k): safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)): return [safe(v) for v in value]
    if hasattr(value, "isoformat"): return value.isoformat()
    if hasattr(value, "item"):
        try: return safe(value.item())
        except (ValueError, TypeError): pass
    if isinstance(value, float) and (value != value or abs(value) == float("inf")): return None
    return value

def number(value, default=0.0):
    try: return float(value)
    except (TypeError, ValueError): return default

def current_threshold():
    with threshold_lock: return threshold

def files_in(folder):
    return sorted(p for p in folder.rglob("*.parquet") if p.is_file()) if folder.exists() else []

def read_dataset(name):
    files = files_in(DATASETS[name])
    if not files: return []
    paths = "[" + ",".join("'" + str(p).replace("\\", "/").replace("'", "''") + "'" for p in files) + "]"
    con = duckdb.connect(":memory:")
    try:
        df = con.execute("SELECT * FROM read_parquet(" + paths + ", union_by_name=true)").fetchdf()
        return [safe(row) for row in df.to_dict(orient="records")]
    finally: con.close()

def transactions():
    rows = read_dataset("normal") + read_dataset("flagged")
    for row in rows:
        row["risk_level"] = "HIGH" if str(row.get("risk_level", "")).upper() == "HIGH" or number(row.get("amount")) >= current_threshold() else "NORMAL"
        row["status"] = str(row.get("status") or "UNKNOWN").upper()
        row["event_time"] = row.get("event_time") or row.get("event_timestamp")
    rows.sort(key=lambda r: str(r.get("event_time") or ""), reverse=True)
    return rows

def summary():
    rows = transactions()
    quarantine = read_dataset("quarantine")
    velocity = read_dataset("velocity")
    statuses = {s: sum(r.get("status") == s for r in rows) for s in ("SUCCESS", "FAILED", "PENDING", "UNKNOWN")}
    reasons = {}
    for row in quarantine:
        reason = str(row.get("validation_error") or "unknown")
        reasons[reason] = reasons.get(reason, 0) + 1
    flagged = [r for r in rows if r.get("risk_level") == "HIGH"]
    total = len(rows)
    return {"total_transactions": total, "success_count": statuses["SUCCESS"],
        "success_rate": round(100 * statuses["SUCCESS"] / total, 2) if total else 0,
        "normal_count": sum(r.get("risk_level") != "HIGH" for r in rows),
        "high_value_count": len(flagged), "flagged_count": len(read_dataset("flagged")),
        "quarantine_count": len(quarantine), "velocity_alert_windows": len(velocity),
        "transaction_value": round(sum(number(r.get("amount")) for r in rows), 2),
        "flagged_value": round(sum(number(r.get("amount")) for r in flagged), 2),
        "threshold": current_threshold(), "status_counts": statuses,
        "quarantine_reasons": reasons, "updated_at": datetime.now().astimezone().isoformat()}

class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs): super().__init__(*args, directory=str(WEB), **kwargs)
    def log_message(self, fmt, *args): print("[dashboard] " + (fmt % args))
    def send_json(self, payload, status=200):
        body = json.dumps(safe(payload), ensure_ascii=False, allow_nan=False).encode("utf-8")
        self.send_response(status); self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store"); self.send_header("Content-Length", str(len(body)))
        self.end_headers(); self.wfile.write(body)
    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/api/health":
            ds = {}
            for name, folder in DATASETS.items():
                fs = files_in(folder)
                ds[name] = {"available": bool(fs), "file_count": len(fs), "path": str(folder.relative_to(ROOT)).replace("\\", "/")}
            return self.send_json({"status":"ok","mode":"local-parquet","threshold":current_threshold(),"datasets":ds,"generated_at":datetime.now().astimezone().isoformat()})
        if path == "/api/summary": return self.send_json(summary())
        if path in ("/api/transactions", "/api/alerts", "/api/velocity", "/api/quarantine"):
            params = parse_qs(urlparse(self.path).query)
            rows = transactions() if path in ("/api/transactions", "/api/alerts") else read_dataset("velocity" if path == "/api/velocity" else "quarantine")
            if path == "/api/alerts": rows = [r for r in rows if r.get("risk_level") == "HIGH"]
            q = params.get("q", [""])[0].strip().lower()
            st = params.get("status", [""])[0].upper()
            risk = params.get("risk", [""])[0].upper()
            day = params.get("date", [""])[0]
            if q: rows = [r for r in rows if q in " ".join(str(r.get(k, "")) for k in ("transaction_id","user_id","merchant_id","payment_method","validation_error")).lower()]
            if st: rows = [r for r in rows if str(r.get("status", "")).upper() == st]
            if risk: rows = [r for r in rows if str(r.get("risk_level", "")).upper() == risk]
            if day: rows = [r for r in rows if str(r.get("event_time") or r.get("event_timestamp") or "").startswith(day)]
            try: limit = min(max(int(params.get("limit", ["500"])[0]), 1), 5000)
            except ValueError: limit = 500
            return self.send_json({"count":len(rows),"records":rows[:limit]})
        if path == "/api/settings": return self.send_json({"threshold":current_threshold()})
        if path == "/": self.path = "/index.html"
        return super().do_GET()
    def do_POST(self):
        global threshold
        if urlparse(self.path).path != "/api/settings": return self.send_json({"error":"Endpoint not found"},404)
        try:
            size = int(self.headers.get("Content-Length","0"))
            if size > 16384: return self.send_json({"error":"Request too large"},413)
            body = json.loads(self.rfile.read(size) or b"{}")
            value = float(body.get("threshold"))
            if value <= 0 or value != value or value == float("inf"): raise ValueError("Threshold must be finite and greater than zero.")
            with threshold_lock: threshold = value
            return self.send_json({"threshold":threshold,"updated":True})
        except (ValueError, TypeError, json.JSONDecodeError) as exc: return self.send_json({"error":str(exc)},400)

if __name__ == "__main__":
    WEB.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print("UPI Sentinel dashboard: http://" + HOST + ":" + str(PORT))
    print("Reads local Parquet files from " + str(DATA))
    try: server.serve_forever()
    except KeyboardInterrupt: print("\nStopping dashboard...")
    finally: server.server_close()
