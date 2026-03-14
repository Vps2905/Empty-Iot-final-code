import csv
import io
import os
import sqlite3
import time
from contextlib import closing
from flask import Flask, jsonify, request, Response

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE_DIR, "footfall.db"))
INGEST_API_KEY = os.environ.get("INGEST_API_KEY", "change_me_ingest_key")
ADMIN_API_KEY = os.environ.get("ADMIN_API_KEY", "change_me_admin_key")
GEOFENCE_LAT = float(os.environ.get("GEOFENCE_LAT", "17.43388"))
GEOFENCE_LON = float(os.environ.get("GEOFENCE_LON", "78.42669"))
GEOFENCE_RADIUS_M = float(os.environ.get("GEOFENCE_RADIUS_M", "300"))

app = Flask(__name__)


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with closing(get_db()) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS device_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                count_value INTEGER NOT NULL DEFAULT 1,
                latitude REAL,
                longitude REAL,
                rssi INTEGER,
                firmware_version TEXT,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ingest_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT,
                status TEXT NOT NULL,
                detail TEXT,
                created_at INTEGER NOT NULL
            );
            """
        )
        conn.commit()


init_db()


def bearer_token():
    header = request.headers.get("Authorization", "")
    if header.startswith("Bearer "):
        return header.split(" ", 1)[1].strip()
    return None


def admin_authorized():
    token = bearer_token()
    return token == ADMIN_API_KEY


@app.get("/")
def home():
    return jsonify({"ok": True, "service": "footfall-backend"})


@app.get("/api/health")
def health():
    try:
        with closing(get_db()) as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM device_events").fetchone()
        return jsonify(
            {
                "ok": True,
                "service": "footfall-backend",
                "database": "ok",
                "events": int(row["c"]),
                "geofence": {
                    "lat": GEOFENCE_LAT,
                    "lon": GEOFENCE_LON,
                    "radius_m": GEOFENCE_RADIUS_M,
                },
                "timestamp": int(time.time()),
            }
        )
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.post("/ingest")
@app.post("/api/ingest")
def ingest():
    token = bearer_token()
    if token != INGEST_API_KEY:
        with closing(get_db()) as conn:
            conn.execute(
                "INSERT INTO ingest_logs(device_id, status, detail, created_at) VALUES (?, ?, ?, ?)",
                (None, "rejected", "invalid ingest key", int(time.time())),
            )
            conn.commit()
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    device_id = str(payload.get("device_id", "unknown-device"))
    events = payload.get("events") or []

    if not isinstance(events, list):
        return jsonify({"ok": False, "error": "events must be a list"}), 400

    now = int(time.time())
    inserted = 0
    with closing(get_db()) as conn:
        for evt in events:
            if not isinstance(evt, dict):
                continue
            conn.execute(
                """
                INSERT INTO device_events
                (device_id, event_type, count_value, latitude, longitude, rssi, firmware_version, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    device_id,
                    str(evt.get("event_type", "presence")),
                    int(evt.get("count", 1)),
                    evt.get("lat"),
                    evt.get("lon"),
                    evt.get("rssi"),
                    str(payload.get("firmware_version", "unknown")),
                    int(evt.get("timestamp", now)),
                ),
            )
            inserted += 1

        conn.execute(
            "INSERT INTO ingest_logs(device_id, status, detail, created_at) VALUES (?, ?, ?, ?)",
            (device_id, "accepted", f"inserted={inserted}", now),
        )
        conn.commit()

    return jsonify({"ok": True, "inserted": inserted, "device_id": device_id})


@app.get("/api/events")
def events():
    limit = min(max(int(request.args.get("limit", 100)), 1), 1000)
    with closing(get_db()) as conn:
        rows = conn.execute(
            """
            SELECT id, device_id, event_type, count_value, latitude, longitude, rssi, firmware_version, created_at
            FROM device_events
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return jsonify([
        {
            "id": row["id"],
            "device_id": row["device_id"],
            "event_type": row["event_type"],
            "count": row["count_value"],
            "lat": row["latitude"],
            "lon": row["longitude"],
            "rssi": row["rssi"],
            "firmware_version": row["firmware_version"],
            "timestamp": row["created_at"],
        }
        for row in rows
    ])


@app.get("/api/stats")
def stats():
    with closing(get_db()) as conn:
        total_events = conn.execute("SELECT COUNT(*) AS c FROM device_events").fetchone()["c"]
        total_count = conn.execute("SELECT COALESCE(SUM(count_value), 0) AS s FROM device_events").fetchone()["s"]
        unique_devices = conn.execute("SELECT COUNT(DISTINCT device_id) AS c FROM device_events").fetchone()["c"]
        latest = conn.execute(
            "SELECT device_id, event_type, count_value, created_at FROM device_events ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return jsonify(
        {
            "ok": True,
            "total_events": int(total_events),
            "total_count": int(total_count),
            "unique_devices": int(unique_devices),
            "latest_event": None
            if latest is None
            else {
                "device_id": latest["device_id"],
                "event_type": latest["event_type"],
                "count": latest["count_value"],
                "timestamp": latest["created_at"],
            },
        }
    )


@app.get("/api/export.csv")
def export_csv():
    if not admin_authorized():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    with closing(get_db()) as conn:
        rows = conn.execute(
            """
            SELECT id, device_id, event_type, count_value, latitude, longitude, rssi, firmware_version, created_at
            FROM device_events ORDER BY id DESC
            """
        ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "device_id", "event_type", "count", "lat", "lon", "rssi", "firmware_version", "timestamp"])
    for row in rows:
        writer.writerow([
            row["id"], row["device_id"], row["event_type"], row["count_value"], row["latitude"],
            row["longitude"], row["rssi"], row["firmware_version"], row["created_at"]
        ])

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=footfall_events.csv"},
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
