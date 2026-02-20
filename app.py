import os
import time
import sqlite3
from math import radians, sin, cos, sqrt, atan2

from flask import Flask, request, jsonify, render_template
from flask_socketio import SocketIO

DB_PATH = os.environ.get("DB_PATH", "footfall.db")

# Demo config (can be overridden via env vars)
GEOFENCE_LAT = float(os.environ.get("GEOFENCE_LAT", "17.456"))
GEOFENCE_LON = float(os.environ.get("GEOFENCE_LON", "78.417"))
GEOFENCE_RADIUS_M = float(os.environ.get("GEOFENCE_RADIUS_M", "300"))

ACTIVE_WINDOW_SEC = int(os.environ.get("ACTIVE_WINDOW_SEC", "30"))
EXPOSURE_DWELL_SEC = int(os.environ.get("EXPOSURE_DWELL_SEC", "30"))

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret")

# IMPORTANT: eventlet breaks on Python 3.13 on Windows.
# Force threading async mode (works for demo + deploy; browser still "live" via polling + emits where supported).
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")


def db():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def init_db():
    con = db()
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        received_ts INTEGER NOT NULL,
        timestamp INTEGER,
        mac_hash TEXT,
        rssi INTEGER,
        lat REAL,
        lon REAL,
        dwell_time_sec INTEGER,
        campaign_id TEXT,
        activation_name TEXT,
        asset_id TEXT,
        creative_id TEXT,
        timestamp_start_utc INTEGER,
        timestamp_end_utc INTEGER
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_mac_hash ON events(mac_hash)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_received_ts ON events(received_ts)")
    con.commit()
    con.close()


def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    p1 = radians(lat1)
    p2 = radians(lat2)
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(p1) * cos(p2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c


def inside_geofence(lat, lon):
    if lat is None or lon is None:
        return False
    return haversine_m(lat, lon, GEOFENCE_LAT, GEOFENCE_LON) <= GEOFENCE_RADIUS_M


def compute_stats():
    con = db()
    cur = con.cursor()
    now = int(time.time())

    # total unique
    cur.execute("SELECT COUNT(DISTINCT mac_hash) AS n FROM events WHERE mac_hash IS NOT NULL")
    total_unique = cur.fetchone()["n"] or 0

    # active devices: last seen within ACTIVE_WINDOW_SEC (use received_ts)
    cur.execute("""
      SELECT mac_hash, MAX(received_ts) AS last_seen, AVG(rssi) AS avg_rssi
      FROM events
      WHERE mac_hash IS NOT NULL
      GROUP BY mac_hash
      HAVING (? - last_seen) <= ?
    """, (now, ACTIVE_WINDOW_SEC))
    active_rows = cur.fetchall()
    active_devices = len(active_rows)

    # avg rssi of active
    if active_devices > 0:
        avg_rssi = sum([r["avg_rssi"] for r in active_rows if r["avg_rssi"] is not None]) / active_devices
    else:
        avg_rssi = 0.0

    # avg dwell over last 200 events
    cur.execute("""
      SELECT AVG(dwell_time_sec) AS avg_dwell
      FROM (SELECT dwell_time_sec FROM events WHERE dwell_time_sec IS NOT NULL ORDER BY id DESC LIMIT 200)
    """)
    avg_dwell = cur.fetchone()["avg_dwell"] or 0.0

    # exit/exposure counts (last 24h)
    since = now - 24 * 3600
    cur.execute("SELECT COUNT(*) AS n FROM events WHERE received_ts >= ?", (since,))
    exit_count = cur.fetchone()["n"] or 0

    cur.execute("SELECT COUNT(*) AS n FROM events WHERE received_ts >= ? AND dwell_time_sec >= ?",
                (since, EXPOSURE_DWELL_SEC))
    exposure_count = cur.fetchone()["n"] or 0

    # approximate inside_geofence count among latest event per active device
    cur.execute("""
      SELECT mac_hash, MAX(id) AS last_id
      FROM events
      WHERE mac_hash IS NOT NULL
      GROUP BY mac_hash
      HAVING (? - (SELECT received_ts FROM events e2 WHERE e2.id = last_id)) <= ?
    """, (now, ACTIVE_WINDOW_SEC))
    last_ids = [r["last_id"] for r in cur.fetchall()]

    inside_count = 0
    if last_ids:
        q = "SELECT lat, lon FROM events WHERE id IN (%s)" % ",".join(["?"] * len(last_ids))
        cur.execute(q, last_ids)
        for r in cur.fetchall():
            if inside_geofence(r["lat"], r["lon"]):
                inside_count += 1

    con.close()

    return {
        "geofence": {"lat": GEOFENCE_LAT, "lon": GEOFENCE_LON, "radius_m": GEOFENCE_RADIUS_M},
        "total_unique": total_unique,
        "active_devices": active_devices,
        "inside_geofence": inside_count,
        "avg_rssi": float(avg_rssi),
        "avg_dwell": float(avg_dwell),
        "enter_count": 0,  # optional: implement enter logic later
        "exit_count": exit_count,
        "exposure_count": exposure_count,
    }


@app.get("/")
def home():
    return render_template("index.html")


@app.get("/api/stats")
def api_stats():
    return jsonify(compute_stats())


@app.get("/api/events")
def api_events():
    limit = int(request.args.get("limit", "200"))
    con = db()
    cur = con.cursor()
    cur.execute("""
      SELECT id, received_ts, timestamp, mac_hash, rssi, lat, lon, dwell_time_sec,
             campaign_id, activation_name, asset_id, creative_id, timestamp_start_utc, timestamp_end_utc
      FROM events
      ORDER BY id DESC
      LIMIT ?
    """, (limit,))
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return jsonify(rows)


@app.get("/api/devices")
def api_devices():
    con = db()
    cur = con.cursor()
    cur.execute("""
      SELECT e.*
      FROM events e
      JOIN (
        SELECT mac_hash, MAX(id) AS max_id
        FROM events
        WHERE mac_hash IS NOT NULL
        GROUP BY mac_hash
      ) x
      ON e.id = x.max_id
      ORDER BY e.received_ts DESC
      LIMIT 500
    """)
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return jsonify(rows)


@app.get("/api/device/<mac_hash>")
def api_device(mac_hash):
    con = db()
    cur = con.cursor()
    cur.execute("""
      SELECT id, received_ts, timestamp, mac_hash, rssi, lat, lon, dwell_time_sec
      FROM events
      WHERE mac_hash = ?
      ORDER BY id DESC
      LIMIT 200
    """, (mac_hash,))
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return jsonify(rows)


@app.post("/ingest")
def ingest():
    """
    Accept:
      - single event JSON
      - OR {"events":[...]} batch
    """
    payload = request.get_json(force=True, silent=True)
    if payload is None:
        return jsonify({"ok": False, "error": "invalid json"}), 400

    events = payload.get("events") if isinstance(payload, dict) else None
    if events is None:
        events = [payload]

    received_ts = int(time.time())
    con = db()
    cur = con.cursor()

    inserted = 0
    last_event = None

    for ev in events:
        if not isinstance(ev, dict):
            continue
        last_event = ev

        cur.execute("""
          INSERT INTO events (
            received_ts, timestamp, mac_hash, rssi, lat, lon, dwell_time_sec,
            campaign_id, activation_name, asset_id, creative_id, timestamp_start_utc, timestamp_end_utc
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            received_ts,
            ev.get("timestamp"),
            ev.get("mac_hash"),
            ev.get("rssi"),
            ev.get("lat"),
            ev.get("lon"),
            ev.get("dwell_time_sec"),
            str(ev.get("campaign_id")) if ev.get("campaign_id") is not None else None,
            ev.get("activation_name"),
            ev.get("asset_id"),
            ev.get("creative_id"),
            ev.get("timestamp_start_utc"),
            ev.get("timestamp_end_utc"),
        ))
        inserted += 1

    con.commit()
    con.close()

    # Emit live update event (works when the client is connected)
    socketio.emit("ingest", {"count": inserted, "last": last_event, "stats": compute_stats()})

    return jsonify({"ok": True, "inserted": inserted})


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", "5050"))

    # On Flask 3, SocketIO in threading mode uses Werkzeug dev server.
    # allow_unsafe_werkzeug=True suppresses the warning for demo use.
    socketio.run(app, host="0.0.0.0", port=port, allow_unsafe_werkzeug=True)