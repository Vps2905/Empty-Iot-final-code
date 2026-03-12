import os
import time
import sqlite3
import csv
import json
import hashlib
from io import StringIO
from math import radians, sin, cos, sqrt, atan2

from flask import Flask, request, jsonify, Response, send_from_directory
from flask_socketio import SocketIO

DB_PATH = os.environ.get("DB_PATH", "footfall.db")
FIRMWARE_DIR = os.environ.get("FIRMWARE_DIR", "firmware_store")

GEOFENCE_LAT = float(os.environ.get("GEOFENCE_LAT", "17.43388"))
GEOFENCE_LON = float(os.environ.get("GEOFENCE_LON", "78.42669"))
GEOFENCE_RADIUS_M = float(os.environ.get("GEOFENCE_RADIUS_M", "300"))

ACTIVE_WINDOW_SEC = int(os.environ.get("ACTIVE_WINDOW_SEC", "30"))
EXPOSURE_DWELL_SEC = int(os.environ.get("EXPOSURE_DWELL_SEC", "10"))
EXPOSURE_RSSI_MIN = int(os.environ.get("EXPOSURE_RSSI_MIN", "-85"))

DEVICE_TOKEN = os.environ.get("DEVICE_TOKEN", "replace_with_real_device_token")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "replace_with_real_admin_token")

GT_ENABLED = os.environ.get("GT_ENABLED", "false").lower() == "true"
GT_API_URL = os.environ.get("GT_API_URL", "")
GT_API_TOKEN = os.environ.get("GT_API_TOKEN", "")

app = Flask(__name__, static_folder="static", template_folder="templates")
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret")
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

os.makedirs(FIRMWARE_DIR, exist_ok=True)


def db():
    # Write-Ahead Logging (WAL) enabled for safe concurrent writes
    con = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=15.0)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.row_factory = sqlite3.Row
    return con


def column_exists(cur, table_name, column_name):
    cur.execute(f"PRAGMA table_info({table_name})")
    cols = [row[1] for row in cur.fetchall()]
    return column_name in cols


def ensure_column(cur, table_name, column_name, column_def):
    if not column_exists(cur, table_name, column_name):
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")


def ensure_events_received_ts(cur):
    if not column_exists(cur, "events", "received_ts"):
        ensure_column(cur, "events", "received_ts", "INTEGER")
        cur.execute(
            "UPDATE events SET received_ts = COALESCE(timestamp, strftime('%s','now')) "
            "WHERE received_ts IS NULL"
        )


def init_db():
    con = db()
    cur = con.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT UNIQUE,
        schema_version TEXT,
        event_type TEXT,
        received_ts INTEGER NOT NULL,
        timestamp INTEGER,
        timestamp_utc TEXT,
        session_start_epoch INTEGER,
        session_end_epoch INTEGER,
        session_start_utc TEXT,
        session_end_utc TEXT,
        mac_hash TEXT,
        signal_source TEXT,
        rssi INTEGER,
        dwell_time_sec INTEGER,
        gps_fix INTEGER,
        lat REAL,
        lon REAL,
        distance_to_geofence_m REAL,
        inside_geofence INTEGER,
        qualified_exposure INTEGER,
        device_id TEXT,
        asset_id TEXT,
        asset_type TEXT,
        site_id TEXT,
        creative_id TEXT,
        campaign_id TEXT,
        activation_name TEXT,
        uplink_type TEXT,
        fw_version TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS devices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_id TEXT UNIQUE,
        site_id TEXT,
        asset_id TEXT,
        asset_type TEXT,
        fw_version TEXT,
        assigned_fw_version TEXT,
        auth_token TEXT,
        uplink_type TEXT,
        deployment_status TEXT,
        last_seen INTEGER,
        last_heartbeat_ts INTEGER,
        health_status TEXT,
        gps_fix INTEGER,
        lat REAL,
        lon REAL,
        queue_depth INTEGER,
        spool_bytes INTEGER,
        dropped_presence INTEGER,
        dropped_exit INTEGER,
        upload_failures INTEGER,
        wifi_status INTEGER,
        modem_ready INTEGER,
        ota_channel TEXT DEFAULT 'stable',
        ota_status TEXT,
        ota_last_checked INTEGER,
        ota_last_result TEXT,
        ota_last_target_version TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ota_releases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        version TEXT NOT NULL,
        channel TEXT NOT NULL DEFAULT 'stable',
        min_fw_version TEXT,
        binary_filename TEXT NOT NULL,
        binary_sha256 TEXT NOT NULL,
        binary_size INTEGER NOT NULL,
        notes TEXT,
        rollout_percent INTEGER NOT NULL DEFAULT 100,
        force_update INTEGER NOT NULL DEFAULT 0,
        active INTEGER NOT NULL DEFAULT 1,
        created_ts INTEGER NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ota_reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_id TEXT NOT NULL,
        current_fw_version TEXT,
        target_fw_version TEXT,
        status TEXT NOT NULL,
        message TEXT,
        reported_ts INTEGER NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS partner_deliveries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        partner_name TEXT NOT NULL,
        event_id TEXT NOT NULL,
        device_id TEXT,
        payload_json TEXT NOT NULL,
        payload_hash TEXT NOT NULL,
        status TEXT NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        response_code INTEGER,
        response_body TEXT,
        created_ts INTEGER NOT NULL,
        last_attempt_ts INTEGER,
        UNIQUE(partner_name, event_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS partner_delivery_attempts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        partner_delivery_id INTEGER NOT NULL,
        attempt_no INTEGER NOT NULL,
        request_ts INTEGER NOT NULL,
        response_code INTEGER,
        response_body TEXT,
        status TEXT NOT NULL
    )
    """)

    ensure_column(cur, "events", "event_id", "TEXT")
    ensure_column(cur, "events", "schema_version", "TEXT")
    ensure_column(cur, "events", "event_type", "TEXT")
    ensure_events_received_ts(cur)
    ensure_column(cur, "events", "timestamp", "INTEGER")
    ensure_column(cur, "events", "timestamp_utc", "TEXT")
    ensure_column(cur, "events", "session_start_epoch", "INTEGER")
    ensure_column(cur, "events", "session_end_epoch", "INTEGER")
    ensure_column(cur, "events", "session_start_utc", "TEXT")
    ensure_column(cur, "events", "session_end_utc", "TEXT")
    ensure_column(cur, "events", "mac_hash", "TEXT")
    ensure_column(cur, "events", "signal_source", "TEXT")
    ensure_column(cur, "events", "rssi", "INTEGER")
    ensure_column(cur, "events", "dwell_time_sec", "INTEGER")
    ensure_column(cur, "events", "gps_fix", "INTEGER")
    ensure_column(cur, "events", "lat", "REAL")
    ensure_column(cur, "events", "lon", "REAL")
    ensure_column(cur, "events", "distance_to_geofence_m", "REAL")
    ensure_column(cur, "events", "inside_geofence", "INTEGER")
    ensure_column(cur, "events", "qualified_exposure", "INTEGER")
    ensure_column(cur, "events", "device_id", "TEXT")
    ensure_column(cur, "events", "asset_id", "TEXT")
    ensure_column(cur, "events", "asset_type", "TEXT")
    ensure_column(cur, "events", "site_id", "TEXT")
    ensure_column(cur, "events", "creative_id", "TEXT")
    ensure_column(cur, "events", "campaign_id", "TEXT")
    ensure_column(cur, "events", "activation_name", "TEXT")
    ensure_column(cur, "events", "uplink_type", "TEXT")
    ensure_column(cur, "events", "fw_version", "TEXT")

    ensure_column(cur, "devices", "device_id", "TEXT")
    ensure_column(cur, "devices", "site_id", "TEXT")
    ensure_column(cur, "devices", "asset_id", "TEXT")
    ensure_column(cur, "devices", "asset_type", "TEXT")
    ensure_column(cur, "devices", "fw_version", "TEXT")
    ensure_column(cur, "devices", "assigned_fw_version", "TEXT")
    ensure_column(cur, "devices", "auth_token", "TEXT")
    ensure_column(cur, "devices", "uplink_type", "TEXT")
    ensure_column(cur, "devices", "deployment_status", "TEXT")
    ensure_column(cur, "devices", "last_seen", "INTEGER")
    ensure_column(cur, "devices", "last_heartbeat_ts", "INTEGER")
    ensure_column(cur, "devices", "health_status", "TEXT")
    ensure_column(cur, "devices", "gps_fix", "INTEGER")
    ensure_column(cur, "devices", "lat", "REAL")
    ensure_column(cur, "devices", "lon", "REAL")
    ensure_column(cur, "devices", "queue_depth", "INTEGER")
    ensure_column(cur, "devices", "spool_bytes", "INTEGER")
    ensure_column(cur, "devices", "dropped_presence", "INTEGER")
    ensure_column(cur, "devices", "dropped_exit", "INTEGER")
    ensure_column(cur, "devices", "upload_failures", "INTEGER")
    ensure_column(cur, "devices", "wifi_status", "INTEGER")
    ensure_column(cur, "devices", "modem_ready", "INTEGER")
    ensure_column(cur, "devices", "ota_channel", "TEXT DEFAULT 'stable'")
    ensure_column(cur, "devices", "ota_status", "TEXT")
    ensure_column(cur, "devices", "ota_last_checked", "INTEGER")
    ensure_column(cur, "devices", "ota_last_result", "TEXT")
    ensure_column(cur, "devices", "ota_last_target_version", "TEXT")

    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id ON events(event_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_mac_hash ON events(mac_hash)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_received_ts ON events(received_ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_device_id ON events(device_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_qualified ON events(qualified_exposure)")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_devices_device_id ON devices(device_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_devices_last_seen ON devices(last_seen)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ota_releases_active ON ota_releases(active, channel)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_partner_deliveries_status ON partner_deliveries(status, partner_name)")

    con.commit()
    con.close()


def auth_ok(req):
    auth = req.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return False
    return auth.split(" ", 1)[1].strip() == DEVICE_TOKEN


def admin_auth_ok(req):
    auth = req.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return False
    return auth.split(" ", 1)[1].strip() == ADMIN_TOKEN


def haversine_m(lat1, lon1, lat2, lon2):
    r = 6371000.0
    p1 = radians(lat1)
    p2 = radians(lat2)
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(p1) * cos(p2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return r * c


def geofence_distance(lat, lon):
    if lat is None or lon is None:
        return None
    return haversine_m(lat, lon, GEOFENCE_LAT, GEOFENCE_LON)


def inside_geofence(lat, lon):
    d = geofence_distance(lat, lon)
    if d is None:
        return False
    return d <= GEOFENCE_RADIUS_M


def qualifies_exposure(ev):
    gps_fix = bool(ev.get("gps_fix"))
    dwell = ev.get("dwell_time_sec") or 0
    rssi = ev.get("rssi")
    lat = ev.get("lat")
    lon = ev.get("lon")

    if not gps_fix:
        return False
    if not inside_geofence(lat, lon):
        return False
    if dwell < EXPOSURE_DWELL_SEC:
        return False
    if rssi is None:
        return False
    if int(rssi) < EXPOSURE_RSSI_MIN:
        return False
    return True


def normalize_event(ev: dict):
    gps_fix = bool(ev.get("gps_fix", False))
    lat = ev.get("lat")
    lon = ev.get("lon")

    distance = geofence_distance(lat, lon)
    inside = inside_geofence(lat, lon)
    qualified = qualifies_exposure({
        "gps_fix": gps_fix,
        "lat": lat,
        "lon": lon,
        "dwell_time_sec": ev.get("dwell_time_sec"),
        "rssi": ev.get("rssi"),
    })

    return {
        "event_id": ev.get("event_id"),
        "schema_version": ev.get("schema_version"),
        "event_type": ev.get("event_type", "unknown"),
        "timestamp": ev.get("timestamp_epoch"),
        "timestamp_utc": ev.get("timestamp_utc"),
        "session_start_epoch": ev.get("session_start_epoch"),
        "session_end_epoch": ev.get("session_end_epoch"),
        "session_start_utc": ev.get("session_start_utc"),
        "session_end_utc": ev.get("session_end_utc"),
        "mac_hash": ev.get("mac_hash"),
        "signal_source": ev.get("signal_source"),
        "rssi": ev.get("rssi"),
        "dwell_time_sec": ev.get("dwell_time_sec"),
        "gps_fix": 1 if gps_fix else 0,
        "lat": lat,
        "lon": lon,
        "distance_to_geofence_m": distance,
        "inside_geofence": 1 if inside else 0,
        "qualified_exposure": 1 if qualified else 0,
        "device_id": ev.get("device_id"),
        "asset_id": ev.get("asset_id"),
        "asset_type": ev.get("asset_type"),
        "site_id": ev.get("site_id"),
        "creative_id": ev.get("creative_id"),
        "campaign_id": str(ev.get("campaign_id")) if ev.get("campaign_id") is not None else None,
        "activation_name": ev.get("activation_name"),
        "uplink_type": ev.get("uplink_type"),
        "fw_version": ev.get("fw_version"),
    }


def upsert_device_from_event(cur, norm, now_ts):
    if not norm.get("device_id"):
        return

    cur.execute("""
      INSERT INTO devices (
        device_id, site_id, asset_id, asset_type, fw_version, assigned_fw_version,
        auth_token, uplink_type, deployment_status, last_seen, health_status,
        gps_fix, lat, lon
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(device_id) DO UPDATE SET
        site_id=excluded.site_id,
        asset_id=excluded.asset_id,
        asset_type=excluded.asset_type,
        fw_version=excluded.fw_version,
        uplink_type=excluded.uplink_type,
        last_seen=excluded.last_seen,
        gps_fix=excluded.gps_fix,
        lat=excluded.lat,
        lon=excluded.lon
    """, (
        norm.get("device_id"),
        norm.get("site_id"),
        norm.get("asset_id"),
        norm.get("asset_type"),
        norm.get("fw_version"),
        norm.get("fw_version"),
        DEVICE_TOKEN,
        norm.get("uplink_type"),
        "testing",
        now_ts,
        "online",
        norm.get("gps_fix"),
        norm.get("lat"),
        norm.get("lon"),
    ))


def version_tuple(v):
    if not v:
        return (0,)
    parts = []
    for x in str(v).split("."):
        try:
            parts.append(int(x))
        except ValueError:
            parts.append(0)
    return tuple(parts)


def pick_release_for_device(cur, device_id, current_fw, channel="stable"):
    cur.execute("""
      SELECT *
      FROM ota_releases
      WHERE active = 1 AND channel = ?
      ORDER BY created_ts DESC
    """, (channel,))
    rows = cur.fetchall()
    if not rows:
        return None

    current_v = version_tuple(current_fw)
    bucket = int(hashlib.sha256(device_id.encode()).hexdigest(), 16) % 100

    for r in rows:
        target_v = version_tuple(r["version"])
        if target_v <= current_v:
            continue
        min_fw = r["min_fw_version"]
        if min_fw and current_v < version_tuple(min_fw):
            continue
        rollout_percent = int(r["rollout_percent"] or 100)
        if bucket >= rollout_percent:
            continue
        return r
    return None


def build_groundtruth_payload(event_row: dict):
    return {
        "event_id": event_row["event_id"],
        "event_type": event_row["event_type"],
        "timestamp_epoch": event_row["timestamp"],
        "timestamp_utc": event_row["timestamp_utc"],
        "session_start_epoch": event_row["session_start_epoch"],
        "session_end_epoch": event_row["session_end_epoch"],
        "mac_hash": event_row["mac_hash"],
        "signal_source": event_row["signal_source"],
        "rssi": event_row["rssi"],
        "dwell_time_sec": event_row["dwell_time_sec"],
        "gps_fix": bool(event_row["gps_fix"]),
        "lat": event_row["lat"],
        "lon": event_row["lon"],
        "distance_to_geofence_m": event_row["distance_to_geofence_m"],
        "inside_geofence": bool(event_row["inside_geofence"]),
        "qualified_exposure": bool(event_row["qualified_exposure"]),
        "device_id": event_row["device_id"],
        "site_id": event_row["site_id"],
        "asset_id": event_row["asset_id"],
        "asset_type": event_row["asset_type"],
        "creative_id": event_row["creative_id"],
        "campaign_id": event_row["campaign_id"],
        "activation_name": event_row["activation_name"],
        "fw_version": event_row["fw_version"],
        "uplink_type": event_row["uplink_type"],
    }


def enqueue_groundtruth_delivery(cur, event_row: dict):
    payload = build_groundtruth_payload(event_row)
    payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    payload_hash = hashlib.sha256(payload_json.encode()).hexdigest()

    try:
        cur.execute("""
          INSERT INTO partner_deliveries (
            partner_name, event_id, device_id, payload_json, payload_hash,
            status, created_ts
          ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            "groundtruth",
            event_row["event_id"],
            event_row["device_id"],
            payload_json,
            payload_hash,
            "queued",
            int(time.time())
        ))
    except sqlite3.IntegrityError:
        pass


def compute_stats():
    con = db()
    cur = con.cursor()
    now = int(time.time())

    cur.execute("SELECT COUNT(DISTINCT mac_hash) AS n FROM events WHERE mac_hash IS NOT NULL")
    total_unique = cur.fetchone()["n"] or 0

    cur.execute("""
      SELECT mac_hash, MAX(received_ts) AS last_seen, AVG(rssi) AS avg_rssi
      FROM events
      WHERE mac_hash IS NOT NULL
      GROUP BY mac_hash
      HAVING (? - last_seen) <= ?
    """, (now, ACTIVE_WINDOW_SEC))
    active_rows = cur.fetchall()
    active_devices = len(active_rows)

    vals = [r["avg_rssi"] for r in active_rows if r["avg_rssi"] is not None]
    avg_rssi = (sum(vals) / len(vals)) if vals else 0.0

    cur.execute("""
      SELECT AVG(dwell_time_sec) AS avg_dwell
      FROM (
        SELECT dwell_time_sec FROM events
        WHERE dwell_time_sec IS NOT NULL
        ORDER BY id DESC
        LIMIT 200
      )
    """)
    avg_dwell = cur.fetchone()["avg_dwell"] or 0.0

    since = now - 24 * 3600
    cur.execute("SELECT COUNT(*) AS n FROM events WHERE received_ts >= ?", (since,))
    exit_count = cur.fetchone()["n"] or 0

    cur.execute("""
      SELECT COUNT(*) AS n
      FROM events
      WHERE received_ts >= ? AND qualified_exposure = 1
    """, (since,))
    exposure_count = cur.fetchone()["n"] or 0

    cur.execute("""
      SELECT COUNT(*) AS n
      FROM (
        SELECT e.*
        FROM events e
        JOIN (
          SELECT mac_hash, MAX(id) AS max_id
          FROM events
          WHERE mac_hash IS NOT NULL
          GROUP BY mac_hash
        ) x
        ON e.id = x.max_id
        WHERE (? - e.received_ts) <= ?
          AND e.inside_geofence = 1
      )
    """, (now, ACTIVE_WINDOW_SEC))
    inside_count = cur.fetchone()["n"] or 0

    con.close()

    return {
        "geofence": {
            "lat": GEOFENCE_LAT,
            "lon": GEOFENCE_LON,
            "radius_m": GEOFENCE_RADIUS_M
        },
        "total_unique": total_unique,
        "active_devices": active_devices,
        "inside_geofence": inside_count,
        "avg_rssi": float(avg_rssi),
        "avg_dwell": float(avg_dwell),
        "enter_count": 0,
        "exit_count": exit_count,
        "exposure_count": exposure_count,
    }


@app.get("/")
def home():
    return send_from_directory("templates", "index.html")


@app.get("/api/stats")
def api_stats():
    return jsonify(compute_stats())


@app.get("/api/events")
def api_events():
    limit = int(request.args.get("limit", "200"))
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,))
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
      SELECT *
      FROM events
      WHERE mac_hash = ?
      ORDER BY id DESC
      LIMIT 200
    """, (mac_hash,))
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return jsonify(rows)


@app.get("/api/partner/groundtruth/queue")
def api_gt_queue():
    con = db()
    cur = con.cursor()
    cur.execute("""
      SELECT id, partner_name, event_id, device_id, status, attempt_count,
             response_code, created_ts, last_attempt_ts
      FROM partner_deliveries
      WHERE partner_name = 'groundtruth'
      ORDER BY id DESC
      LIMIT 500
    """)
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return jsonify(rows)


@app.get("/export.csv")
def export_csv():
    limit = int(request.args.get("limit", "100000"))
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM events ORDER BY id ASC LIMIT ?", (limit,))
    rows = cur.fetchall()
    con.close()

    if not rows:
        return Response("", mimetype="text/csv")

    header = rows[0].keys()
    out = StringIO()
    w = csv.writer(out)
    w.writerow(header)
    for r in rows:
        w.writerow([r[h] for h in header])

    filename = f"events_{int(time.time())}.csv"
    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.get("/export.jsonl")
def export_jsonl():
    limit = int(request.args.get("limit", "100000"))
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM events ORDER BY id ASC LIMIT ?", (limit,))
    rows = cur.fetchall()
    con.close()

    def gen():
        for r in rows:
            yield json.dumps(dict(r), ensure_ascii=False) + "\n"

    filename = f"events_{int(time.time())}.jsonl"
    return Response(
        gen(),
        mimetype="application/x-ndjson",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.post("/ingest")
def ingest():
    if not auth_ok(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

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
    duplicates = 0
    last_event = None

    for ev in events:
        if not isinstance(ev, dict):
            continue

        norm = normalize_event(ev)
        last_event = norm

        if not norm["event_id"] or not norm["mac_hash"] or not norm["device_id"]:
            continue

        try:
            cur.execute("""
              INSERT INTO events (
                event_id, schema_version, event_type,
                received_ts, timestamp, timestamp_utc,
                session_start_epoch, session_end_epoch, session_start_utc, session_end_utc,
                mac_hash, signal_source, rssi, dwell_time_sec,
                gps_fix, lat, lon,
                distance_to_geofence_m, inside_geofence, qualified_exposure,
                device_id, asset_id, asset_type, site_id, creative_id,
                campaign_id, activation_name, uplink_type, fw_version
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                norm["event_id"], norm["schema_version"], norm["event_type"],
                received_ts, norm["timestamp"], norm["timestamp_utc"],
                norm["session_start_epoch"], norm["session_end_epoch"], norm["session_start_utc"], norm["session_end_utc"],
                norm["mac_hash"], norm["signal_source"], norm["rssi"], norm["dwell_time_sec"],
                norm["gps_fix"], norm["lat"], norm["lon"],
                norm["distance_to_geofence_m"], norm["inside_geofence"], norm["qualified_exposure"],
                norm["device_id"], norm["asset_id"], norm["asset_type"], norm["site_id"], norm["creative_id"],
                norm["campaign_id"], norm["activation_name"], norm["uplink_type"], norm["fw_version"],
            ))
            inserted += 1
            upsert_device_from_event(cur, norm, received_ts)

            if norm["qualified_exposure"] == 1:
                enqueue_groundtruth_delivery(cur, norm)

        except sqlite3.IntegrityError:
            duplicates += 1

    con.commit()
    con.close()

    socketio.emit("ingest", {
        "count": inserted,
        "duplicates": duplicates,
        "last": last_event,
        "stats": compute_stats()
    })

    return jsonify({"ok": True, "inserted": inserted, "duplicates": duplicates})


@app.post("/heartbeat")
def heartbeat():
    if not auth_ok(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(force=True, silent=True)
    if payload is None:
        return jsonify({"ok": False, "error": "invalid json"}), 400

    device_id = payload.get("device_id")
    if not device_id:
        return jsonify({"ok": False, "error": "device_id required"}), 400

    now_ts = int(time.time())
    con = db()
    cur = con.cursor()

    cur.execute("""
      INSERT INTO devices (
        device_id, site_id, asset_id, asset_type, fw_version, assigned_fw_version,
        auth_token, uplink_type, deployment_status, last_seen, last_heartbeat_ts,
        health_status, gps_fix, lat, lon, queue_depth, spool_bytes,
        dropped_presence, dropped_exit, upload_failures, wifi_status, modem_ready, ota_channel
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(device_id) DO UPDATE SET
        site_id=excluded.site_id,
        asset_id=excluded.asset_id,
        asset_type=excluded.asset_type,
        fw_version=excluded.fw_version,
        uplink_type=excluded.uplink_type,
        last_seen=excluded.last_seen,
        last_heartbeat_ts=excluded.last_heartbeat_ts,
        health_status=excluded.health_status,
        gps_fix=excluded.gps_fix,
        lat=excluded.lat,
        lon=excluded.lon,
        queue_depth=excluded.queue_depth,
        spool_bytes=excluded.spool_bytes,
        dropped_presence=excluded.dropped_presence,
        dropped_exit=excluded.dropped_exit,
        upload_failures=excluded.upload_failures,
        wifi_status=excluded.wifi_status,
        modem_ready=excluded.modem_ready,
        ota_channel=excluded.ota_channel
    """, (
        device_id,
        payload.get("site_id"),
        payload.get("asset_id"),
        payload.get("asset_type"),
        payload.get("fw_version"),
        payload.get("fw_version"),
        DEVICE_TOKEN,
        payload.get("uplink_type"),
        "testing",
        now_ts,
        now_ts,
        "online",
        1 if payload.get("gps_fix") else 0,
        payload.get("lat"),
        payload.get("lon"),
        payload.get("queue_depth"),
        payload.get("spool_bytes"),
        payload.get("dropped_presence"),
        payload.get("dropped_exit"),
        payload.get("upload_failures"),
        payload.get("wifi_status"),
        1 if payload.get("modem_ready") else 0,
        payload.get("ota_channel", "stable"),
    ))

    con.commit()
    con.close()

    socketio.emit("heartbeat", {"device_id": device_id, "ts": now_ts, "stats": compute_stats()})
    return jsonify({"ok": True, "device_id": device_id})


@app.post("/ota/check")
def ota_check():
    if not auth_ok(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(force=True, silent=True)
    if payload is None:
        return jsonify({"ok": False, "error": "invalid json"}), 400

    device_id = payload.get("device_id")
    current_fw = payload.get("fw_version")
    channel = payload.get("ota_channel", "stable")

    if not device_id or not current_fw:
        return jsonify({"ok": False, "error": "device_id and fw_version required"}), 400

    now_ts = int(time.time())
    con = db()
    cur = con.cursor()

    cur.execute("""
      INSERT INTO devices (device_id, fw_version, assigned_fw_version, auth_token, ota_channel, ota_last_checked)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(device_id) DO UPDATE SET
        fw_version=excluded.fw_version,
        ota_channel=excluded.ota_channel,
        ota_last_checked=excluded.ota_last_checked
    """, (device_id, current_fw, current_fw, DEVICE_TOKEN, channel, now_ts))

    rel = pick_release_for_device(cur, device_id, current_fw, channel)

    if rel is None:
        cur.execute("""
          UPDATE devices
          SET ota_last_result = ?, ota_last_target_version = ?
          WHERE device_id = ?
        """, ("up_to_date", current_fw, device_id))
        con.commit()
        con.close()
        return jsonify({
            "ok": True,
            "update_available": False,
            "device_id": device_id,
            "current_version": current_fw
        })

    cur.execute("""
      UPDATE devices
      SET assigned_fw_version = ?, ota_last_result = ?, ota_last_target_version = ?
      WHERE device_id = ?
    """, (rel["version"], "update_available", rel["version"], device_id))

    con.commit()
    con.close()

    return jsonify({
        "ok": True,
        "update_available": True,
        "device_id": device_id,
        "current_version": current_fw,
        "target_version": rel["version"],
        "force_update": bool(rel["force_update"]),
        "binary_sha256": rel["binary_sha256"],
        "binary_size": rel["binary_size"],
        "notes": rel["notes"] or "",
        "download_url": f"/firmware/{rel['binary_filename']}"
    })


@app.post("/ota/report")
def ota_report():
    if not auth_ok(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(force=True, silent=True)
    if payload is None:
        return jsonify({"ok": False, "error": "invalid json"}), 400

    device_id = payload.get("device_id")
    current_fw = payload.get("current_fw_version")
    target_fw = payload.get("target_fw_version")
    status = payload.get("status")
    message = payload.get("message", "")

    if not device_id or not status:
        return jsonify({"ok": False, "error": "device_id and status required"}), 400

    now_ts = int(time.time())
    con = db()
    cur = con.cursor()

    cur.execute("""
      INSERT INTO ota_reports (
        device_id, current_fw_version, target_fw_version, status, message, reported_ts
      ) VALUES (?, ?, ?, ?, ?, ?)
    """, (device_id, current_fw, target_fw, status, message, now_ts))

    cur.execute("""
      UPDATE devices
      SET ota_status = ?, ota_last_result = ?, assigned_fw_version = COALESCE(?, assigned_fw_version)
      WHERE device_id = ?
    """, (status, status, target_fw, device_id))

    con.commit()
    con.close()
    return jsonify({"ok": True})


@app.post("/admin/ota/release")
def admin_ota_release():
    if not admin_auth_ok(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(force=True, silent=True)
    if payload is None:
        return jsonify({"ok": False, "error": "invalid json"}), 400

    version = payload.get("version")
    channel = payload.get("channel", "stable")
    binary_filename = payload.get("binary_filename")
    notes = payload.get("notes", "")
    rollout_percent = int(payload.get("rollout_percent", 100))
    force_update = 1 if payload.get("force_update") else 0
    min_fw_version = payload.get("min_fw_version")

    if not version or not binary_filename:
        return jsonify({"ok": False, "error": "version and binary_filename required"}), 400

    full_path = os.path.join(FIRMWARE_DIR, binary_filename)
    if not os.path.exists(full_path):
        return jsonify({"ok": False, "error": "firmware file not found"}), 404

    with open(full_path, "rb") as f:
        data = f.read()

    sha256 = hashlib.sha256(data).hexdigest()
    size = len(data)
    now_ts = int(time.time())

    con = db()
    cur = con.cursor()

    cur.execute("""
      INSERT INTO ota_releases (
        version, channel, min_fw_version, binary_filename, binary_sha256,
        binary_size, notes, rollout_percent, force_update, active, created_ts
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
    """, (
        version, channel, min_fw_version, binary_filename, sha256,
        size, notes, rollout_percent, force_update, now_ts
    ))

    con.commit()
    con.close()

    return jsonify({
        "ok": True,
        "version": version,
        "channel": channel,
        "binary_filename": binary_filename,
        "binary_sha256": sha256,
        "binary_size": size
    })


@app.post("/admin/partner/groundtruth/flush")
def admin_gt_flush():
    if not admin_auth_ok(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    con = db()
    cur = con.cursor()

    cur.execute("""
      SELECT * FROM partner_deliveries
      WHERE partner_name = 'groundtruth' AND status IN ('queued', 'retry')
      ORDER BY id ASC
      LIMIT 500
    """)
    rows = cur.fetchall()

    sent = 0
    failed = 0

    for r in rows:
        pid = r["id"]
        attempt_no = (r["attempt_count"] or 0) + 1
        now_ts = int(time.time())

        if not GT_ENABLED or not GT_API_URL:
            status = "simulated_sent"
            response_code = 200
            response_body = "GT disabled; simulated success"
        else:
            status = "queued_external"
            response_code = 202
            response_body = "placeholder external delivery"

        cur.execute("""
          UPDATE partner_deliveries
          SET status = ?, attempt_count = ?, response_code = ?, response_body = ?, last_attempt_ts = ?
          WHERE id = ?
        """, (status, attempt_no, response_code, response_body, now_ts, pid))

        cur.execute("""
          INSERT INTO partner_delivery_attempts (
            partner_delivery_id, attempt_no, request_ts, response_code, response_body, status
          ) VALUES (?, ?, ?, ?, ?, ?)
        """, (pid, attempt_no, now_ts, response_code, response_body, status))

        if status in ("simulated_sent", "queued_external"):
            sent += 1
        else:
            failed += 1

    con.commit()
    con.close()

    return jsonify({"ok": True, "processed": len(rows), "sent": sent, "failed": failed})


@app.get("/firmware/<path:filename>")
def firmware_download(filename):
    return send_from_directory(FIRMWARE_DIR, filename, as_attachment=True)


init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5050"))
    socketio.run(app, host="0.0.0.0", port=port)