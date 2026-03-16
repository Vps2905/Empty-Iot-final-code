"""
Footfall Intelligence Platform — Integrated Backend (PostgreSQL)
================================================================
Database: PostgreSQL via DATABASE_URL (Railway-provided)
"""

import csv
import io
import os
import time
import uuid
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import requests as http_requests
from flask import Flask, Response, jsonify, render_template, request

# --- Configuration ---
DATABASE_URL = os.environ.get("DATABASE_URL", "")
INGEST_API_KEY = os.environ.get("INGEST_API_KEY", "vps_2004")
ADMIN_API_KEY = os.environ.get("ADMIN_API_KEY", "change_me_admin_key")
SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-change-me")
GEOFENCE_LAT = float(os.environ.get("GEOFENCE_LAT", "17.43388"))
GEOFENCE_LON = float(os.environ.get("GEOFENCE_LON", "78.42669"))
GEOFENCE_RADIUS_M = float(os.environ.get("GEOFENCE_RADIUS_M", "300"))

FSQ_API_KEY = os.environ.get("FSQ_API_KEY", "").strip()
FSQ_RADIUS = int(os.environ.get("FSQ_RADIUS", "200"))
FSQ_LIMIT = int(os.environ.get("FSQ_LIMIT", "5"))
FSQ_SEARCH_URL = "https://api.foursquare.com/v3/places/search"

AQ_MIN_CONFIDENCE = float(os.environ.get("AQ_MIN_CONFIDENCE", "0.50"))
AQ_MIN_DWELL = float(os.environ.get("AQ_MIN_DWELL", "5"))
AQ_MIN_RSSI = float(os.environ.get("AQ_MIN_RSSI", "-85"))

app = Flask(__name__)
app.secret_key = SECRET_KEY
DB_INIT_ERROR = None


# --- Database helpers (PostgreSQL) ---
def get_db():
    if not DATABASE_URL.strip():
        raise RuntimeError(
            "DATABASE_URL is not set. Configure Railway with "
            "DATABASE_URL=${{Postgres.DATABASE_URL}}."
        )
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    return conn

def db_fetchone(query, params=None):
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or ())
            return cur.fetchone()

def db_fetchall(query, params=None):
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or ())
            return cur.fetchall()

def db_execute(query, params=None):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or ())
            conn.commit()
            return cur
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def safe_int(value, default=0):
    try: return int(value)
    except (TypeError, ValueError): return default

def safe_float(value, default=0.0):
    try: return float(value)
    except (TypeError, ValueError): return default


def init_db():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS device_events (
                    id SERIAL PRIMARY KEY,
                    device_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    count_value INTEGER NOT NULL DEFAULT 1,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    rssi INTEGER,
                    firmware_version TEXT,
                    timestamp_epoch BIGINT,
                    mac_hash TEXT,
                    campaign_id TEXT,
                    asset_id TEXT,
                    creative_id TEXT,
                    activation_name TEXT,
                    dwell_time_sec INTEGER NOT NULL DEFAULT 0,
                    created_at BIGINT NOT NULL
                )""")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ingest_logs (
                    id SERIAL PRIMARY KEY,
                    device_id TEXT,
                    status TEXT NOT NULL,
                    detail TEXT,
                    created_at BIGINT NOT NULL
                )""")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS audience_candidates (
                    id SERIAL PRIMARY KEY,
                    source_event_id INTEGER,
                    mac_hash TEXT NOT NULL,
                    device_id TEXT,
                    campaign_id TEXT,
                    asset_id TEXT,
                    creative_id TEXT,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    rssi INTEGER,
                    dwell_time_sec INTEGER DEFAULT 0,
                    confidence_score DOUBLE PRECISION DEFAULT 0.0,
                    repeat_count INTEGER DEFAULT 1,
                    audience_status TEXT DEFAULT 'qualified',
                    created_at BIGINT NOT NULL
                )""")
            cur.execute("""
                DO $$ BEGIN
                    ALTER TABLE audience_candidates
                        ADD CONSTRAINT uq_aud_mac_camp_evt
                        UNIQUE (mac_hash, campaign_id, source_event_id);
                EXCEPTION WHEN duplicate_table THEN NULL;
                         WHEN duplicate_object THEN NULL;
                END $$""")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS foursquare_enriched (
                    id SERIAL PRIMARY KEY,
                    audience_candidate_id INTEGER,
                    mac_hash TEXT,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    place_name TEXT,
                    place_id TEXT,
                    place_address TEXT,
                    place_categories TEXT,
                    distance_m INTEGER,
                    created_at BIGINT NOT NULL
                )""")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS partner_exports (
                    id SERIAL PRIMARY KEY,
                    partner TEXT NOT NULL,
                    audience_candidate_id INTEGER,
                    mac_hash TEXT,
                    campaign_id TEXT,
                    creative_id TEXT,
                    asset_id TEXT,
                    segment_id TEXT,
                    audience_id TEXT,
                    match_type TEXT,
                    match_confidence DOUBLE PRECISION,
                    status TEXT DEFAULT 'accepted',
                    created_at BIGINT NOT NULL
                )""")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mock_impressions (
                    id SERIAL PRIMARY KEY,
                    impression_id TEXT UNIQUE,
                    device_hash TEXT,
                    campaign_id TEXT,
                    creative_id TEXT,
                    ad_title TEXT,
                    source_partner TEXT,
                    served_at BIGINT NOT NULL
                )""")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_de_mac ON device_events (mac_hash)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_de_created ON device_events (created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_ac_mac ON audience_candidates (mac_hash)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pe_mac ON partner_exports (mac_hash)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pe_partner ON partner_exports (partner)")
        conn.commit()
        print("[DB] PostgreSQL tables initialized")
    except Exception as e:
        conn.rollback()
        print(f"[DB] Init error: {e}")
        raise
    finally:
        conn.close()

try:
    init_db()
except Exception as exc:
    DB_INIT_ERROR = str(exc)
    print(f"[DB] Startup warning: {DB_INIT_ERROR}")


# --- Helpers ---
def utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def bearer_token():
    header = request.headers.get("Authorization", "")
    if header.startswith("Bearer "):
        return header.split(" ", 1)[1].strip()
    return None

def admin_authorized():
    token = bearer_token()
    query_token = request.args.get("admin_key")
    return token == ADMIN_API_KEY or query_token == ADMIN_API_KEY

def compute_confidence(rssi, dwell, repeat_count):
    rssi_score = max(0.0, min(1.0, (rssi + 100) / 40.0))
    dwell_score = min(1.0, dwell / 30.0)
    repeat_score = min(1.0, repeat_count / 5.0)
    return round(0.4 * rssi_score + 0.35 * dwell_score + 0.25 * repeat_score, 3)


# ============================================================
#  CORE ROUTES
# ============================================================
@app.get("/")
def home():
    return jsonify({"ok": True, "service": "footfall-platform", "dashboard": "/dashboard"})

@app.get("/dashboard")
def dashboard():
    return render_template("dashboard.html")

@app.get("/api/health")
def health():
    if DB_INIT_ERROR:
        return jsonify({
            "ok": False,
            "service": "footfall-platform",
            "database": "unavailable",
            "database_configured": bool(DATABASE_URL.strip()),
            "error": DB_INIT_ERROR,
            "timestamp": int(time.time()),
        }), 503

    try:
        ev = db_fetchone("SELECT COUNT(*) AS c FROM device_events")["c"]
        au = db_fetchone("SELECT COUNT(*) AS c FROM audience_candidates")["c"]
        ex = db_fetchone("SELECT COUNT(*) AS c FROM partner_exports")["c"]
        im = db_fetchone("SELECT COUNT(*) AS c FROM mock_impressions")["c"]
        return jsonify({
            "ok": True, "service": "footfall-platform", "database": "ok",
            "events": ev, "audience_candidates": au,
            "partner_exports": ex, "mock_impressions": im,
            "foursquare_configured": bool(FSQ_API_KEY),
            "geofence": {"lat": GEOFENCE_LAT, "lon": GEOFENCE_LON, "radius_m": GEOFENCE_RADIUS_M},
            "timestamp": int(time.time()),
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.post("/ingest")
@app.post("/api/ingest")
def ingest():
    token = bearer_token()
    if token != INGEST_API_KEY:
        db_execute(
            "INSERT INTO ingest_logs(device_id, status, detail, created_at) VALUES (%s,%s,%s,%s)",
            (None, "rejected", "invalid ingest key", int(time.time())),
        )
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    device_id = str(payload.get("device_id", "unknown-device"))
    firmware_version = str(payload.get("firmware_version", "unknown"))
    events = payload.get("events") or []
    if not isinstance(events, list):
        return jsonify({"ok": False, "error": "events must be a list"}), 400

    now = int(time.time())
    inserted = 0
    qualified = 0

    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            for evt in events:
                if not isinstance(evt, dict):
                    continue
                timestamp_epoch = safe_int(evt.get("timestamp", now), now)
                cur.execute("""
                    INSERT INTO device_events
                    (device_id, event_type, count_value, latitude, longitude,
                     rssi, firmware_version, timestamp_epoch, mac_hash,
                     campaign_id, asset_id, creative_id, activation_name,
                     dwell_time_sec, created_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id""",
                    (device_id, str(evt.get("event_type", "presence")),
                     safe_int(evt.get("count", 1), 1),
                     evt.get("lat"), evt.get("lon"),
                     safe_int(evt.get("rssi")), firmware_version,
                     timestamp_epoch, evt.get("mac_hash"),
                     str(evt.get("campaign_id", payload.get("campaign_id", ""))) or None,
                     str(evt.get("asset_id", payload.get("asset_id", ""))) or None,
                     str(evt.get("creative_id", payload.get("creative_id", ""))) or None,
                     str(evt.get("activation_name", payload.get("activation_name", ""))) or None,
                     safe_int(evt.get("dwell_time_sec")), now))
                event_id = cur.fetchone()["id"]
                inserted += 1

                # -- Auto-qualify --
                mac_hash = evt.get("mac_hash")
                rssi = safe_int(evt.get("rssi"), -100)
                dwell = safe_int(evt.get("dwell_time_sec"), 0)
                lat = evt.get("lat"); lon = evt.get("lon")
                campaign_id = str(evt.get("campaign_id", "")) or None

                if mac_hash and lat and lon and lat != 0 and lon != 0:
                    cur.execute("SELECT COUNT(*) AS c FROM device_events WHERE mac_hash = %s", (mac_hash,))
                    repeat_count = cur.fetchone()["c"] or 1
                    confidence = compute_confidence(rssi, dwell, repeat_count)

                    if confidence >= AQ_MIN_CONFIDENCE and dwell >= AQ_MIN_DWELL and rssi >= AQ_MIN_RSSI:
                        try:
                            cur.execute("""
                                INSERT INTO audience_candidates
                                (source_event_id, mac_hash, device_id, campaign_id,
                                 asset_id, creative_id, latitude, longitude, rssi,
                                 dwell_time_sec, confidence_score, repeat_count,
                                 audience_status, created_at)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'qualified',%s)
                                ON CONFLICT (mac_hash, campaign_id, source_event_id) DO NOTHING""",
                                (event_id, mac_hash, device_id, campaign_id,
                                 str(evt.get("asset_id", "")) or None,
                                 str(evt.get("creative_id", "")) or None,
                                 lat, lon, rssi, dwell, confidence, repeat_count, now))
                            if cur.rowcount > 0:
                                qualified += 1
                        except Exception:
                            pass

            cur.execute(
                "INSERT INTO ingest_logs(device_id, status, detail, created_at) VALUES (%s,%s,%s,%s)",
                (device_id, "accepted", f"inserted={inserted} qualified={qualified}", now))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return jsonify({"ok": True, "inserted": inserted, "qualified": qualified, "device_id": device_id})


@app.get("/api/events")
def events():
    limit = min(max(safe_int(request.args.get("limit", 100), 100), 1), 1000)
    rows = db_fetchall("""
        SELECT id, device_id, event_type, count_value, latitude, longitude,
               rssi, firmware_version, timestamp_epoch, created_at,
               mac_hash, campaign_id, asset_id, creative_id,
               activation_name, dwell_time_sec
        FROM device_events ORDER BY id DESC LIMIT %s""", (limit,))
    return jsonify([{
        "id": r["id"], "device_id": r["device_id"], "event_type": r["event_type"],
        "count": r["count_value"], "lat": r["latitude"], "lon": r["longitude"],
        "rssi": r["rssi"], "firmware_version": r["firmware_version"],
        "timestamp": r["timestamp_epoch"] or r["created_at"],
        "created_at": r["created_at"], "mac_hash": r["mac_hash"],
        "campaign_id": r["campaign_id"], "asset_id": r["asset_id"],
        "creative_id": r["creative_id"], "activation_name": r["activation_name"],
        "dwell_time_sec": r["dwell_time_sec"],
    } for r in rows])


@app.get("/api/stats")
def stats():
    te = db_fetchone("SELECT COUNT(*) AS c FROM device_events")["c"]
    tc = db_fetchone("SELECT COALESCE(SUM(count_value),0) AS s FROM device_events")["s"]
    ud = db_fetchone("SELECT COUNT(DISTINCT device_id) AS c FROM device_events")["c"]
    um = db_fetchone("SELECT COUNT(DISTINCT mac_hash) AS c FROM device_events WHERE mac_hash IS NOT NULL AND mac_hash != ''")["c"]
    td = db_fetchone("SELECT COALESCE(SUM(dwell_time_sec),0) AS s FROM device_events")["s"]
    ac = db_fetchone("SELECT COUNT(*) AS c FROM audience_candidates")["c"]
    pc = db_fetchone("SELECT COUNT(*) AS c FROM partner_exports")["c"]
    ic = db_fetchone("SELECT COUNT(*) AS c FROM mock_impressions")["c"]
    latest = db_fetchone("""
        SELECT device_id, event_type, count_value, timestamp_epoch, created_at,
               mac_hash, campaign_id, asset_id, creative_id, activation_name, dwell_time_sec
        FROM device_events ORDER BY id DESC LIMIT 1""")
    return jsonify({
        "ok": True, "total_events": int(te), "total_count": int(tc),
        "unique_devices": int(ud), "unique_mac_hashes": int(um),
        "total_dwell_time_sec": int(td),
        "audience_candidates": int(ac), "partner_exports": int(pc),
        "mock_impressions": int(ic),
        "latest_event": None if latest is None else {
            "device_id": latest["device_id"], "event_type": latest["event_type"],
            "count": latest["count_value"],
            "timestamp": latest["timestamp_epoch"] or latest["created_at"],
            "created_at": latest["created_at"], "mac_hash": latest["mac_hash"],
            "campaign_id": latest["campaign_id"], "asset_id": latest["asset_id"],
            "creative_id": latest["creative_id"], "activation_name": latest["activation_name"],
            "dwell_time_sec": latest["dwell_time_sec"],
        },
    })


@app.get("/api/export.csv")
def export_csv():
    if not admin_authorized():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    rows = db_fetchall("""
        SELECT id, device_id, event_type, count_value, latitude, longitude,
               rssi, firmware_version, timestamp_epoch, created_at,
               mac_hash, campaign_id, asset_id, creative_id,
               activation_name, dwell_time_sec
        FROM device_events ORDER BY id DESC""")
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id","device_id","event_type","count","lat","lon","rssi",
                     "firmware_version","timestamp","created_at","mac_hash",
                     "campaign_id","asset_id","creative_id","activation_name","dwell_time_sec"])
    for r in rows:
        writer.writerow([r["id"],r["device_id"],r["event_type"],r["count_value"],
                         r["latitude"],r["longitude"],r["rssi"],r["firmware_version"],
                         r["timestamp_epoch"],r["created_at"],r["mac_hash"],
                         r["campaign_id"],r["asset_id"],r["creative_id"],
                         r["activation_name"],r["dwell_time_sec"]])
    return Response(output.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=footfall_events.csv"})


# ============================================================
#  AUDIENCE BUILDER
# ============================================================
@app.get("/api/audience")
def api_audience():
    limit = min(max(safe_int(request.args.get("limit", 200), 200), 1), 1000)
    rows = db_fetchall("""
        SELECT id, source_event_id, mac_hash, device_id, campaign_id,
               asset_id, creative_id, latitude, longitude, rssi,
               dwell_time_sec, confidence_score, repeat_count,
               audience_status, created_at
        FROM audience_candidates ORDER BY id DESC LIMIT %s""", (limit,))
    return jsonify([dict(r) for r in rows])

@app.get("/api/audience/stats")
def api_audience_stats():
    total = db_fetchone("SELECT COUNT(*) AS c FROM audience_candidates")["c"]
    avg_c = db_fetchone("SELECT COALESCE(AVG(confidence_score),0) AS a FROM audience_candidates")["a"]
    avg_d = db_fetchone("SELECT COALESCE(AVG(dwell_time_sec),0) AS a FROM audience_candidates")["a"]
    by_camp = db_fetchall(
        "SELECT campaign_id, COUNT(*) AS c FROM audience_candidates GROUP BY campaign_id ORDER BY c DESC LIMIT 10")
    return jsonify({
        "ok": True, "total": total,
        "avg_confidence": round(float(avg_c), 3),
        "avg_dwell_sec": round(float(avg_d), 1),
        "by_campaign": [{"campaign_id": r["campaign_id"], "count": r["c"]} for r in by_camp],
    })


# ============================================================
#  FOURSQUARE (real API)
# ============================================================
def fsq_search(lat, lon):
    headers = {"Accept": "application/json", "Authorization": FSQ_API_KEY}
    params = {"ll": f"{lat},{lon}", "radius": FSQ_RADIUS, "limit": FSQ_LIMIT}
    resp = http_requests.get(FSQ_SEARCH_URL, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()

@app.post("/api/partners/foursquare/enrich")
def foursquare_enrich():
    if not FSQ_API_KEY:
        return jsonify({"ok": False, "error": "FSQ_API_KEY not configured"}), 500
    limit = safe_int(request.args.get("limit", 20), 20)
    now = int(time.time())
    enriched = 0; errors_list = []
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT ac.id, ac.mac_hash, ac.latitude, ac.longitude
                FROM audience_candidates ac
                LEFT JOIN foursquare_enriched fe ON fe.audience_candidate_id = ac.id
                WHERE fe.id IS NULL AND ac.latitude IS NOT NULL AND ac.longitude IS NOT NULL
                ORDER BY ac.id DESC LIMIT %s""", (limit,))
            candidates = cur.fetchall()
            for cand in candidates:
                try:
                    data = fsq_search(cand["latitude"], cand["longitude"])
                    for p in data.get("results", []):
                        cats = ", ".join(c.get("name","") for c in p.get("categories",[]))
                        cur.execute("""
                            INSERT INTO foursquare_enriched
                            (audience_candidate_id, mac_hash, latitude, longitude,
                             place_name, place_id, place_address, place_categories,
                             distance_m, created_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            (cand["id"], cand["mac_hash"], cand["latitude"], cand["longitude"],
                             p.get("name"), p.get("fsq_place_id"),
                             p.get("location",{}).get("formatted_address"),
                             cats, p.get("distance"), now))
                    enriched += 1
                except Exception as e:
                    errors_list.append({"candidate_id": cand["id"], "error": str(e)})
        conn.commit()
    except Exception: conn.rollback(); raise
    finally: conn.close()
    return jsonify({"ok": True, "enriched": enriched, "errors": errors_list, "total_candidates": len(candidates)})

@app.get("/api/partners/foursquare/results")
def foursquare_results():
    limit = min(safe_int(request.args.get("limit", 100), 100), 500)
    rows = db_fetchall("SELECT * FROM foursquare_enriched ORDER BY id DESC LIMIT %s", (limit,))
    return jsonify([dict(r) for r in rows])


# ============================================================
#  LIVERAMP SIM
# ============================================================
@app.post("/api/simulate/liveramp")
def simulate_liveramp():
    limit = safe_int(request.args.get("limit", 50), 50)
    now = int(time.time()); accepted = 0
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT ac.id, ac.mac_hash, ac.campaign_id, ac.creative_id,
                       ac.asset_id, ac.confidence_score
                FROM audience_candidates ac
                LEFT JOIN partner_exports pe ON pe.audience_candidate_id = ac.id AND pe.partner = 'liveramp_sim'
                WHERE pe.id IS NULL AND ac.audience_status = 'qualified'
                ORDER BY ac.confidence_score DESC LIMIT %s""", (limit,))
            candidates = cur.fetchall()
            for cand in candidates:
                seg = f"seg_lr_{cand['campaign_id'] or '0'}_{uuid.uuid4().hex[:8]}"
                mc = round(min(0.98, safe_float(cand["confidence_score"]) + 0.05), 3)
                cur.execute("""
                    INSERT INTO partner_exports
                    (partner, audience_candidate_id, mac_hash, campaign_id, creative_id,
                     asset_id, segment_id, match_type, match_confidence, status, created_at)
                    VALUES ('liveramp_sim',%s,%s,%s,%s,%s,%s,'probabilistic',%s,'accepted',%s)""",
                    (cand["id"], cand["mac_hash"], cand["campaign_id"], cand["creative_id"],
                     cand["asset_id"], seg, mc, now))
                accepted += 1
        conn.commit()
    except Exception: conn.rollback(); raise
    finally: conn.close()
    return jsonify({"ok": True, "partner": "liveramp_sim", "accepted": accepted, "total_candidates": len(candidates)})


# ============================================================
#  GROUNDTRUTH SIM
# ============================================================
@app.post("/api/simulate/groundtruth")
def simulate_groundtruth():
    limit = safe_int(request.args.get("limit", 50), 50)
    now = int(time.time()); accepted = 0
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT ac.id, ac.mac_hash, ac.campaign_id, ac.creative_id,
                       ac.asset_id, ac.confidence_score
                FROM audience_candidates ac
                LEFT JOIN partner_exports pe ON pe.audience_candidate_id = ac.id AND pe.partner = 'groundtruth_sim'
                WHERE pe.id IS NULL AND ac.audience_status = 'qualified'
                ORDER BY ac.confidence_score DESC LIMIT %s""", (limit,))
            candidates = cur.fetchall()
            for cand in candidates:
                aid = f"gt_aud_{cand['campaign_id'] or '0'}_{uuid.uuid4().hex[:8]}"
                cur.execute("""
                    INSERT INTO partner_exports
                    (partner, audience_candidate_id, mac_hash, campaign_id, creative_id,
                     asset_id, audience_id, match_type, match_confidence, status, created_at)
                    VALUES ('groundtruth_sim',%s,%s,%s,%s,%s,%s,'location_dwell',%s,'accepted',%s)""",
                    (cand["id"], cand["mac_hash"], cand["campaign_id"], cand["creative_id"],
                     cand["asset_id"], aid, safe_float(cand["confidence_score"]), now))
                accepted += 1
        conn.commit()
    except Exception: conn.rollback(); raise
    finally: conn.close()
    return jsonify({"ok": True, "partner": "groundtruth_sim", "accepted": accepted, "total_candidates": len(candidates)})


# ============================================================
#  PARTNER EXPORTS
# ============================================================
@app.get("/api/partners/exports")
def partner_exports_list():
    limit = min(safe_int(request.args.get("limit", 200), 200), 1000)
    partner = request.args.get("partner", "").strip()
    if partner:
        rows = db_fetchall("SELECT * FROM partner_exports WHERE partner=%s ORDER BY id DESC LIMIT %s", (partner, limit))
    else:
        rows = db_fetchall("SELECT * FROM partner_exports ORDER BY id DESC LIMIT %s", (limit,))
    return jsonify([dict(r) for r in rows])

@app.get("/api/partners/stats")
def partner_stats():
    by_p = db_fetchall("SELECT partner, COUNT(*) AS c, AVG(match_confidence) AS avg_conf FROM partner_exports GROUP BY partner")
    total = db_fetchone("SELECT COUNT(*) AS c FROM partner_exports")["c"]
    return jsonify({
        "ok": True, "total_exports": total,
        "by_partner": [{"partner": r["partner"], "count": r["c"], "avg_confidence": round(float(r["avg_conf"] or 0), 3)} for r in by_p],
    })


# ============================================================
#  MOCK DSP
# ============================================================
@app.get("/api/simulate/dsp/ad")
def simulate_dsp_ad():
    device_hash = request.args.get("device_hash", "").strip()
    campaign_id = request.args.get("campaign_id", "").strip()
    if not device_hash:
        return jsonify({"ok": False, "error": "device_hash required"}), 400

    q = """SELECT pe.*, ac.latitude, ac.longitude, ac.dwell_time_sec, ac.confidence_score
           FROM partner_exports pe
           JOIN audience_candidates ac ON ac.id = pe.audience_candidate_id
           WHERE pe.mac_hash = %s"""
    p = [device_hash]
    if campaign_id: q += " AND pe.campaign_id = %s"; p.append(campaign_id)
    q += " ORDER BY pe.id DESC LIMIT 1"

    row = db_fetchone(q, p)
    if not row:
        return jsonify({"ok": True, "matched": False, "message": "No audience match for this device"})

    imp_id = f"imp_{uuid.uuid4().hex[:12]}"
    now = int(time.time())
    ad = {
        "impression_id": imp_id,
        "campaign_id": row["campaign_id"],
        "creative_id": row["creative_id"] or "Creative_A",
        "ad_title": f"Retargeted Ad — Campaign {row['campaign_id']}",
        "ad_body": f"Detected near ({row['latitude']:.4f}, {row['longitude']:.4f}) with {row['dwell_time_sec']}s dwell. Confidence: {row['confidence_score']:.0%}",
        "image_url": "https://dummyimage.com/728x90/0f172a/38bdf8&text=FOOTFALL+INTELLIGENCE+—+RETARGETED+AD",
        "click_url": "https://example.com/landing",
        "source_partner": row["partner"],
        "source_segment": row["segment_id"] or row["audience_id"],
        "served_at": utc_now_iso(),
    }
    db_execute("""INSERT INTO mock_impressions
        (impression_id, device_hash, campaign_id, creative_id, ad_title, source_partner, served_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s)""",
        (imp_id, device_hash, row["campaign_id"], row["creative_id"], ad["ad_title"], row["partner"], now))
    return jsonify({"ok": True, "matched": True, "ad": ad})

@app.get("/api/simulate/dsp/impressions")
def dsp_impressions():
    limit = min(safe_int(request.args.get("limit", 100), 100), 500)
    rows = db_fetchall("SELECT * FROM mock_impressions ORDER BY id DESC LIMIT %s", (limit,))
    return jsonify([dict(r) for r in rows])


# ============================================================
#  PIPELINE
# ============================================================
@app.post("/api/pipeline/run")
def pipeline_run():
    results = {}
    if FSQ_API_KEY:
        try:
            with app.test_client() as c: results["foursquare"] = c.post("/api/partners/foursquare/enrich?limit=20").get_json()
        except Exception as e: results["foursquare"] = {"ok": False, "error": str(e)}
    else:
        results["foursquare"] = {"ok": False, "error": "FSQ_API_KEY not set"}
    try:
        with app.test_client() as c: results["liveramp"] = c.post("/api/simulate/liveramp?limit=50").get_json()
    except Exception as e: results["liveramp"] = {"ok": False, "error": str(e)}
    try:
        with app.test_client() as c: results["groundtruth"] = c.post("/api/simulate/groundtruth?limit=50").get_json()
    except Exception as e: results["groundtruth"] = {"ok": False, "error": str(e)}
    return jsonify({"ok": True, "pipeline": results})

@app.get("/api/pipeline/stats")
def pipeline_stats():
    raw = db_fetchone("SELECT COUNT(*) AS c FROM device_events")["c"]
    qualified = db_fetchone("SELECT COUNT(*) AS c FROM audience_candidates")["c"]
    enriched = db_fetchone("SELECT COUNT(DISTINCT audience_candidate_id) AS c FROM foursquare_enriched")["c"]
    lr = db_fetchone("SELECT COUNT(*) AS c FROM partner_exports WHERE partner='liveramp_sim'")["c"]
    gt = db_fetchone("SELECT COUNT(*) AS c FROM partner_exports WHERE partner='groundtruth_sim'")["c"]
    imps = db_fetchone("SELECT COUNT(*) AS c FROM mock_impressions")["c"]
    recent_aud = db_fetchall("""SELECT mac_hash, campaign_id, confidence_score, dwell_time_sec, created_at
        FROM audience_candidates ORDER BY id DESC LIMIT 5""")
    recent_exp = db_fetchall("""SELECT partner, mac_hash, campaign_id, segment_id, audience_id,
        match_confidence, status, created_at FROM partner_exports ORDER BY id DESC LIMIT 10""")
    return jsonify({
        "ok": True,
        "funnel": {"raw_events": raw, "audience_qualified": qualified, "foursquare_enriched": enriched,
                   "liveramp_exported": lr, "groundtruth_exported": gt, "dsp_impressions": imps},
        "recent_audience": [dict(r) for r in recent_aud],
        "recent_exports": [dict(r) for r in recent_exp],
    })


# --- Boot ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=True)
