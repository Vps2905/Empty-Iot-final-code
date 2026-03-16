# Footfall Intelligence Platform — Full Pipeline

> **IoT Detection → Audience Builder → Foursquare Enrichment → LiveRamp/GroundTruth Export → DSP Ad Delivery**

## Architecture

```
ESP32-S3 + EC200U (WiFi probes + BLE + GNSS)
        │
        ▼
  POST /ingest (Railway backend)
        │
        ▼
  raw device_events table
        │
        ▼
  Auto-qualification engine
  (confidence scoring on every ingest)
        │
        ▼
  audience_candidates table
        │
        ├──► Foursquare Places API (real) → nearby POI enrichment
        ├──► LiveRamp Simulator → segment export
        ├──► GroundTruth Simulator → location audience export
        │
        ▼
  partner_exports table
        │
        ▼
  Mock DSP Ad Server → ad impression delivery
        │
        ▼
  mock_impressions table → visible in DSP Viewer tab
```

## What's New (vs original repo)

| Feature | Type | Description |
|---------|------|-------------|
| Audience Builder | Auto | Every ingest event is scored; qualifying events become audience candidates |
| Foursquare Enrichment | **Real API** | Nearby POIs from Foursquare Places API using your lat/lon |
| LiveRamp Export | Simulated | Probabilistic match segments with confidence scoring |
| GroundTruth Export | Simulated | Location-based retargetable exposure audiences |
| Mock DSP | Simulated | Ad decision engine + visual ad rendering in browser |
| Pipeline Runner | One-click | "Run Full Pipeline" button triggers all 3 partner steps |
| 7-tab Dashboard | UI | Overview, Pipeline, Audience, Partners, DSP Viewer, Geospatial, Events |

## Deployment to Railway

### 1. Set Environment Variables in Railway

Go to your Railway project → Variables tab. Add these:

```
INGEST_API_KEY=<your-new-secure-key>       # CHANGE from old key
ADMIN_API_KEY=<your-admin-key>
FSQ_API_KEY=<your-foursquare-api-key>      # Get from developer.foursquare.com
FSQ_RADIUS=200
FSQ_LIMIT=5
AQ_MIN_CONFIDENCE=0.50
AQ_MIN_DWELL=5
AQ_MIN_RSSI=-85
GEOFENCE_LAT=17.43388
GEOFENCE_LON=78.42669
GEOFENCE_RADIUS_M=300
```

### 2. Get a Foursquare API Key (free)

1. Go to [developer.foursquare.com](https://developer.foursquare.com)
2. Create an account → Create a project
3. Generate an API key
4. Paste it as `FSQ_API_KEY` in Railway

### 3. Push Code

Replace the files in your existing repo with this package:

```bash
# In your local clone of Vps2905/Empty-Iot-final-code
cp -r output/* .
git add -A
git commit -m "Add full pipeline: audience builder + partners + DSP"
git push origin main
```

Railway will auto-deploy.

### 4. Update Firmware API Key

In your ESP32 firmware, change `API_KEY` to match the new `INGEST_API_KEY`.

**CRITICAL**: Also change your WiFi password. Both were exposed in the chat.

## Dashboard Tabs

### Overview
- KPI cards: raw events, unique MACs, audience qualified, partner exports, DSP impressions
- Event volume trend chart
- Event type distribution

### Pipeline
- Visual funnel: Raw → Qualified → Enriched → LiveRamp → GroundTruth → DSP
- "Run Full Pipeline" button (one-click demo)
- Recent partner export log
- Audience confidence distribution chart

### Audience
- All qualified audience candidates with confidence scores
- Click any MAC hash to jump to DSP Viewer for ad testing
- Stats: total qualified, avg confidence, avg dwell

### Partners
- Foursquare POI enrichment results (real data)
- Individual "Enrich via Foursquare" / "Push to LiveRamp" / "Push to GroundTruth" buttons
- Export breakdown doughnut chart
- Full partner export log

### DSP Viewer
- Paste a device_hash from the Audience tab
- Click "Load Retargeted Ad" → see the mock ad render
- Raw API response visible
- Full impression log

### Geospatial
- Dark-themed CARTO map with heatmap overlay
- Marker popups with device/event details

### Events
- Full raw event stream with filters
- Device and event type dropdowns

## API Endpoints

### Original (unchanged)
| Method | Path | Description |
|--------|------|-------------|
| POST | `/ingest` | Device event ingestion (now also auto-qualifies audience) |
| GET | `/api/health` | Health check (now includes audience/partner/impression counts) |
| GET | `/api/stats` | Aggregate statistics |
| GET | `/api/events` | Raw event listing |
| GET | `/api/export.csv` | CSV export (admin key required) |

### New: Audience
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/audience` | List audience candidates |
| GET | `/api/audience/stats` | Audience aggregate stats |

### New: Partners
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/partners/foursquare/enrich` | Enrich candidates via real Foursquare API |
| GET | `/api/partners/foursquare/results` | View Foursquare enrichment results |
| POST | `/api/simulate/liveramp` | Push to simulated LiveRamp |
| POST | `/api/simulate/groundtruth` | Push to simulated GroundTruth |
| GET | `/api/partners/exports` | All partner export records |
| GET | `/api/partners/stats` | Partner export statistics |

### New: DSP
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/simulate/dsp/ad` | Check audience match & serve mock ad |
| GET | `/api/simulate/dsp/impressions` | View served impressions |

### New: Pipeline
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/pipeline/run` | One-click: Foursquare → LiveRamp → GroundTruth |
| GET | `/api/pipeline/stats` | Full funnel statistics |

## Demo Walkthrough (for CTO/Investor)

### Prep (before the meeting)
1. Deploy to Railway with the Foursquare key set
2. Make sure your ESP32 device has been running and sending data
3. Open the dashboard and verify events are flowing

### During the demo

**Step 1 — Show live IoT data (Overview tab)**
> "Our ESP32 device is detecting WiFi and Bluetooth signals in real time. Every probe is hashed for privacy, tagged with GPS, and streamed to our cloud backend."

**Step 2 — Show the pipeline (Pipeline tab)**
> "Watch this — one click runs our entire data pipeline."
> Click **Run Full Pipeline**.
> "Events flow through audience qualification, Foursquare enrichment for location context, then export to LiveRamp and GroundTruth for retargeting."

**Step 3 — Show audience quality (Audience tab)**
> "Each detection is scored on signal strength, dwell time, and repeat visits. Only high-confidence contacts enter the audience."

**Step 4 — Show real Foursquare data (Partners tab)**
> "This is real Foursquare Places API data — we're enriching each contact with the nearest POI. This proves our GPS coordinates are accurate and commercially useful."

**Step 5 — Show the ad (DSP Viewer tab)**
> Click a MAC hash from the Audience tab → it auto-fills in DSP Viewer.
> Click **Load Retargeted Ad**.
> "This is what the end advertiser sees. A real retargeted ad delivered to a device we detected at a physical location."

**Step 6 — Show the map (Geospatial tab)**
> "Here's where our detections happened. Each dot is a qualified audience contact."

### Key talking points
- Privacy-first: MAC addresses are SHA-256 hashed with a unique salt
- Foursquare integration is **real**, not simulated
- LiveRamp and GroundTruth paths mirror their actual API contracts
- The DSP viewer proves the complete retargeting loop works
- Everything runs on a single $5/month Railway instance

## What's Real vs Simulated

| Component | Status |
|-----------|--------|
| ESP32 WiFi/BLE detection | **Real** |
| GNSS location | **Real** |
| Backend ingestion | **Real** |
| Audience scoring | **Real** |
| Foursquare POI enrichment | **Real API** |
| LiveRamp segment export | **Simulated** (mirrors real API contract) |
| GroundTruth audience export | **Simulated** (mirrors real API contract) |
| DSP ad delivery | **Simulated** (mock ad server) |

## Security Reminders

- [ ] Rotate your WiFi password (it was exposed)
- [ ] Rotate your API key (it was exposed)
- [ ] Set `INGEST_API_KEY` as a Railway environment variable
- [ ] Never hardcode secrets in firmware for production
- [ ] The Foursquare free tier has rate limits — check your usage
