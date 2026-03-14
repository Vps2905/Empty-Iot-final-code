# Empty-Iot-final-code Railway Fix

This package is the minimum working backend + pilot firmware for Railway deployment.
It now includes a root `requirements.txt` and `Dockerfile` so Railway can build and start the service deterministically.

## Repo structure

- `backend/app.py`
- `backend/requirements.txt`
- `requirements.txt`
- `Dockerfile`
- `Procfile`
- `.env.example`
- `firmware/footfall_pilot_v3_arduino.ino`

## Railway steps

1. Push this repo to GitHub.
2. In Railway, deploy from the repo.
3. Railway will build using the included `Dockerfile`.
4. Set variables from `.env.example`.
5. Do not set `PORT` manually.
6. Generate a service domain.
7. Open `/api/health`.

## Important note about storage

`DB_PATH` defaults to `/tmp/footfall.db` in `.env.example` so the app can always start on Railway.
That file is ephemeral and may reset on redeploy or restart.

## Test endpoints

- `/api/health`
- `/api/stats`
- `/api/events`
- `/ingest`
- `/api/ingest`

## Quick curl test

```bash
curl -X POST "https://YOUR_APP.up.railway.app/ingest" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ff_ingest_change_me" \
  -d '{
    "device_id":"FF-001",
    "firmware_version":"pilot-v3",
    "events":[{"event_type":"presence","count":1,"lat":17.43388,"lon":78.42669,"rssi":-62}]
  }'
```

## Firmware update before flashing

Edit inside `firmware/footfall_pilot_v3_arduino.ino`:

- `WIFI_SSID`
- `WIFI_PASS`
- `BACKEND_URL`
- `API_KEY`
- `DEVICE_ID`
