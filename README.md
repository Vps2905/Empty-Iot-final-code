# Empty-Iot-final-code Railway Fix

This package is the minimum working backend + pilot firmware for Railway deployment.

## Repo structure

- `backend/app.py`
- `backend/requirements.txt`
- `Procfile`
- `.env.example`
- `firmware/footfall_pilot_v3_arduino.ino`

## Railway steps

1. Replace the repo contents with these files.
2. Push to GitHub.
3. In Railway, deploy from the repo.
4. Set variables from `.env.example`.
5. Do not set `PORT` manually.
6. Generate service domain on port `8080`.
7. Open `/api/health`.

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
