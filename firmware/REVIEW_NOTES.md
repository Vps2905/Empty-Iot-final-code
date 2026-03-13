# Firmware review notes

## Key issues found in the provided sketch

1. **Compile-order bug:** `buildHeartbeatJson()` calls `spoolSize()` before `spoolSize()` is declared/defined.
2. **Hardcoded secrets in source:** SSID/password/token defaults were embedded directly in firmware.
3. **mbedTLS API compatibility:** `mbedtls_sha256_starts/update/finish` are deprecated on newer ESP-IDF cores.

## What was updated

- Added a forward declaration for `spoolSize()`.
- Switched SHA-256 calls to `_ret` variants for ESP-IDF/Arduino compatibility.
- Removed hardcoded Wi-Fi and token defaults (left blank, still provisionable through serial `SET ...`).
- Kept event schema and `/ingest` + `/heartbeat` payload layout compatible with backend `app.py`.

## Provisioning example

Send over serial monitor, then reboot:

```text
SET wifi_ssid=YourHotspot
SET wifi_pass=YourPassword
SET token=my_test_token_123
```
