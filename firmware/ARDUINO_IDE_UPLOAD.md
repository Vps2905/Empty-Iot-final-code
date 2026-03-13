# Arduino IDE upload quick steps (ESP32 + EC200U)

1. Open Arduino IDE and install **ESP32 by Espressif Systems** in Board Manager.
2. Open `firmware/esp32_footfall_tracker.ino`.
3. Select your board (for example ESP32 Dev Module) and the correct COM port.
4. Upload.
5. Open Serial Monitor at **115200** baud.
6. Provision credentials and token (then reboot):

```text
SET wifi_ssid=YourHotspot
SET wifi_pass=YourPassword
SET token=my_test_token_123
```

Optional:

```text
SET device_id=iot_sn_001
SET site_id=site_001
SET asset_id=asset_001
SET campaign_id=1023
```
