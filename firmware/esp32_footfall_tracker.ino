#include <Arduino.h>
#include <cstring>
#include <cstdio>
#include <time.h>

#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPClient.h>
#include <Preferences.h>

#include <BLEDevice.h>
#include <BLEScan.h>
#include <BLEAdvertisedDevice.h>

#include <SPIFFS.h>
#include "mbedtls/sha256.h"

struct DeviceEntry {
  bool used;
  uint8_t mac_hash[32];
  int8_t last_rssi;
  uint64_t first_seen_up_s;
  uint64_t last_seen_up_s;
  uint64_t last_touch_up_s;
  char signal_source[16];
};

struct FootfallEvent {
  char schema_version[8];
  char event_id[56];
  char event_type[24];
  uint64_t timestamp_epoch;
  char timestamp_utc[21];
  uint64_t session_start_epoch;
  uint64_t session_end_epoch;
  char session_start_utc[21];
  char session_end_utc[21];
  uint8_t mac_hash[32];
  int8_t rssi;
  uint32_t dwell_time_sec;
  bool gps_fix;
  double lat;
  double lon;
  char signal_source[16];
  char device_id[32];
  char site_id[32];
  char asset_id[32];
  char asset_type[24];
  char creative_id[32];
  uint32_t campaign_id;
  char activation_name[48];
  char uplink_type[20];
  char fw_version[20];
};

struct HeartbeatPayload {
  char schema_version[8];
  char device_id[32];
  char site_id[32];
  char asset_id[32];
  char asset_type[24];
  char fw_version[20];
  char uplink_type[20];
  char ota_channel[16];
  uint64_t timestamp_epoch;
  char timestamp_utc[21];
  bool gps_fix;
  double lat;
  double lon;
  uint32_t uptime_sec;
  uint16_t queue_depth;
  uint32_t spool_bytes;
  uint32_t dropped_presence;
  uint32_t dropped_exit;
  uint32_t upload_failures;
  int wifi_status;
  bool modem_ready;
};

struct RuntimeConfig {
  char device_id[32];
  char site_id[32];
  char asset_id[32];
  char asset_type[24];
  char creative_id[32];
  char activation_name[48];
  char ota_channel[16];
  char wifi_ssid[64];
  char wifi_pass[64];
  char device_token[96];
  uint32_t campaign_id;
};

static const char* INGEST_URL = "https://grateful-vibrancy-production.up.railway.app/ingest";
static const char* HEARTBEAT_URL = "https://grateful-vibrancy-production.up.railway.app/heartbeat";
static const char* SCHEMA_VERSION = "1.0";
static const char* FW_VERSION = "2.8.1-bike-test";

// Keep defaults blank for security; provision over serial with SET key=value.
static const char* DEFAULT_WIFI_SSID = "";
static const char* DEFAULT_WIFI_PASS = "";
static const char* DEFAULT_TOKEN = "";
static const char* DEFAULT_DEVICE_ID = "iot_sn_001";
static const char* DEFAULT_SITE_ID = "site_001";
static const char* DEFAULT_ASSET_ID = "asset_001";
static const char* DEFAULT_ASSET_TYPE = "bike_mobile";
static const char* DEFAULT_CREATIVE_ID = "Creative_A";
static const char* DEFAULT_ACTIVATION = "Pilot_GT";
static const char* DEFAULT_OTA_CHANNEL = "stable";
static const uint32_t DEFAULT_CAMPAIGN_ID = 1023;

static const int MODEM_RX = 12;
static const int MODEM_TX = 13;
static const int MODEM_BAUD = 115200;
static HardwareSerial Modem(1);

static const uint32_t GNSS_POLL_MS = 5000;
static const uint32_t GNSS_STALE_SEC = 30;
static const uint16_t MAX_DEVICES = 512;
static const uint32_t EXIT_THRESHOLD_SEC = 20;
static const uint32_t MIN_DWELL_SEC = 10;
static const uint16_t EVENT_Q_LEN = 256;
static const uint8_t BATCH_MAX_EVENTS = 6;
static const uint32_t PRESENCE_TICK_MS = 1000;
static const uint32_t PRESENCE_RECENT_SEC = 1;
static const uint16_t PRESENCE_MAX_PER_TICK = 30;
static const uint32_t UPLOAD_PERIOD_MS = 5000;
static const uint32_t HEARTBEAT_PERIOD_MS = 60000;
static const uint32_t WIFI_CONNECT_TIMEOUT_MS = 15000;
static const uint32_t BACKOFF_MIN_MS = 5000;
static const uint32_t BACKOFF_MAX_MS = 60000;

static const char* SPOOL_PATH = "/spool.jsonl";
static const size_t SPOOL_MAX_BYTES = 256 * 1024;

static Preferences prefs;
static uint8_t g_salt[16];
static RuntimeConfig g_cfg{};
static DeviceEntry g_tab[MAX_DEVICES];
static FootfallEvent g_q[EVENT_Q_LEN];
static FootfallEvent g_uploadBatch[BATCH_MAX_EVENTS];

static volatile uint16_t g_q_head = 0;
static volatile uint16_t g_q_tail = 0;
static bool g_fix = false;
static double g_lat = 0.0;
static double g_lon = 0.0;
static uint64_t g_lastUtc = 0;
static uint64_t g_lastUtc_up_s = 0;
static BLEScan* pBLEScan = nullptr;
static bool g_spiffs_ok = false;

static uint32_t g_nextUploadMs = 0;
static uint32_t g_backoffMs = BACKOFF_MIN_MS;
static uint32_t g_lastHeartbeatMs = 0;
static uint32_t g_lastGnssMs = 0;
static uint32_t g_lastBleKickMs = 0;
static uint32_t g_lastSweepMs = 0;
static uint32_t g_lastPresenceMs = 0;

static uint32_t g_droppedPresence = 0;
static uint32_t g_droppedExit = 0;
static uint32_t g_uploadFailures = 0;

static size_t spoolSize();

static inline uint64_t up_s() { return (uint64_t)(millis() / 1000ULL); }

static void copyStr(char* dst, size_t dstSize, const char* src) {
  if (!dst || dstSize == 0) return;
  memset(dst, 0, dstSize);
  if (src) strncpy(dst, src, dstSize - 1);
}

static bool hasProvisionedWifi() {
  return strlen(g_cfg.wifi_ssid) > 0 && strlen(g_cfg.wifi_pass) > 0;
}

static bool hasProvisionedToken() {
  return strlen(g_cfg.device_token) > 0;
}

static uint64_t now_utc_epoch() {
  if (g_lastUtc == 0) return 0;
  return g_lastUtc + (up_s() - g_lastUtc_up_s);
}

static void epochToIsoUtc(uint64_t epoch, char out[21]) {
  if (epoch == 0) {
    out[0] = '\0';
    return;
  }
  time_t tt = (time_t)epoch;
  struct tm tm_utc;
  gmtime_r(&tt, &tm_utc);
  snprintf(out, 21, "%04d-%02d-%02dT%02d:%02d:%02dZ",
           tm_utc.tm_year + 1900, tm_utc.tm_mon + 1, tm_utc.tm_mday,
           tm_utc.tm_hour, tm_utc.tm_min, tm_utc.tm_sec);
}

static void generateEventId(char out[56]) {
  snprintf(out, 56, "evt_%s_%llu_%08x", g_cfg.device_id,
           (unsigned long long)up_s(), (uint32_t)esp_random());
}

static const char* currentUplinkType() { return "wifi_hotspot"; }

static uint16_t queueDepth() {
  if (g_q_head >= g_q_tail) return g_q_head - g_q_tail;
  return EVENT_Q_LEN - g_q_tail + g_q_head;
}

static void sha256_calc(uint8_t out32[32],
                        const uint8_t* data1, size_t len1,
                        const uint8_t* data2, size_t len2) {
  mbedtls_sha256_context ctx;
  mbedtls_sha256_init(&ctx);
  mbedtls_sha256_starts_ret(&ctx, 0);
  mbedtls_sha256_update_ret(&ctx, data1, len1);
  mbedtls_sha256_update_ret(&ctx, data2, len2);
  mbedtls_sha256_finish_ret(&ctx, out32);
  mbedtls_sha256_free(&ctx);
}

static void hashMacSalted(uint8_t out32[32], const uint8_t mac[6]) {
  sha256_calc(out32, mac, 6, g_salt, sizeof(g_salt));
}

static void hashToHex(const uint8_t h[32], char out[65]) {
  static const char* hex = "0123456789abcdef";
  for (int i = 0; i < 32; i++) {
    out[i * 2] = hex[(h[i] >> 4) & 0xF];
    out[i * 2 + 1] = hex[h[i] & 0xF];
  }
  out[64] = 0;
}

static uint64_t makeUnixUtc(int year, int month, int day, int hour, int minute, int second) {
  static const uint16_t daysBeforeMonth[] = {0,31,59,90,120,151,181,212,243,273,304,334};
  auto isLeap = [](int y) -> bool {
    return ((y % 4 == 0) && (y % 100 != 0)) || (y % 400 == 0);
  };

  if (year < 1970 || month < 1 || month > 12 || day < 1 || day > 31 ||
      hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) {
    return 0;
  }

  uint64_t days = 0;
  for (int y = 1970; y < year; y++) days += isLeap(y) ? 366 : 365;
  days += daysBeforeMonth[month - 1];
  if (month > 2 && isLeap(year)) days += 1;
  days += (day - 1);

  return days * 86400ULL + (uint64_t)hour * 3600ULL + (uint64_t)minute * 60ULL + (uint64_t)second;
}

static void loadOrCreateSalt() {
  prefs.begin("cfg", false);
  size_t len = prefs.getBytesLength("salt");
  if (len == 16) {
    prefs.getBytes("salt", g_salt, 16);
  } else {
    for (int i = 0; i < 16; i++) g_salt[i] = (uint8_t)esp_random();
    prefs.putBytes("salt", g_salt, 16);
  }
  prefs.end();
}

static void loadConfig() {
  prefs.begin("pilot", true);
  copyStr(g_cfg.wifi_ssid, sizeof(g_cfg.wifi_ssid), prefs.getString("wifi_ssid", DEFAULT_WIFI_SSID).c_str());
  copyStr(g_cfg.wifi_pass, sizeof(g_cfg.wifi_pass), prefs.getString("wifi_pass", DEFAULT_WIFI_PASS).c_str());
  copyStr(g_cfg.device_id, sizeof(g_cfg.device_id), prefs.getString("device_id", DEFAULT_DEVICE_ID).c_str());
  copyStr(g_cfg.site_id, sizeof(g_cfg.site_id), prefs.getString("site_id", DEFAULT_SITE_ID).c_str());
  copyStr(g_cfg.asset_id, sizeof(g_cfg.asset_id), prefs.getString("asset_id", DEFAULT_ASSET_ID).c_str());
  copyStr(g_cfg.asset_type, sizeof(g_cfg.asset_type), prefs.getString("asset_type", DEFAULT_ASSET_TYPE).c_str());
  copyStr(g_cfg.creative_id, sizeof(g_cfg.creative_id), prefs.getString("creative_id", DEFAULT_CREATIVE_ID).c_str());
  copyStr(g_cfg.activation_name, sizeof(g_cfg.activation_name), prefs.getString("activation", DEFAULT_ACTIVATION).c_str());
  copyStr(g_cfg.ota_channel, sizeof(g_cfg.ota_channel), prefs.getString("ota_channel", DEFAULT_OTA_CHANNEL).c_str());
  copyStr(g_cfg.device_token, sizeof(g_cfg.device_token), prefs.getString("token", DEFAULT_TOKEN).c_str());
  g_cfg.campaign_id = prefs.getUInt("campaign_id", DEFAULT_CAMPAIGN_ID);
  prefs.end();
}

static void handleProvisionCommand(String line) {
  line.trim();
  if (!line.startsWith("SET ")) return;

  int eq = line.indexOf('=');
  if (eq < 0) return;

  String key = line.substring(4, eq);
  String value = line.substring(eq + 1);
  key.trim();
  value.trim();

  prefs.begin("pilot", false);
  if (key == "wifi_ssid") prefs.putString("wifi_ssid", value);
  else if (key == "wifi_pass") prefs.putString("wifi_pass", value);
  else if (key == "device_id") prefs.putString("device_id", value);
  else if (key == "site_id") prefs.putString("site_id", value);
  else if (key == "asset_id") prefs.putString("asset_id", value);
  else if (key == "asset_type") prefs.putString("asset_type", value);
  else if (key == "creative_id") prefs.putString("creative_id", value);
  else if (key == "activation") prefs.putString("activation", value);
  else if (key == "ota_channel") prefs.putString("ota_channel", value);
  else if (key == "token") prefs.putString("token", value);
  else if (key == "campaign_id") prefs.putUInt("campaign_id", (uint32_t)value.toInt());
  prefs.end();

  Serial.println("[CFG] saved. reboot required.");
}


static bool qPush(const FootfallEvent& ev) {
  uint16_t next = (uint16_t)((g_q_head + 1) % EVENT_Q_LEN);
  if (next == g_q_tail) return false;
  g_q[g_q_head] = ev;
  g_q_head = next;
  return true;
}

static bool qPop(FootfallEvent& ev) {
  if (g_q_tail == g_q_head) return false;
  ev = g_q[g_q_tail];
  g_q_tail = (uint16_t)((g_q_tail + 1) % EVENT_Q_LEN);
  return true;
}

static int findEntry(const uint8_t h[32]) {
  for (int i = 0; i < MAX_DEVICES; i++) {
    if (g_tab[i].used && memcmp(g_tab[i].mac_hash, h, 32) == 0) return i;
  }
  return -1;
}

static int allocOrEvict() {
  for (int i = 0; i < MAX_DEVICES; i++) {
    if (!g_tab[i].used) return i;
  }
  int lru_i = 0;
  uint64_t lru_t = g_tab[0].last_touch_up_s;
  for (int i = 1; i < MAX_DEVICES; i++) {
    if (g_tab[i].last_touch_up_s < lru_t) {
      lru_t = g_tab[i].last_touch_up_s;
      lru_i = i;
    }
  }
  return lru_i;
}

static void fillCommonEventFields(FootfallEvent &ev, const char* eventType) {
  memset(&ev, 0, sizeof(ev));
  copyStr(ev.schema_version, sizeof(ev.schema_version), SCHEMA_VERSION);
  generateEventId(ev.event_id);
  copyStr(ev.event_type, sizeof(ev.event_type), eventType);
  copyStr(ev.device_id, sizeof(ev.device_id), g_cfg.device_id);
  copyStr(ev.site_id, sizeof(ev.site_id), g_cfg.site_id);
  copyStr(ev.asset_id, sizeof(ev.asset_id), g_cfg.asset_id);
  copyStr(ev.asset_type, sizeof(ev.asset_type), g_cfg.asset_type);
  copyStr(ev.creative_id, sizeof(ev.creative_id), g_cfg.creative_id);
  ev.campaign_id = g_cfg.campaign_id;
  copyStr(ev.activation_name, sizeof(ev.activation_name), g_cfg.activation_name);
  copyStr(ev.uplink_type, sizeof(ev.uplink_type), currentUplinkType());
  copyStr(ev.fw_version, sizeof(ev.fw_version), FW_VERSION);

  uint64_t utc = now_utc_epoch();
  ev.timestamp_epoch = utc;
  epochToIsoUtc(utc, ev.timestamp_utc);

  uint64_t utc_now = now_utc_epoch();
  if (g_fix && g_lastUtc != 0 && utc_now != 0 && (utc_now - g_lastUtc) <= GNSS_STALE_SEC) {
    ev.gps_fix = true;
    ev.lat = g_lat;
    ev.lon = g_lon;
  } else {
    ev.gps_fix = false;
    ev.lat = 0;
    ev.lon = 0;
  }
}

static String buildBatchJson(FootfallEvent* evs, uint8_t n) {
  String payload;
  payload.reserve(4200);
  payload += "{\"events\":[";
  for (uint8_t i = 0; i < n; i++) {
    char hx[65];
    hashToHex(evs[i].mac_hash, hx);
    if (i) payload += ",";

    payload += "{";
    payload += "\"schema_version\":\""; payload += evs[i].schema_version; payload += "\",";
    payload += "\"event_id\":\""; payload += evs[i].event_id; payload += "\",";
    payload += "\"event_type\":\""; payload += evs[i].event_type; payload += "\",";
    payload += "\"device_id\":\""; payload += evs[i].device_id; payload += "\",";
    payload += "\"site_id\":\""; payload += evs[i].site_id; payload += "\",";
    payload += "\"asset_id\":\""; payload += evs[i].asset_id; payload += "\",";
    payload += "\"asset_type\":\""; payload += evs[i].asset_type; payload += "\",";
    payload += "\"creative_id\":\""; payload += evs[i].creative_id; payload += "\",";
    payload += "\"campaign_id\":"; payload += evs[i].campaign_id; payload += ",";
    payload += "\"activation_name\":\""; payload += evs[i].activation_name; payload += "\",";
    payload += "\"timestamp_epoch\":"; payload += String((unsigned long long)evs[i].timestamp_epoch); payload += ",";
    payload += "\"timestamp_utc\":\""; payload += evs[i].timestamp_utc; payload += "\",";
    payload += "\"session_start_epoch\":"; payload += String((unsigned long long)evs[i].session_start_epoch); payload += ",";
    payload += "\"session_end_epoch\":"; payload += String((unsigned long long)evs[i].session_end_epoch); payload += ",";
    payload += "\"session_start_utc\":\""; payload += evs[i].session_start_utc; payload += "\",";
    payload += "\"session_end_utc\":\""; payload += evs[i].session_end_utc; payload += "\",";
    payload += "\"mac_hash\":\""; payload += hx; payload += "\",";
    payload += "\"signal_source\":\""; payload += evs[i].signal_source; payload += "\",";
    payload += "\"rssi\":"; payload += (int)evs[i].rssi; payload += ",";
    payload += "\"dwell_time_sec\":"; payload += evs[i].dwell_time_sec; payload += ",";
    payload += "\"gps_fix\":"; payload += (evs[i].gps_fix ? "true" : "false"); payload += ",";
    if (evs[i].gps_fix) {
      payload += "\"lat\":"; payload += String(evs[i].lat, 7); payload += ",";
      payload += "\"lon\":"; payload += String(evs[i].lon, 7); payload += ",";
    } else {
      payload += "\"lat\":null,";
      payload += "\"lon\":null,";
    }
    payload += "\"uplink_type\":\""; payload += evs[i].uplink_type; payload += "\",";
    payload += "\"fw_version\":\""; payload += evs[i].fw_version; payload += "\"";
    payload += "}";
  }
  payload += "]}";
  return payload;
}

static String buildHeartbeatJson() {
  HeartbeatPayload hb{};
  copyStr(hb.schema_version, sizeof(hb.schema_version), SCHEMA_VERSION);
  copyStr(hb.device_id, sizeof(hb.device_id), g_cfg.device_id);
  copyStr(hb.site_id, sizeof(hb.site_id), g_cfg.site_id);
  copyStr(hb.asset_id, sizeof(hb.asset_id), g_cfg.asset_id);
  copyStr(hb.asset_type, sizeof(hb.asset_type), g_cfg.asset_type);
  copyStr(hb.fw_version, sizeof(hb.fw_version), FW_VERSION);
  copyStr(hb.uplink_type, sizeof(hb.uplink_type), currentUplinkType());
  copyStr(hb.ota_channel, sizeof(hb.ota_channel), g_cfg.ota_channel);
  hb.timestamp_epoch = now_utc_epoch();
  epochToIsoUtc(hb.timestamp_epoch, hb.timestamp_utc);
  hb.gps_fix = g_fix;
  hb.lat = g_fix ? g_lat : 0;
  hb.lon = g_fix ? g_lon : 0;
  hb.uptime_sec = (uint32_t)up_s();
  hb.queue_depth = queueDepth();
  hb.spool_bytes = (uint32_t)spoolSize();
  hb.dropped_presence = g_droppedPresence;
  hb.dropped_exit = g_droppedExit;
  hb.upload_failures = g_uploadFailures;
  hb.wifi_status = WiFi.status();
  hb.modem_ready = true;

  String body;
  body.reserve(700);
  body += "{";
  body += "\"schema_version\":\""; body += hb.schema_version; body += "\",";
  body += "\"device_id\":\""; body += hb.device_id; body += "\",";
  body += "\"site_id\":\""; body += hb.site_id; body += "\",";
  body += "\"asset_id\":\""; body += hb.asset_id; body += "\",";
  body += "\"asset_type\":\""; body += hb.asset_type; body += "\",";
  body += "\"fw_version\":\""; body += hb.fw_version; body += "\",";
  body += "\"uplink_type\":\""; body += hb.uplink_type; body += "\",";
  body += "\"ota_channel\":\""; body += hb.ota_channel; body += "\",";
  body += "\"timestamp_epoch\":"; body += String((unsigned long long)hb.timestamp_epoch); body += ",";
  body += "\"timestamp_utc\":\""; body += hb.timestamp_utc; body += "\",";
  body += "\"gps_fix\":"; body += (hb.gps_fix ? "true" : "false"); body += ",";
  if (hb.gps_fix) {
    body += "\"lat\":"; body += String(hb.lat, 7); body += ",";
    body += "\"lon\":"; body += String(hb.lon, 7); body += ",";
  } else {
    body += "\"lat\":null,";
    body += "\"lon\":null,";
  }
  body += "\"uptime_sec\":"; body += hb.uptime_sec; body += ",";
  body += "\"queue_depth\":"; body += hb.queue_depth; body += ",";
  body += "\"spool_bytes\":"; body += hb.spool_bytes; body += ",";
  body += "\"dropped_presence\":"; body += hb.dropped_presence; body += ",";
  body += "\"dropped_exit\":"; body += hb.dropped_exit; body += ",";
  body += "\"upload_failures\":"; body += hb.upload_failures; body += ",";
  body += "\"wifi_status\":"; body += hb.wifi_status; body += ",";
  body += "\"modem_ready\":"; body += (hb.modem_ready ? "true" : "false");
  body += "}";
  return body;
}

static void observeMac(const uint8_t mac[6], int8_t rssi, const char* source) {
  uint8_t h[32];
  hashMacSalted(h, mac);
  int idx = findEntry(h);
  if (idx < 0) idx = allocOrEvict();

  DeviceEntry& e = g_tab[idx];
  uint64_t t = up_s();
  if (!e.used || memcmp(e.mac_hash, h, 32) != 0) {
    memset(&e, 0, sizeof(e));
    e.used = true;
    memcpy(e.mac_hash, h, 32);
    e.first_seen_up_s = t;
    e.last_seen_up_s = t;
    e.last_rssi = rssi;
    e.last_touch_up_s = t;
    copyStr(e.signal_source, sizeof(e.signal_source), source);
    return;
  }

  e.last_seen_up_s = t;
  e.last_rssi = rssi;
  e.last_touch_up_s = t;
  copyStr(e.signal_source, sizeof(e.signal_source), source);
}

static void presenceTick() {
  uint64_t t = up_s();
  uint16_t pushed = 0;
  for (int i = 0; i < MAX_DEVICES; i++) {
    DeviceEntry& e = g_tab[i];
    if (!e.used) continue;
    uint64_t age = (t > e.last_seen_up_s) ? (t - e.last_seen_up_s) : 0;
    if (age > PRESENCE_RECENT_SEC) continue;

    FootfallEvent ev;
    fillCommonEventFields(ev, "presence");
    memcpy(ev.mac_hash, e.mac_hash, 32);
    ev.rssi = e.last_rssi;
    ev.dwell_time_sec = 0;
    copyStr(ev.signal_source, sizeof(ev.signal_source), e.signal_source);

    if (!qPush(ev)) {
      g_droppedPresence++;
      break;
    }
    if (++pushed >= PRESENCE_MAX_PER_TICK) break;
  }
}

static void dwellSweep() {
  uint64_t t = up_s();
  for (int i = 0; i < MAX_DEVICES; i++) {
    DeviceEntry& e = g_tab[i];
    if (!e.used) continue;

    uint64_t gap = (t > e.last_seen_up_s) ? (t - e.last_seen_up_s) : 0;
    if (gap < EXIT_THRESHOLD_SEC) continue;

    uint32_t dwell = (e.last_seen_up_s > e.first_seen_up_s) ? (uint32_t)(e.last_seen_up_s - e.first_seen_up_s) : 0;
    if (dwell >= MIN_DWELL_SEC) {
      FootfallEvent ev;
      fillCommonEventFields(ev, "exposure_exit");
      memcpy(ev.mac_hash, e.mac_hash, 32);
      ev.rssi = e.last_rssi;
      ev.dwell_time_sec = dwell;
      copyStr(ev.signal_source, sizeof(ev.signal_source), e.signal_source);

      uint64_t nowUtc = now_utc_epoch();
      if (nowUtc != 0) {
        uint64_t ageSinceLastSeen = (t > e.last_seen_up_s) ? (t - e.last_seen_up_s) : 0;
        uint64_t endUtc = (nowUtc > ageSinceLastSeen) ? (nowUtc - ageSinceLastSeen) : 0;
        uint64_t startUtc = (endUtc > dwell) ? (endUtc - dwell) : 0;
        ev.session_start_epoch = startUtc;
        ev.session_end_epoch = endUtc;
        epochToIsoUtc(startUtc, ev.session_start_utc);
        epochToIsoUtc(endUtc, ev.session_end_utc);
      }

      if (!qPush(ev)) g_droppedExit++;
    }
    e.used = false;
  }
}

class BLECB : public BLEAdvertisedDeviceCallbacks {
  void onResult(BLEAdvertisedDevice dev) override {
    String s = dev.getAddress().toString();
    int v[6];
    uint8_t mac[6];

    if (sscanf(s.c_str(), "%x:%x:%x:%x:%x:%x", &v[0], &v[1], &v[2], &v[3], &v[4], &v[5]) == 6) {
      for (int i = 0; i < 6; i++) mac[i] = (uint8_t)v[i];
      observeMac(mac, (int8_t)dev.getRSSI(), "ble");
    }
  }
};

static BLECB g_blecb;

static void bleInit() {
  BLEDevice::init("");
  pBLEScan = BLEDevice::getScan();
  pBLEScan->setAdvertisedDeviceCallbacks(&g_blecb);
  pBLEScan->setActiveScan(false);
  pBLEScan->setInterval(80);
  pBLEScan->setWindow(80);
  Serial.println("[BLE] configured");
}

static size_t spoolSize() {
  if (!g_spiffs_ok) return 0;
  File f = SPIFFS.open(SPOOL_PATH, FILE_READ);
  if (!f) return 0;
  size_t s = f.size();
  f.close();
  return s;
}

static void spoolAppendLine(const String& line) {
  if (!g_spiffs_ok) return;
  if (spoolSize() > SPOOL_MAX_BYTES) SPIFFS.remove(SPOOL_PATH);

  File f = SPIFFS.open(SPOOL_PATH, FILE_APPEND);
  if (!f) return;
  f.print(line);
  f.print("\n");
  f.close();
}

static bool spoolReadPopLine(String &outLine) {
  if (!g_spiffs_ok) return false;
  File f = SPIFFS.open(SPOOL_PATH, FILE_READ);
  if (!f) return false;

  outLine = f.readStringUntil('\n');
  outLine.trim();
  bool hasMore = f.available();
  String rest;
  if (hasMore) rest = f.readString();
  f.close();

  SPIFFS.remove(SPOOL_PATH);
  if (hasMore && rest.length() > 0) {
    File w = SPIFFS.open(SPOOL_PATH, FILE_WRITE);
    if (w) {
      w.print(rest);
      w.close();
    }
  }
  return outLine.length() > 0;
}

static bool wifiConnectSta() {
  if (!hasProvisionedWifi()) {
    Serial.println("[WIFI] not provisioned");
    return false;
  }
  if (WiFi.status() == WL_CONNECTED) return true;

  WiFi.persistent(false);
  WiFi.setSleep(false);
  WiFi.setAutoReconnect(true);
  WiFi.mode(WIFI_STA);
  delay(300);

  Serial.printf("[WIFI] connecting to SSID=%s\n", g_cfg.wifi_ssid);
  WiFi.begin(g_cfg.wifi_ssid, g_cfg.wifi_pass);

  uint32_t start = millis();
  wl_status_t lastStatus = WL_IDLE_STATUS;
  while ((millis() - start) < WIFI_CONNECT_TIMEOUT_MS) {
    wl_status_t st = WiFi.status();
    if (st != lastStatus) {
      Serial.printf("[WIFI] status=%d\n", (int)st);
      lastStatus = st;
    }

    if (st == WL_CONNECTED) {
      Serial.print("[WIFI] connected IP=");
      Serial.println(WiFi.localIP());
      return true;
    }
    delay(250);
  }

  Serial.println("[WIFI] connect failed");
  return false;
}

static bool httpsPostJson(const char* url, const String& body, String* respOut = nullptr) {
  if (!hasProvisionedToken()) {
    Serial.println("[HTTP] token not provisioned");
    return false;
  }
  if (!wifiConnectSta()) return false;

  WiFiClientSecure client;
  client.setInsecure();

  HTTPClient http;
  if (!http.begin(client, url)) {
    Serial.println("[HTTP] begin failed");
    return false;
  }

  http.addHeader("Content-Type", "application/json");
  http.addHeader("Authorization", String("Bearer ") + g_cfg.device_token);

  int code = http.POST((uint8_t*)body.c_str(), body.length());
  String resp = http.getString();

  Serial.printf("[HTTP] POST %s -> %d\n", url, code);
  if (resp.length() > 0) Serial.println(resp);
  http.end();

  if (respOut) *respOut = resp;
  return code >= 200 && code < 300;
}

static bool sendHeartbeatOnce() {
  return httpsPostJson(HEARTBEAT_URL, buildHeartbeatJson());
}

static bool doUploadBurst() {
  if (!hasProvisionedWifi() || !hasProvisionedToken()) return false;
  if (!wifiConnectSta()) return false;

  bool anyOk = false;
  String line;
  if (spoolReadPopLine(line) && line.length() > 0) {
    bool ok = httpsPostJson(INGEST_URL, line);
    if (!ok) {
      spoolAppendLine(line);
      g_uploadFailures++;
      return false;
    }
    anyOk = true;
  }

  uint8_t n = 0;
  FootfallEvent ev;
  while (n < BATCH_MAX_EVENTS && qPop(ev)) {
    g_uploadBatch[n++] = ev;
  }

  if (n > 0) {
    String body = buildBatchJson(g_uploadBatch, n);
    bool ok = httpsPostJson(INGEST_URL, body);
    if (!ok) {
      spoolAppendLine(body);
      g_uploadFailures++;
      return false;
    }
    anyOk = true;
  }
  return anyOk;
}

static void scheduleNextUpload(bool lastOk) {
  if (lastOk) {
    g_backoffMs = BACKOFF_MIN_MS;
    g_nextUploadMs = millis() + UPLOAD_PERIOD_MS;
  } else {
    g_backoffMs = (g_backoffMs * 2 > BACKOFF_MAX_MS) ? BACKOFF_MAX_MS : g_backoffMs * 2;
    g_nextUploadMs = millis() + g_backoffMs;
  }
}

static String atRead(uint32_t timeoutMs) {
  String r;
  uint32_t start = millis();
  while (millis() - start < timeoutMs) {
    while (Modem.available()) r += (char)Modem.read();
    if (r.indexOf("\r\nOK\r\n") >= 0) break;
    if (r.indexOf("+CME ERROR:") >= 0) break;
    delay(10);
  }
  return r;
}

static String atCmd(const char* cmd, uint32_t timeoutMs = 1500) {
  while (Modem.available()) Modem.read();
  Modem.print(cmd);
  Modem.print("\r\n");
  return atRead(timeoutMs);
}

static bool gnssEnsureOn() {
  String r = atCmd("AT", 1500);
  if (r.indexOf("OK") < 0) return false;

  r = atCmd("AT+QGPS?", 1500);
  bool running = (r.indexOf("+QGPS: 1") >= 0);
  if (!running) {
    r = atCmd("AT+QGPS=1", 3000);
    if (r.indexOf("OK") < 0) return false;
  }

  atCmd("AT+QGPSPOWER=1", 2000);
  return true;
}


static bool getCsvField(const String& line, int fieldIndex, String& out) {
  int start = 0;
  int idx = 0;
  while (start <= (int)line.length()) {
    int end = line.indexOf(',', start);
    if (end < 0) end = line.length();
    if (idx == fieldIndex) {
      out = line.substring(start, end);
      out.trim();
      return true;
    }
    idx++;
    if (end >= (int)line.length()) break;
    start = end + 1;
  }
  out = "";
  return false;
}

static bool gnssPoll(bool &fix, double &lat, double &lon, uint64_t &utc_epoch) {
  String r = atCmd("AT+QGPSLOC=2", 4000);
  if (r.indexOf("+CME ERROR: 516") >= 0) {
    fix = false;
    return true;
  }

  int p = r.indexOf("+QGPSLOC:");
  if (p < 0) {
    fix = false;
    return true;
  }

  int colon = r.indexOf(':', p);
  if (colon < 0) {
    fix = false;
    return true;
  }

  String line = r.substring(colon + 1);
  line.trim();
  int eol = line.indexOf('\n');
  if (eol >= 0) line = line.substring(0, eol);
  line.trim();

  int c1 = line.indexOf(',');
  int c2 = (c1 >= 0) ? line.indexOf(',', c1 + 1) : -1;
  int c3 = (c2 >= 0) ? line.indexOf(',', c2 + 1) : -1;
  if (c1 < 0 || c2 < 0 || c3 < 0) {
    fix = false;
    return true;
  }

  String utcStr = line.substring(0, c1); utcStr.trim();
  String latStr = line.substring(c1 + 1, c2); latStr.trim();
  String lonStr = line.substring(c2 + 1, c3); lonStr.trim();

  // Quectel QGPSLOC format keeps date as field index 9 (ddmmyy).
  String dateStr;
  if (!getCsvField(line, 9, dateStr)) dateStr = "";

  lat = latStr.toDouble();
  lon = lonStr.toDouble();
  if (lat < -90.0 || lat > 90.0 || lon < -180.0 || lon > 180.0) {
    fix = false;
    return true;
  }

  bool utcDigits = true;
  for (int i = 0; i < 6 && i < (int)utcStr.length(); i++) {
    if (utcStr[i] < '0' || utcStr[i] > '9') { utcDigits = false; break; }
  }
  bool dateDigits = (dateStr.length() == 6);
  for (int i = 0; i < 6 && dateDigits; i++) {
    if (dateStr[i] < '0' || dateStr[i] > '9') { dateDigits = false; break; }
  }

  if (utcStr.length() < 6 || !utcDigits || !dateDigits) {
    fix = true;
    utc_epoch = 0;
    return true;
  }

  int hh = utcStr.substring(0, 2).toInt();
  int mm = utcStr.substring(2, 4).toInt();
  int ss = utcStr.substring(4, 6).toInt();
  int dd = dateStr.substring(0, 2).toInt();
  int mo = dateStr.substring(2, 4).toInt();
  int yy = dateStr.substring(4, 6).toInt() + 2000;

  utc_epoch = makeUnixUtc(yy, mo, dd, hh, mm, ss);
  fix = true;
  return true;
}

void setup() {
  Serial.begin(115200);
  uint32_t t0 = millis();
  while (!Serial && (millis() - t0) < 3000) delay(10);
  delay(200);

  Serial.println("[BOOT] starting");
  loadOrCreateSalt();
  loadConfig();

  Serial.printf("[CFG] device_id=%s site_id=%s asset_id=%s\n", g_cfg.device_id, g_cfg.site_id, g_cfg.asset_id);
  Serial.printf("[CFG] wifi_ssid=%s\n", strlen(g_cfg.wifi_ssid) ? g_cfg.wifi_ssid : "<not set>");

  g_spiffs_ok = SPIFFS.begin(true);
  if (!g_spiffs_ok) {
    Serial.println("[SPIFFS] mount failed, continuing without spool");
  } else {
    Serial.printf("[SPIFFS] ok, spool=%u bytes\n", (unsigned)spoolSize());
  }

  Modem.begin(MODEM_BAUD, SERIAL_8N1, MODEM_RX, MODEM_TX);
  delay(300);
  Serial.printf("[GNSS] ensure on = %s\n", gnssEnsureOn() ? "OK" : "FAIL");

  bleInit();

  g_lastBleKickMs = millis();
  g_lastSweepMs = millis();
  g_lastPresenceMs = millis();
  g_lastGnssMs = millis();
  g_lastHeartbeatMs = millis();

  g_nextUploadMs = millis() + 5000;
  g_backoffMs = BACKOFF_MIN_MS;

  Serial.println("[BOOT] running");
  Serial.println("[MODE] BLE + GNSS + hotspot upload only");
  Serial.println("[CMD] SET wifi_ssid=..., SET wifi_pass=..., SET token=...");

  if (hasProvisionedWifi()) wifiConnectSta();
}

void loop() {
  uint32_t ms = millis();

  while (Serial.available()) {
    String line = Serial.readStringUntil('\n');
    handleProvisionCommand(line);
  }

  if (pBLEScan && (ms - g_lastBleKickMs >= 1200)) {
    g_lastBleKickMs = ms;
    pBLEScan->start(1, false);
    pBLEScan->clearResults();
  }

  if (ms - g_lastSweepMs >= 1000) {
    g_lastSweepMs = ms;
    dwellSweep();
  }

  if (ms - g_lastPresenceMs >= PRESENCE_TICK_MS) {
    g_lastPresenceMs = ms;
    presenceTick();
  }

  if (ms - g_lastGnssMs >= GNSS_POLL_MS) {
    g_lastGnssMs = ms;
    bool fix = false;
    double lat = 0, lon = 0;
    uint64_t epoch = 0;

    if (gnssPoll(fix, lat, lon, epoch)) {
      g_fix = fix;
      if (fix) {
        g_lat = lat;
        g_lon = lon;
        if (epoch > 0) {
          g_lastUtc = epoch;
          g_lastUtc_up_s = up_s();
        }
        Serial.printf("[GNSS] FIX lat=%.7f lon=%.7f utc=%llu\n", g_lat, g_lon, (unsigned long long)g_lastUtc);
      } else {
        Serial.println("[GNSS] no fix");
      }
    }
  }

  if ((int32_t)(ms - g_nextUploadMs) >= 0) {
    bool ok = doUploadBurst();
    scheduleNextUpload(ok);
  }

  if (ms - g_lastHeartbeatMs >= HEARTBEAT_PERIOD_MS) {
    g_lastHeartbeatMs = ms;
    sendHeartbeatOnce();
  }

  delay(5);
}
