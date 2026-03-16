/*
 * Footfall Pilot — ESP32-S3 + EC200U Firmware
 * Board: 7SEMI ESP32-S3 EC200U 4G
 * Arduino IDE:
 *   Tools -> Board: "ESP32S3 Dev Module"
 *   Tools -> USB CDC On Boot: "Enabled"
 *
 * Updated:
 * - Dual WiFi support
 *   1) Home WiFi (primary)
 *   2) Phone hotspot (secondary)
 * - SIM upload fallback skeleton added (not yet implemented)
 * - No 0.0/0.0 GPS upload
 * - Reuses last known valid GPS while fix is still fresh
 * - Smaller upload batches for backend stability
 * - Better HTTP/DNS handling for Railway
 */

#include <Arduino.h>
#include "types.h"

#include <cstring>
#include <cstdio>
#include <time.h>

#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPClient.h>
#include <esp_wifi.h>
#include <Preferences.h>

#include <BLEDevice.h>
#include <BLEScan.h>
#include <BLEAdvertisedDevice.h>

#include <SPIFFS.h>
#include "mbedtls/sha256.h"

// lwIP DNS direct control
#include "lwip/dns.h"
#include "lwip/ip_addr.h"

// =============================================================
// DEPLOYMENT CONFIG
// =============================================================
static const char* DEVICE_ID   = "FF-001";
static const char* FW_VERSION  = "prod-v5";

// -------------------------------------------------------------
// WiFi priority:
// 1) Home WiFi
// 2) Phone hotspot
// -------------------------------------------------------------
struct WifiCred {
  const char* ssid;
  const char* pass;
  const char* label;
};

static const WifiCred WIFI_LIST[] = {
  { "Vijay",    "Vijay@29ps",          "HOME_WIFI" },
  { "vps", "Vijayps@290504", "PHONE_HOTSPOT" }
};

static const uint8_t WIFI_LIST_COUNT = sizeof(WIFI_LIST) / sizeof(WIFI_LIST[0]);
static int g_lastWifiIndex = -1;

static const char* BACKEND_HOST = "grateful-vibrancy-production-fc0b.up.railway.app";
static const char* BACKEND_URL  = "https://grateful-vibrancy-production-fc0b.up.railway.app/api/ingest";
static const char* API_KEY      = "psv_2004";

static const char*    ASSET_ID           = "VEHICLE_01";
static const char*    CREATIVE_ID        = "Creative_A";
static const char*    ACTIVATION_NAME    = "RoadTest_Pilot";
static const uint32_t CAMPAIGN_ID        = 1001;
static const uint64_t CAMPAIGN_START_UTC = 0ULL;
static const uint64_t CAMPAIGN_END_UTC   = 0ULL;

static const int MODEM_RX   = 12;
static const int MODEM_TX   = 13;
static const int MODEM_BAUD = 115200;
static HardwareSerial Modem(1);

static const uint32_t GNSS_POLL_MS   = 5000;
static const uint32_t GNSS_STALE_SEC = 120;

static const uint16_t MAX_DEVICES             = 512;
static const uint32_t EXIT_THRESHOLD_SEC      = 20;
static const uint32_t MIN_DWELL_SEC           = 5;
static const uint16_t EVENT_Q_LEN             = 256;
static const uint8_t  BATCH_MAX_EVENTS        = 5;
static const uint32_t PRESENCE_TICK_MS        = 2000;
static const uint32_t PRESENCE_RECENT_SEC     = 2;
static const uint16_t PRESENCE_MAX_PER_TICK   = 30;

static const bool     WIFI_CHANNEL_HOP_ENABLE = true;
static const uint32_t WIFI_CH_HOP_MS          = 200;
static const uint8_t  WIFI_CH_MIN             = 1;
static const uint8_t  WIFI_CH_MAX             = 13;

static const uint32_t UPLOAD_PERIOD_MS        = 5000;
static const uint32_t WIFI_CONNECT_TIMEOUT_MS = 15000;
static const uint32_t UPLOAD_BURST_MAX_MS     = 12000;
static const uint8_t  UPLOAD_BURST_MAX_POSTS  = 4;
static const uint32_t BACKOFF_MIN_MS          = 5000;
static const uint32_t BACKOFF_MAX_MS          = 60000;

static const uint32_t HTTP_CONNECT_TIMEOUT_MS = 20000;
static const uint32_t HTTP_READ_TIMEOUT_MS    = 45000;

static const char*    SPOOL_PATH              = "/spool.jsonl";
static const size_t   SPOOL_MAX_BYTES         = 256 * 1024;

// =============================================================
// GLOBALS
// =============================================================
static Preferences prefs;
static uint8_t g_salt[16];

static DeviceEntry   g_tab[MAX_DEVICES];
static FootfallEvent g_q[EVENT_Q_LEN];

static volatile uint16_t g_q_head = 0;
static volatile uint16_t g_q_tail = 0;

static bool     g_fix            = false;
static bool     g_hasEverFix     = false;
static double   g_lat            = 0.0;
static double   g_lon            = 0.0;
static double   g_lastGoodLat    = 0.0;
static double   g_lastGoodLon    = 0.0;
static uint64_t g_lastUtc        = 0;
static uint64_t g_lastUtc_up_s   = 0;
static uint32_t g_lastGnssMs     = 0;
static uint32_t g_lastFixMs      = 0;

static BLEScan* pBLEScan = nullptr;
static bool     g_sniffer_on = false;

static uint32_t g_nextUploadMs   = 0;
static uint32_t g_backoffMs      = BACKOFF_MIN_MS;
static uint32_t g_lastChanHopMs  = 0;
static uint8_t  g_chan           = WIFI_CH_MIN;
static uint32_t g_lastBleKickMs  = 0;
static uint32_t g_lastSweepMs    = 0;
static uint32_t g_lastPresenceMs = 0;

static uint32_t g_totalDetections = 0;
static uint32_t g_totalUploaded   = 0;
static uint32_t g_totalSpooled    = 0;
static uint32_t g_totalRejected   = 0;

// =============================================================
// TIME HELPERS
// =============================================================
static inline uint64_t up_s() {
  return (uint64_t)(millis() / 1000ULL);
}

static bool in_campaign(uint64_t utc_epoch) {
  if (CAMPAIGN_START_UTC == 0 || CAMPAIGN_END_UTC == 0) return true;
  return (utc_epoch >= CAMPAIGN_START_UTC && utc_epoch <= CAMPAIGN_END_UTC);
}

static uint64_t now_utc_epoch() {
  if (g_lastUtc == 0) return 0;
  return g_lastUtc + (up_s() - g_lastUtc_up_s);
}

static void epochToIsoUtc(uint64_t epoch, char out[24]) {
  time_t tt = (time_t)epoch;
  struct tm tm_utc;
  gmtime_r(&tt, &tm_utc);
  snprintf(out, 24, "%04d-%02d-%02dT%02d:%02d:%02dZ",
           tm_utc.tm_year + 1900, tm_utc.tm_mon + 1, tm_utc.tm_mday,
           tm_utc.tm_hour, tm_utc.tm_min, tm_utc.tm_sec);
}

static uint64_t utcEpochFromFields(int year, int month, int day, int hour, int minute, int second) {
  auto days_from_civil = [](int y, unsigned m, unsigned d) -> int64_t {
    y -= m <= 2;
    const int era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = (unsigned)(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int)doe - 719468;
  };

  if (year < 2024 || month < 1 || month > 12 || day < 1 || day > 31 ||
      hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) {
    return 0;
  }

  int64_t days = days_from_civil(year, (unsigned)month, (unsigned)day);
  int64_t secs = days * 86400LL + hour * 3600LL + minute * 60LL + second;
  if (secs <= 1700000000LL) return 0;
  return (uint64_t)secs;
}

// =============================================================
// CRYPTO
// =============================================================
static void loadOrCreateSalt() {
  prefs.begin("cfg", false);
  if (prefs.getBytesLength("salt") == 16) {
    prefs.getBytes("salt", g_salt, 16);
    Serial.println("[SALT] Loaded existing salt");
  } else {
    for (int i = 0; i < 16; i++) g_salt[i] = (uint8_t)esp_random();
    prefs.putBytes("salt", g_salt, 16);
    Serial.println("[SALT] Generated new salt");
  }
  prefs.end();
}

static void hashMacSalted(uint8_t out32[32], const uint8_t mac[6]) {
  mbedtls_sha256_context ctx;
  mbedtls_sha256_init(&ctx);
  mbedtls_sha256_starts(&ctx, 0);
  mbedtls_sha256_update(&ctx, mac, 6);
  mbedtls_sha256_update(&ctx, g_salt, sizeof(g_salt));
  mbedtls_sha256_finish(&ctx, out32);
  mbedtls_sha256_free(&ctx);
}

static void hashToHex(const uint8_t h[32], char out[65]) {
  static const char* hex = "0123456789abcdef";
  for (int i = 0; i < 32; i++) {
    out[i * 2]     = hex[(h[i] >> 4) & 0x0F];
    out[i * 2 + 1] = hex[h[i] & 0x0F];
  }
  out[64] = 0;
}

// =============================================================
// QUEUE
// =============================================================
static uint16_t qSize() {
  return (uint16_t)((g_q_head - g_q_tail + EVENT_Q_LEN) % EVENT_Q_LEN);
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

// =============================================================
// DEVICE TABLE
// =============================================================
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

static void observeMac(const uint8_t mac[6], int8_t rssi) {
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
    e.last_seen_up_s  = t;
    e.last_rssi       = rssi;
    e.last_touch_up_s = t;
    e.exit_emitted    = false;
    g_totalDetections++;
    return;
  }

  e.last_seen_up_s  = t;
  e.last_rssi       = rssi;
  e.last_touch_up_s = t;
}

// =============================================================
// GPS HELPERS
// =============================================================
static bool hasFreshGps() {
  return g_hasEverFix && ((millis() - g_lastFixMs) <= (GNSS_STALE_SEC * 1000UL));
}

// =============================================================
// EVENT GENERATION
// =============================================================
static void fillEventTimeAndGps(FootfallEvent& ev) {
  uint64_t utc = now_utc_epoch();

  if (utc == 0) {
    ev.timestamp_epoch = up_s();
    strncpy(ev.timestamp_utc, "1970-01-01T00:00:00Z", sizeof(ev.timestamp_utc));
    ev.timestamp_utc[sizeof(ev.timestamp_utc) - 1] = 0;
  } else {
    ev.timestamp_epoch = utc;
    epochToIsoUtc(utc, ev.timestamp_utc);
  }

  if (hasFreshGps()) {
    ev.gps_fix = true;
    ev.lat = g_lastGoodLat;
    ev.lon = g_lastGoodLon;
  } else {
    ev.gps_fix = false;
    ev.lat = 0.0;
    ev.lon = 0.0;
  }
}

static void presenceTick() {
  uint64_t t = up_s();
  uint16_t pushed = 0;

  for (int i = 0; i < MAX_DEVICES && pushed < PRESENCE_MAX_PER_TICK; i++) {
    DeviceEntry& e = g_tab[i];
    if (!e.used) continue;

    uint64_t gap = (t > e.last_seen_up_s) ? (t - e.last_seen_up_s) : 0;
    if (gap > PRESENCE_RECENT_SEC) continue;

    FootfallEvent ev{};
    fillEventTimeAndGps(ev);
    if (!in_campaign(ev.timestamp_epoch)) continue;

    memcpy(ev.mac_hash, e.mac_hash, 32);
    ev.rssi = e.last_rssi;
    ev.dwell_time_sec = 0;
    ev.event_type = EVENT_PRESENCE;

    if (!qPush(ev)) break;
    pushed++;
  }
}

static void dwellSweep() {
  uint64_t t = up_s();

  for (int i = 0; i < MAX_DEVICES; i++) {
    DeviceEntry& e = g_tab[i];
    if (!e.used) continue;

    uint64_t gap = (t > e.last_seen_up_s) ? (t - e.last_seen_up_s) : 0;
    if (gap < EXIT_THRESHOLD_SEC) continue;

    uint32_t dwell = (e.last_seen_up_s > e.first_seen_up_s)
      ? (uint32_t)(e.last_seen_up_s - e.first_seen_up_s) : 0;

    if (dwell >= MIN_DWELL_SEC && !e.exit_emitted) {
      FootfallEvent ev{};
      fillEventTimeAndGps(ev);

      if (in_campaign(ev.timestamp_epoch)) {
        memcpy(ev.mac_hash, e.mac_hash, 32);
        ev.rssi = e.last_rssi;
        ev.dwell_time_sec = dwell;
        ev.event_type = EVENT_DWELL;
        qPush(ev);
      }
      e.exit_emitted = true;
    }

    if (gap >= EXIT_THRESHOLD_SEC * 2) e.used = false;
  }
}

// =============================================================
// WIFI PROBE SNIFFER
// =============================================================
typedef struct __attribute__((packed)) {
  uint16_t frame_ctrl;
  uint16_t duration;
  uint8_t addr1[6];
  uint8_t addr2[6];
  uint8_t addr3[6];
  uint16_t seq_ctrl;
} wifi_ieee80211_hdr_t;

static inline uint8_t fc_type(uint16_t fc) { return (fc >> 2) & 0x3; }
static inline uint8_t fc_sub(uint16_t fc)  { return (fc >> 4) & 0xF; }

static void IRAM_ATTR wifiPromiscCb(void* buf, wifi_promiscuous_pkt_type_t type) {
  if (type != WIFI_PKT_MGMT) return;

  auto* ppkt = (wifi_promiscuous_pkt_t*)buf;
  auto* hdr  = (wifi_ieee80211_hdr_t*)ppkt->payload;

  if (fc_type(hdr->frame_ctrl) != 0 || fc_sub(hdr->frame_ctrl) != 4) return;
  observeMac(hdr->addr2, (int8_t)ppkt->rx_ctrl.rssi);
}

static void snifferStart() {
  if (g_sniffer_on) return;

  WiFi.mode(WIFI_MODE_NULL);
  esp_wifi_set_promiscuous(false);
  esp_wifi_set_promiscuous_rx_cb(&wifiPromiscCb);
  esp_wifi_set_promiscuous(true);

  g_sniffer_on = true;
  Serial.println("[SNIFF] Promiscuous mode ON");
}

static void snifferStop() {
  if (!g_sniffer_on) return;
  esp_wifi_set_promiscuous(false);
  esp_wifi_set_promiscuous_rx_cb(nullptr);
  g_sniffer_on = false;
}

// =============================================================
// BLE SCAN
// =============================================================
class BLECB : public BLEAdvertisedDeviceCallbacks {
  void onResult(BLEAdvertisedDevice dev) override {
    String s = dev.getAddress().toString();
    int v[6];
    uint8_t mac[6];

    if (sscanf(s.c_str(), "%x:%x:%x:%x:%x:%x", &v[0], &v[1], &v[2], &v[3], &v[4], &v[5]) == 6) {
      for (int i = 0; i < 6; i++) mac[i] = (uint8_t)v[i];
      observeMac(mac, (int8_t)dev.getRSSI());
    }
  }
};

static BLECB g_blecb;

// =============================================================
// SPIFFS SPOOL
// =============================================================
static void spoolInit() {
  if (!SPIFFS.begin(true)) {
    Serial.println("[SPOOL] SPIFFS mount failed");
    return;
  }
  Serial.printf("[SPOOL] SPIFFS total=%u used=%u\n", SPIFFS.totalBytes(), SPIFFS.usedBytes());
}

static bool spoolAppend(const String& jsonLine) {
  if (SPIFFS.usedBytes() >= SPOOL_MAX_BYTES) {
    Serial.println("[SPOOL] Full");
    return false;
  }

  File f = SPIFFS.open(SPOOL_PATH, FILE_APPEND);
  if (!f) return false;

  f.println(jsonLine);
  f.close();
  g_totalSpooled++;
  return true;
}

static String spoolReadAll() {
  File f = SPIFFS.open(SPOOL_PATH, FILE_READ);
  if (!f) return "";
  String data = f.readString();
  f.close();
  return data;
}

static void spoolClear() {
  SPIFFS.remove(SPOOL_PATH);
}

static size_t spoolSize() {
  File f = SPIFFS.open(SPOOL_PATH, FILE_READ);
  if (!f) return 0;
  size_t sz = f.size();
  f.close();
  return sz;
}

// =============================================================
// JSON BUILDER
// =============================================================
static String eventTypeToString(EventKind kind) {
  switch (kind) {
    case EVENT_DWELL: return "dwell";
    case EVENT_PRESENCE: return "presence";
    case EVENT_HEARTBEAT: return "heartbeat";
    default: return "presence";
  }
}

static String buildBatchJson(FootfallEvent* evs, uint8_t n) {
  String p;
  p.reserve(3072);

  p += "{\"device_id\":\""; p += DEVICE_ID; p += "\"";
  p += ",\"firmware_version\":\""; p += FW_VERSION; p += "\"";
  p += ",\"events\":[";

  for (uint8_t i = 0; i < n; i++) {
    if (i) p += ",";

    p += "{\"event_type\":\""; p += eventTypeToString(evs[i].event_type); p += "\"";
    p += ",\"count\":1";
    p += ",\"rssi\":"; p += (int)evs[i].rssi;

    if (evs[i].gps_fix) {
      p += ",\"lat\":"; p += String(evs[i].lat, 7);
      p += ",\"lon\":"; p += String(evs[i].lon, 7);
    } else {
      p += ",\"lat\":null,\"lon\":null";
    }

    p += ",\"timestamp\":"; p += String((unsigned long long)evs[i].timestamp_epoch);

    char hx[65];
    hashToHex(evs[i].mac_hash, hx);
    p += ",\"mac_hash\":\""; p += hx; p += "\"";

    p += ",\"dwell_time_sec\":"; p += evs[i].dwell_time_sec;
    p += ",\"campaign_id\":"; p += CAMPAIGN_ID;
    p += ",\"activation_name\":\""; p += ACTIVATION_NAME; p += "\"";
    p += ",\"asset_id\":\""; p += ASSET_ID; p += "\"";
    p += ",\"creative_id\":\""; p += CREATIVE_ID; p += "\"}";
  }

  p += "]}";
  return p;
}

static String buildSingleJson(const FootfallEvent& ev) {
  FootfallEvent arr[1] = { ev };
  return buildBatchJson(arr, 1);
}

// =============================================================
// FORCE DNS via lwIP
// =============================================================
static void forceDnsGoogle() {
  ip_addr_t d;

  IP_ADDR4(&d, 8, 8, 8, 8);
  dns_setserver(0, &d);

  IP_ADDR4(&d, 8, 8, 4, 4);
  dns_setserver(1, &d);

  const ip_addr_t* s0 = dns_getserver(0);
  const ip_addr_t* s1 = dns_getserver(1);

  Serial.printf("[DNS] lwIP forced -> DNS0=%s DNS1=%s\n",
                ipaddr_ntoa(s0), ipaddr_ntoa(s1));
}

// =============================================================
// WIFI + HTTPS
// =============================================================
static void wifiHardDisconnect() {
  WiFi.disconnect(true, true);
  delay(200);
  WiFi.mode(WIFI_MODE_NULL);
  delay(200);
}

static bool wifiConnectOne(const WifiCred& cred, int index) {
  Serial.printf("[WIFI] Trying %s SSID=%s\n", cred.label, cred.ssid);

  wifiHardDisconnect();

  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  WiFi.begin(cred.ssid, cred.pass);

  uint32_t start = millis();
  while (WiFi.status() != WL_CONNECTED && (millis() - start) < WIFI_CONNECT_TIMEOUT_MS) {
    delay(250);
    Serial.print(".");
  }
  Serial.println();

  if (WiFi.status() != WL_CONNECTED) {
    Serial.printf("[WIFI] Failed %s status=%d\n", cred.label, (int)WiFi.status());
    return false;
  }

  Serial.printf("[WIFI] Connected via %s IP=%s GW=%s RSSI=%d\n",
                cred.label,
                WiFi.localIP().toString().c_str(),
                WiFi.gatewayIP().toString().c_str(),
                WiFi.RSSI());

  forceDnsGoogle();
  delay(150);

  IPAddress resolved;
  bool ok = WiFi.hostByName(BACKEND_HOST, resolved);

  Serial.printf("[DNS] resolve %s -> ok=%d ip=%s via %s\n",
                BACKEND_HOST, ok, resolved.toString().c_str(), cred.label);

  if (!ok || resolved == IPAddress(0, 0, 0, 0)) {
    Serial.printf("[DNS] First resolve failed on %s, retrying...\n", cred.label);
    forceDnsGoogle();
    delay(300);
    ok = WiFi.hostByName(BACKEND_HOST, resolved);
    Serial.printf("[DNS] retry -> ok=%d ip=%s\n", ok, resolved.toString().c_str());
  }

  if (!ok || resolved == IPAddress(0, 0, 0, 0)) {
    Serial.printf("[DNS] Failed on %s\n", cred.label);
    wifiHardDisconnect();
    return false;
  }

  g_lastWifiIndex = index;
  return true;
}

static bool wifiConnectSta() {
  for (uint8_t i = 0; i < WIFI_LIST_COUNT; i++) {
    if (wifiConnectOne(WIFI_LIST[i], i)) return true;
  }

  Serial.println("[WIFI] No configured WiFi connected");
  return false;
}

static bool httpPostJson(const String& body) {
  forceDnsGoogle();

  const char* netLabel = (g_lastWifiIndex >= 0 && g_lastWifiIndex < WIFI_LIST_COUNT)
    ? WIFI_LIST[g_lastWifiIndex].label
    : "UNKNOWN_NET";

  WiFiClientSecure secClient;
  secClient.setInsecure();
  secClient.setTimeout(HTTP_READ_TIMEOUT_MS / 1000);

  HTTPClient http;
  http.setReuse(false);
  http.setConnectTimeout(HTTP_CONNECT_TIMEOUT_MS);
  http.setTimeout(HTTP_READ_TIMEOUT_MS);

  Serial.printf("[HTTP] POST %s via %s\n", BACKEND_URL, netLabel);
  Serial.printf("[HTTP] payload_bytes=%u\n", (unsigned)body.length());

  if (!http.begin(secClient, BACKEND_URL)) {
    Serial.println("[HTTP] begin failed");
    return false;
  }

  http.useHTTP10(true);
  http.addHeader("Content-Type", "application/json");
  http.addHeader("Authorization", String("Bearer ") + API_KEY);
  http.addHeader("Connection", "close");
  http.addHeader("User-Agent", "ESP32S3-Footfall/1.0");

  int code = http.POST(body);
  String resp = (code > 0) ? http.getString() : "";
  String err  = (code <= 0) ? http.errorToString(code) : "";
  http.end();

  if (code <= 0) {
    Serial.printf("[HTTP] code=%d err=%s via %s\n", code, err.c_str(), netLabel);
    return false;
  }

  Serial.printf("[HTTP] code=%d via %s resp=%s\n", code, netLabel, resp.c_str());

  if (code >= 200 && code < 300) return true;
  if (code == 401 || code == 403) g_totalRejected++;
  return false;
}

// =============================================================
// SIM FALLBACK SKELETON
// =============================================================
static bool simHttpPostJson(const String& body) {
  (void)body;
  Serial.println("[SIM] Fallback not implemented yet");
  return false;
}

// =============================================================
// UPLOAD BURST
// =============================================================
static bool doUploadBurst() {
  if (qSize() == 0 && spoolSize() == 0) {
    return false;
  }

  snifferStop();

  if (!wifiConnectSta()) {
    Serial.println("[UPLOAD] WiFi unavailable, SIM fallback not active, keeping data in queue/spool");
    wifiHardDisconnect();
    snifferStart();
    return false;
  }

  uint32_t tStart = millis();
  uint8_t posts = 0;
  bool anyOk = false;

  while ((millis() - tStart) < UPLOAD_BURST_MAX_MS && posts < UPLOAD_BURST_MAX_POSTS) {
    FootfallEvent evs[BATCH_MAX_EVENTS];
    uint8_t n = 0;
    FootfallEvent ev;

    while (n < BATCH_MAX_EVENTS && qPop(ev)) {
      evs[n++] = ev;
    }

    if (n == 0) break;

    String body = buildBatchJson(evs, n);
    bool ok = httpPostJson(body);
    posts++;

    if (ok) {
      g_totalUploaded += n;
      anyOk = true;
      Serial.printf("[HTTP] Posted %u events OK\n", n);
    } else {
      for (uint8_t i = 0; i < n; i++) {
        spoolAppend(buildSingleJson(evs[i]));
      }
      break;
    }
  }

  if (anyOk && spoolSize() > 0 && (millis() - tStart) < UPLOAD_BURST_MAX_MS) {
    String data = spoolReadAll();
    if (data.length() > 0) {
      int start = 0;
      bool spoolOk = true;

      while (start < (int)data.length() && spoolOk && (millis() - tStart) < UPLOAD_BURST_MAX_MS) {
        int nl = data.indexOf('\n', start);
        if (nl < 0) nl = data.length();

        String line = data.substring(start, nl);
        line.trim();
        start = nl + 1;

        if (line.length() < 10) continue;

        if (httpPostJson(line)) {
          Serial.println("[SPOOL] Drained 1");
        } else {
          spoolOk = false;
        }
      }

      if (spoolOk) {
        spoolClear();
        Serial.println("[SPOOL] Fully drained");
      }
    }
  }

  wifiHardDisconnect();
  snifferStart();
  return anyOk;
}

// =============================================================
// EC200U GNSS
// =============================================================
static String atCmd(const char* cmd, uint32_t timeoutMs = 1500) {
  String r;
  while (Modem.available()) Modem.read();

  Modem.print(cmd);
  Modem.print("\r\n");

  uint32_t start = millis();
  while (millis() - start < timeoutMs) {
    while (Modem.available()) r += (char)Modem.read();
    if (r.indexOf("\r\nOK\r\n") >= 0 || r.indexOf("+CME ERROR:") >= 0) break;
    delay(10);
  }
  return r;
}

static bool gnssPoll() {
  String r = atCmd("AT+QGPSLOC=2", 4000);

  if (r.indexOf("+CME ERROR: 516") >= 0) {
    Serial.println("[GNSS] no fix yet (+CME 516)");
    g_fix = false;
    return true;
  }

  int p = r.indexOf("+QGPSLOC:");
  if (p < 0) {
    g_fix = false;
    return true;
  }

  int colon = r.indexOf(':', p);
  if (colon < 0) {
    g_fix = false;
    return true;
  }

  String line = r.substring(colon + 1);
  line.trim();

  int f = 0;
  int idx = 0;
  String fields[11];

  while (idx < (int)line.length() && f < 11) {
    int comma = line.indexOf(',', idx);
    if (comma < 0) comma = line.length();
    fields[f++] = line.substring(idx, comma);
    idx = comma + 1;
  }

  if (f < 3) {
    g_fix = false;
    return true;
  }

  double lat = fields[1].toDouble();
  double lon = fields[2].toDouble();

  if (lat == 0.0 && lon == 0.0) {
    g_fix = false;
    return true;
  }

  g_fix = true;
  g_hasEverFix = true;
  g_lat = lat;
  g_lon = lon;
  g_lastGoodLat = lat;
  g_lastGoodLon = lon;
  g_lastFixMs = millis();

  if (f >= 10 && fields[9].length() == 6 && fields[0].length() >= 6) {
    int dd = fields[9].substring(0, 2).toInt();
    int mm = fields[9].substring(2, 4).toInt();
    int yy = fields[9].substring(4, 6).toInt();
    int hh = fields[0].substring(0, 2).toInt();
    int mi = fields[0].substring(2, 4).toInt();
    int ss = (int)fields[0].substring(4, 6).toFloat();

    uint64_t epoch = utcEpochFromFields(2000 + yy, mm, dd, hh, mi, ss);
    if (epoch > 1700000000ULL) {
      g_lastUtc = epoch;
      g_lastUtc_up_s = up_s();
    }
  }

  Serial.printf("[GNSS] fix lat=%.6f lon=%.6f\n", g_lat, g_lon);
  return true;
}

// =============================================================
// STATUS LOG
// =============================================================
static void printStatus() {
  int active = 0;
  uint64_t t = up_s();

  for (int i = 0; i < MAX_DEVICES; i++) {
    if (g_tab[i].used && (t - g_tab[i].last_seen_up_s) <= EXIT_THRESHOLD_SEC) active++;
  }

  Serial.printf(
    "[STATUS] active=%d q=%u det=%u up=%u spool=%u rej=%u gps=%s lat=%.6f lon=%.6f utc=%llu net=%s\n",
    active,
    qSize(),
    g_totalDetections,
    g_totalUploaded,
    g_totalSpooled,
    g_totalRejected,
    hasFreshGps() ? "YES" : "NO",
    hasFreshGps() ? g_lastGoodLat : 0.0,
    hasFreshGps() ? g_lastGoodLon : 0.0,
    (unsigned long long)now_utc_epoch(),
    (g_lastWifiIndex >= 0 && g_lastWifiIndex < WIFI_LIST_COUNT) ? WIFI_LIST[g_lastWifiIndex].label : "NONE"
  );
}

// =============================================================
// SETUP
// =============================================================
void setup() {
  Serial.begin(115200);
  delay(2000);

  Serial.println();
  Serial.println("=========================================");
  Serial.println(" Footfall Pilot Firmware v5");
  Serial.println(" 7SEMI ESP32-S3 + EC200U");
  Serial.println(" Dual WiFi + DNS Fix");
  Serial.println("=========================================");

  loadOrCreateSalt();
  spoolInit();

  Modem.begin(MODEM_BAUD, SERIAL_8N1, MODEM_RX, MODEM_TX);
  delay(500);

  String r = atCmd("AT", 2000);
  Serial.printf("[MODEM] AT -> %s\n", r.c_str());

  Serial.println("[GNSS] configuring...");
  atCmd("AT+QGPSEND", 2000);
  delay(300);
  atCmd("AT+QGPSCFG=\"gnssconfig\",1", 3000);
  atCmd("AT+QGPS=1", 3000);
  delay(500);
  Serial.println("[GNSS] Enabled");

  BLEDevice::init("");
  pBLEScan = BLEDevice::getScan();
  pBLEScan->setAdvertisedDeviceCallbacks(&g_blecb);
  pBLEScan->setActiveScan(false);
  pBLEScan->setInterval(80);
  pBLEScan->setWindow(80);
  Serial.println("[BLE] Scanner ready");

  memset(g_tab, 0, sizeof(g_tab));
  snifferStart();
  Serial.println("[BOOT] System ready - scanning");
}

// =============================================================
// LOOP
// =============================================================
void loop() {
  uint32_t ms = millis();

  if (g_sniffer_on && WIFI_CHANNEL_HOP_ENABLE && (ms - g_lastChanHopMs >= WIFI_CH_HOP_MS)) {
    g_lastChanHopMs = ms;
    g_chan++;
    if (g_chan > WIFI_CH_MAX) g_chan = WIFI_CH_MIN;
    esp_wifi_set_channel(g_chan, WIFI_SECOND_CHAN_NONE);
  }

  if (pBLEScan && (ms - g_lastBleKickMs >= 1100)) {
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
    gnssPoll();
  }

  if ((int32_t)(ms - g_nextUploadMs) >= 0) {
    printStatus();
    bool ok = doUploadBurst();

    if (ok) {
      g_backoffMs = BACKOFF_MIN_MS;
      g_nextUploadMs = millis() + UPLOAD_PERIOD_MS;
    } else {
      g_backoffMs = min((uint32_t)BACKOFF_MAX_MS, g_backoffMs * 2);
      g_nextUploadMs = millis() + g_backoffMs;
    }
  }

  delay(5);
}
