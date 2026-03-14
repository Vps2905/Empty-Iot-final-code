#include <WiFi.h>
#include <HTTPClient.h>
#include <ArduinoJson.h>

static const char* DEVICE_ID   = "FF-001";
static const char* WIFI_SSID   = "YOUR_WIFI_NAME";
static const char* WIFI_PASS   = "YOUR_WIFI_PASSWORD";
static const char* BACKEND_URL = "https://your-app.up.railway.app/ingest";
static const char* API_KEY     = "ff_ingest_change_me";
static const char* FW_VERSION  = "pilot-v3";

unsigned long lastPostMs = 0;
int fakeCounter = 0;

void ensureWifi() {
  if (WiFi.status() == WL_CONNECTED) return;
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  unsigned long start = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - start < 20000) {
    delay(500);
    Serial.print(".");
  }
  Serial.println();
  if (WiFi.status() == WL_CONNECTED) {
    Serial.print("[WIFI] connected IP=");
    Serial.println(WiFi.localIP());
  } else {
    Serial.println("[WIFI] connect failed");
  }
}

bool postEvent() {
  if (WiFi.status() != WL_CONNECTED) return false;

  HTTPClient http;
  http.begin(BACKEND_URL);
  http.addHeader("Content-Type", "application/json");
  http.addHeader("Authorization", String("Bearer ") + API_KEY);

  DynamicJsonDocument doc(512);
  doc["device_id"] = DEVICE_ID;
  doc["firmware_version"] = FW_VERSION;
  JsonArray events = doc.createNestedArray("events");
  JsonObject evt = events.createNestedObject();
  evt["event_type"] = "presence";
  evt["count"] = 1;
  evt["rssi"] = -62;
  evt["lat"] = 17.43388;
  evt["lon"] = 78.42669;
  evt["timestamp"] = 0;

  String body;
  serializeJson(doc, body);
  int code = http.POST(body);
  String resp = http.getString();
  http.end();

  Serial.printf("[UPLOAD] code=%d resp=%s\n", code, resp.c_str());
  return code > 0 && code < 300;
}

void setup() {
  Serial.begin(115200);
  delay(1000);
  Serial.println("\n=== FOOTFALL PILOT V3 ===");
  ensureWifi();
}

void loop() {
  ensureWifi();

  if (millis() - lastPostMs > 15000) {
    lastPostMs = millis();
    fakeCounter++;
    Serial.printf("[LOOP] sending sample event %d\n", fakeCounter);
    postEvent();
  }

  delay(200);
}
