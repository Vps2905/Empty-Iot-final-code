#pragma once
#include <cstdint>
#include <cstring>

enum EventKind : uint8_t {
  EVENT_PRESENCE  = 0,
  EVENT_DWELL     = 1,
  EVENT_HEARTBEAT = 2
};

struct DeviceEntry {
  bool     used;
  uint8_t  mac_hash[32];
  uint64_t first_seen_up_s;
  uint64_t last_seen_up_s;
  int8_t   last_rssi;
  uint64_t last_touch_up_s;
  bool     exit_emitted;
};

struct FootfallEvent {
  uint64_t  timestamp_epoch;
  char      timestamp_utc[24];
  uint8_t   mac_hash[32];
  int8_t    rssi;
  bool      gps_fix;
  double    lat;
  double    lon;
  uint32_t  dwell_time_sec;
  EventKind event_type;
};
