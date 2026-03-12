let map, geoCircle, pin;
let selectedMac = null;
let currentGeofence = null;
let refreshTimer = null;
let loading = false;

function fmt(x, d = 1) {
  if (x === null || x === undefined) return "-";
  if (typeof x === "number") return x.toFixed(d);
  return String(x);
}

function setPill(text, good) {
  const el = document.getElementById("socketPill");
  el.textContent = text;
  el.style.color = good ? "#2dd4bf" : "#fb7185";
}

async function fetchJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return await r.json();
}

function initMap(geo) {
  map = L.map("map").setView([geo.lat, geo.lon], 14);

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19
  }).addTo(map);

  geoCircle = L.circle([geo.lat, geo.lon], { radius: geo.radius_m }).addTo(map);
  pin = L.marker([geo.lat, geo.lon]).addTo(map);
  currentGeofence = geo;
}

function updateMapGeofence(geo) {
  currentGeofence = geo;
  if (!map) {
    initMap(geo);
    return;
  }
  geoCircle.setLatLng([geo.lat, geo.lon]);
  geoCircle.setRadius(geo.radius_m);
}

function updateCards(s) {
  document.getElementById("geofenceLine").textContent =
    `Geofence: ${s.geofence.lat}, ${s.geofence.lon} • Radius: ${s.geofence.radius_m} m`;

  document.getElementById("totalUnique").textContent = s.total_unique ?? 0;
  document.getElementById("activeDevices").textContent = s.active_devices ?? 0;
  document.getElementById("insideCount").textContent = s.inside_geofence ?? 0;
  document.getElementById("avgRssi").textContent = fmt(s.avg_rssi, 1);
  document.getElementById("avgDwell").textContent = fmt(s.avg_dwell, 1);
  document.getElementById("presenceCount").textContent = s.presence_count ?? 0;
  document.getElementById("exitCount").textContent = s.exposure_exit_count ?? 0;
  document.getElementById("exposureCount").textContent = s.exposure_count ?? 0;

  document.getElementById("mapStatus").textContent = "Status: live";
}

function rowHTML(d) {
  return `
    <tr data-mac="${d.mac_hash || ""}">
      <td>${d.mac_hash || "-"}</td>
      <td>${d.received_ts || "-"}</td>
      <td>${d.event_type || "-"}</td>
      <td>${d.signal_source || "-"}</td>
      <td>${d.rssi ?? "-"}</td>
      <td>${d.dwell_time_sec ?? "-"}</td>
      <td>${Number(d.inside_geofence) === 1 ? "YES" : "NO"}</td>
      <td>${Number(d.qualified_exposure) === 1 ? "YES" : "NO"}</td>
      <td>${d.lat ?? "-"}</td>
      <td>${d.lon ?? "-"}</td>
    </tr>
  `;
}

function applyFilters(devs) {
  const q = document.getElementById("search").value.trim().toLowerCase();
  const activeOnly = document.getElementById("activeOnly").checked;
  const inGeoOnly = document.getElementById("inGeoOnly").checked;
  const sort = document.getElementById("sort").value;
  const now = Math.floor(Date.now() / 1000);

  let out = devs.filter(d => d.mac_hash);

  if (q) out = out.filter(d => (d.mac_hash || "").toLowerCase().includes(q));
  if (activeOnly) out = out.filter(d => d.received_ts && (now - d.received_ts) <= 30);
  if (inGeoOnly) out = out.filter(d => Number(d.inside_geofence) === 1);

  if (sort === "rssi") {
    out.sort((a, b) => (b.rssi ?? -999) - (a.rssi ?? -999));
  } else if (sort === "dwell") {
    out.sort((a, b) => (b.dwell_time_sec ?? 0) - (a.dwell_time_sec ?? 0));
  } else {
    out.sort((a, b) => (b.received_ts ?? 0) - (a.received_ts ?? 0));
  }

  return out;
}

async function loadDeviceDetail(macHash) {
  selectedMac = macHash;
  document.getElementById("detailMac").textContent = selectedMac || "none";
  if (!selectedMac) return;

  const rows = await fetchJSON(`/api/device/${encodeURIComponent(selectedMac)}?limit=1`);
  const last = rows[0];
  if (!last) return;

  document.getElementById("dStatus").textContent = "seen";
  document.getElementById("dType").textContent = last.event_type ?? "-";
  document.getElementById("dTs").textContent = last.received_ts ?? "-";
  document.getElementById("dRssi").textContent = last.rssi ?? "-";
  document.getElementById("dDwell").textContent = last.dwell_time_sec ?? "-";
  document.getElementById("dGps").textContent = Number(last.gps_fix) === 1 ? "FIX" : "NO FIX";
  document.getElementById("dGeo").textContent = Number(last.inside_geofence) === 1 ? "YES" : "NO";
  document.getElementById("dQualified").textContent = Number(last.qualified_exposure) === 1 ? "YES" : "NO";
  document.getElementById("dSignalSource").textContent = last.signal_source ?? "-";
  document.getElementById("dLat").textContent = last.lat ?? "-";
  document.getElementById("dLon").textContent = last.lon ?? "-";

  if (last.lat != null && last.lon != null && map) {
    map.setView([last.lat, last.lon], 15);
    pin.setLatLng([last.lat, last.lon]);
  } else if (currentGeofence && map) {
    map.setView([currentGeofence.lat, currentGeofence.lon], 14);
    pin.setLatLng([currentGeofence.lat, currentGeofence.lon]);
  }
}

async function loadAll(forceDetailRefresh = false) {
  if (loading) return;
  loading = true;

  try {
    const stats = await fetchJSON("/api/stats");
    updateCards(stats);
    updateMapGeofence(stats.geofence);

    const devs = await fetchJSON("/api/devices?limit=500");
    const filtered = applyFilters(devs);

    const tbody = document.querySelector("#devTable tbody");
    tbody.innerHTML = filtered.map(rowHTML).join("");

    document.querySelectorAll("#devTable tbody tr").forEach(tr => {
      tr.addEventListener("click", async () => {
        const mac = tr.getAttribute("data-mac");
        await loadDeviceDetail(mac);
      });
    });

    if (forceDetailRefresh && selectedMac) {
      await loadDeviceDetail(selectedMac);
    }
  } catch (err) {
    console.error(err);
  } finally {
    loading = false;
  }
}

function wireUI() {
  document.getElementById("refreshBtn").onclick = () => loadAll(true);

  document.getElementById("copyBtn").onclick = async () => {
    if (!selectedMac) return;
    await navigator.clipboard.writeText(selectedMac);
  };

  document.getElementById("centerBtn").onclick = () => {
    if (geoCircle && map) map.fitBounds(geoCircle.getBounds());
  };

  ["search", "activeOnly", "inGeoOnly", "sort"].forEach(id => {
    const el = document.getElementById(id);
    el.addEventListener("input", () => loadAll(false));
    el.addEventListener("change", () => loadAll(false));
  });
}

function wireSocket() {
  const socket = io();

  socket.on("connect", () => setPill("Socket: connected", true));
  socket.on("disconnect", () => setPill("Socket: disconnected", false));

  socket.on("ingest", () => loadAll(true));
  socket.on("heartbeat", () => loadAll(true));
}

function startPolling() {
  if (refreshTimer) clearInterval(refreshTimer);
  refreshTimer = setInterval(() => loadAll(false), 5000);
}

(async function () {
  wireUI();
  wireSocket();
  await loadAll(false);
  startPolling();
})();