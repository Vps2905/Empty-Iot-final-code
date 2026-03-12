let map, geoCircle, pin;
let selectedMac = null;

function fmt(x, d=1){
  if (x === null || x === undefined) return "-";
  if (typeof x === "number") return x.toFixed(d);
  return String(x);
}

function setPill(text, good){
  const el = document.getElementById("socketPill");
  el.textContent = text;
  el.style.color = good ? "#2dd4bf" : "#fb7185";
}

async function fetchJSON(url){
  const r = await fetch(url);
  return await r.json();
}

function initMap(geo){
  map = L.map('map').setView([geo.lat, geo.lon], 14);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19
  }).addTo(map);

  geoCircle = L.circle([geo.lat, geo.lon], {radius: geo.radius_m}).addTo(map);
  pin = L.marker([geo.lat, geo.lon]).addTo(map);
}

function updateCards(s){
  document.getElementById("geofenceLine").textContent =
    `Geofence: ${s.geofence.lat}, ${s.geofence.lon} • Radius: ${s.geofence.radius_m} m`;

  document.getElementById("totalUnique").textContent = s.total_unique;
  document.getElementById("activeDevices").textContent = s.active_devices;
  document.getElementById("insideCount").textContent = s.inside_geofence;
  document.getElementById("avgRssi").textContent = fmt(s.avg_rssi, 1);
  document.getElementById("avgDwell").textContent = fmt(s.avg_dwell, 1);
  document.getElementById("exitCount").textContent = s.exit_count;
  document.getElementById("exposureCount").textContent = s.exposure_count;
}

function rowHTML(d){
  return `
    <tr data-mac="${d.mac_hash || ''}">
      <td>${d.mac_hash || '-'}</td>
      <td>${d.received_ts || '-'}</td>
      <td>${d.rssi ?? '-'}</td>
      <td>${d.dwell_time_sec ?? '-'}</td>
      <td>${d.lat ?? '-'}</td>
      <td>${d.lon ?? '-'}</td>
    </tr>
  `;
}

function applyFilters(devs, stats){
  const q = document.getElementById("search").value.trim().toLowerCase();
  const activeOnly = document.getElementById("activeOnly").checked;
  const inGeoOnly = document.getElementById("inGeoOnly").checked;
  const sort = document.getElementById("sort").value;

  const now = Math.floor(Date.now()/1000);

  function inGeo(d){
    if (d.lat === null || d.lon === null || d.lat === undefined || d.lon === undefined) return false;
    return true;
  }

  let out = devs.filter(d => d.mac_hash);

  if (q) out = out.filter(d => d.mac_hash.toLowerCase().includes(q));
  if (activeOnly) out = out.filter(d => d.received_ts && (now - d.received_ts) <= 30);
  if (inGeoOnly) out = out.filter(d => inGeo(d));

  if (sort === "rssi") out.sort((a,b)=>(b.rssi??-999)-(a.rssi??-999));
  else if (sort === "dwell") out.sort((a,b)=>(b.dwell_time_sec??0)-(a.dwell_time_sec??0));
  else out.sort((a,b)=>(b.received_ts??0)-(a.received_ts??0));

  return out;
}

async function loadAll(){
  const stats = await fetchJSON("/api/stats");
  updateCards(stats);

  if (!map) initMap(stats.geofence);

  const devs = await fetchJSON("/api/devices");
  const filtered = applyFilters(devs, stats);

  const tbody = document.querySelector("#devTable tbody");
  tbody.innerHTML = filtered.map(rowHTML).join("");

  document.querySelectorAll("#devTable tbody tr").forEach(tr=>{
    tr.addEventListener("click", async ()=>{
      selectedMac = tr.getAttribute("data-mac");
      document.getElementById("detailMac").textContent = selectedMac || "none";
      if (!selectedMac) return;

      const rows = await fetchJSON(`/api/device/${selectedMac}`);
      const last = rows[0];
      if (!last) return;

      document.getElementById("dStatus").textContent = "seen";
      document.getElementById("dTs").textContent = last.received_ts ?? "-";
      document.getElementById("dRssi").textContent = last.rssi ?? "-";
      document.getElementById("dDwell").textContent = last.dwell_time_sec ?? "-";
      document.getElementById("dGps").textContent = (last.lat!=null && last.lon!=null) ? "FIX" : "NO FIX";
      document.getElementById("dLat").textContent = last.lat ?? "-";
      document.getElementById("dLon").textContent = last.lon ?? "-";

      if (last.lat != null && last.lon != null) {
        map.setView([last.lat, last.lon], 15);
        pin.setLatLng([last.lat, last.lon]);
      }
    });
  });
}

function wireUI(){
  document.getElementById("refreshBtn").onclick = loadAll;
  document.getElementById("copyBtn").onclick = async ()=>{
    if (!selectedMac) return;
    await navigator.clipboard.writeText(selectedMac);
  };
  document.getElementById("centerBtn").onclick = ()=>{
    if (geoCircle) map.fitBounds(geoCircle.getBounds());
  };

  ["search","activeOnly","inGeoOnly","sort"].forEach(id=>{
    const el = document.getElementById(id);
    el.addEventListener("input", loadAll);
    el.addEventListener("change", loadAll);
  });
}

function wireSocket(){
  const socket = io();
  socket.on("connect", ()=> setPill("Socket: connected", true));
  socket.on("disconnect", ()=> setPill("Socket: disconnected", false));
  socket.on("ingest", (msg)=>{
    // Update frontend cards dynamically without triggering heavy API calls
    if (msg && msg.stats) {
      updateCards(msg.stats);
    }
  });
}

(async function(){
  wireUI();
  wireSocket();
  await loadAll();
  // Safe, controlled polling every 5 seconds
  setInterval(()=>loadAll().catch(()=>{}), 5000);
})();