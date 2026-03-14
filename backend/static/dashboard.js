const state = {
  stats: null,
  events: [],
  charts: {},
  refreshTimer: null,
  map: null,
  heatLayer: null,
  markerLayer: null,
};

const dom = {
  totalEvents: document.getElementById('totalEvents'),
  totalCount: document.getElementById('totalCount'),
  uniqueDevices: document.getElementById('uniqueDevices'),
  uniqueMacHashes: document.getElementById('uniqueMacHashes'),
  totalDwell: document.getElementById('totalDwell'),
  latestEventText: document.getElementById('latestEventText'),
  eventsDelta: document.getElementById('eventsDelta'),
  apiHealthBadge: document.getElementById('apiHealthBadge'),
  dbHealthBadge: document.getElementById('dbHealthBadge'),
  lastSyncText: document.getElementById('lastSyncText'),
  deviceFilter: document.getElementById('deviceFilter'),
  eventTypeFilter: document.getElementById('eventTypeFilter'),
  refreshInterval: document.getElementById('refreshInterval'),
  refreshBtn: document.getElementById('refreshBtn'),
  exportBtn: document.getElementById('exportBtn'),
  fitBoundsBtn: document.getElementById('fitBoundsBtn'),
  assetPerformance: document.getElementById('assetPerformance'),
  eventsTableBody: document.getElementById('eventsTableBody'),
  tableMeta: document.getElementById('tableMeta'),
};

function fmtNumber(value) {
  return new Intl.NumberFormat().format(value ?? 0);
}

function fmtTs(ts) {
  if (!ts) return '-';
  const d = new Date(ts * 1000);
  return d.toLocaleString();
}

function truncate(text, max = 14) {
  if (!text) return '-';
  return text.length > max ? `${text.slice(0, max)}…` : text;
}

function eventBadge(type) {
  const cls = `badge badge-${type || 'heartbeat'}`;
  return `<span class="${cls}">${type || '-'}</span>`;
}

function filteredEvents() {
  const device = dom.deviceFilter.value;
  const type = dom.eventTypeFilter.value;
  return state.events.filter(ev => (!device || ev.device_id === device) && (!type || ev.event_type === type));
}

async function fetchJson(url) {
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) throw new Error(`${url} -> ${res.status}`);
  return res.json();
}

async function loadData() {
  try {
    const [health, stats, events] = await Promise.all([
      fetchJson('/api/health'),
      fetchJson('/api/stats'),
      fetchJson('/api/events?limit=500'),
    ]);

    state.stats = stats;
    state.events = Array.isArray(events) ? events : [];

    renderHealth(health);
    syncFilters();
    renderKpis();
    renderCharts();
    renderMap();
    renderAssetPerformance();
    renderTable();
  } catch (err) {
    console.error(err);
    dom.apiHealthBadge.className = 'pill pill-bad';
    dom.apiHealthBadge.textContent = 'Error';
    dom.dbHealthBadge.className = 'pill pill-bad';
    dom.dbHealthBadge.textContent = 'Error';
  }
}

function renderHealth(health) {
  dom.apiHealthBadge.className = health.ok ? 'pill pill-ok' : 'pill pill-bad';
  dom.apiHealthBadge.textContent = health.ok ? 'Online' : 'Offline';
  dom.dbHealthBadge.className = health.database === 'ok' ? 'pill pill-ok' : 'pill pill-bad';
  dom.dbHealthBadge.textContent = health.database || 'Unknown';
  dom.lastSyncText.textContent = new Date().toLocaleTimeString();
}

function syncFilters() {
  const devices = [...new Set(state.events.map(e => e.device_id).filter(Boolean))].sort();
  const current = dom.deviceFilter.value;
  dom.deviceFilter.innerHTML = '<option value="">All devices</option>' + devices.map(d => `<option value="${d}">${d}</option>`).join('');
  dom.deviceFilter.value = devices.includes(current) ? current : '';
}

function renderKpis() {
  const s = state.stats || {};
  const events = filteredEvents();
  dom.totalEvents.textContent = fmtNumber(s.total_events ?? events.length);
  dom.totalCount.textContent = fmtNumber(s.total_count ?? events.reduce((a, b) => a + (b.count || 0), 0));
  dom.uniqueDevices.textContent = fmtNumber(s.unique_devices ?? new Set(events.map(e => e.device_id)).size);
  dom.uniqueMacHashes.textContent = fmtNumber(s.unique_mac_hashes ?? new Set(events.map(e => e.mac_hash).filter(Boolean)).size);
  dom.totalDwell.textContent = `${fmtNumber(s.total_dwell_time_sec ?? events.reduce((a, b) => a + (b.dwell_time_sec || 0), 0))} sec`;

  const latest = s.latest_event || events[0];
  dom.latestEventText.textContent = latest
    ? `${latest.device_id || '-'} • ${latest.event_type || '-'} • ${fmtTs(latest.timestamp)}`
    : 'No data';
  dom.eventsDelta.textContent = `${events.length} rows in current filtered view`;
}

function buildTrendData(events) {
  const sorted = [...events].sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));
  const buckets = new Map();
  sorted.forEach(ev => {
    const ts = ev.timestamp || ev.created_at || 0;
    const d = new Date(ts * 1000);
    d.setSeconds(0, 0);
    const key = d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    buckets.set(key, (buckets.get(key) || 0) + (ev.count || 1));
  });
  return {
    labels: [...buckets.keys()],
    values: [...buckets.values()],
  };
}

function chartFactory(id, config) {
  const ctx = document.getElementById(id);
  if (state.charts[id]) state.charts[id].destroy();
  state.charts[id] = new Chart(ctx, config);
}

function renderCharts() {
  const events = filteredEvents();
  const trend = buildTrendData(events);

  chartFactory('trendChart', {
    type: 'line',
    data: {
      labels: trend.labels,
      datasets: [{
        label: 'Event count',
        data: trend.values,
        tension: 0.35,
        fill: true,
        borderWidth: 2,
      }],
    },
    options: baseChartOptions('Recent Event Trend'),
  });

  const mix = { presence: 0, dwell: 0, heartbeat: 0 };
  events.forEach(ev => mix[ev.event_type] = (mix[ev.event_type] || 0) + 1);
  chartFactory('eventMixChart', {
    type: 'doughnut',
    data: {
      labels: Object.keys(mix),
      datasets: [{ data: Object.values(mix), borderWidth: 0 }],
    },
    options: doughnutOptions(),
  });

  const deviceCounts = {};
  events.forEach(ev => deviceCounts[ev.device_id || 'unknown'] = (deviceCounts[ev.device_id || 'unknown'] || 0) + 1);
  chartFactory('deviceChart', {
    type: 'bar',
    data: {
      labels: Object.keys(deviceCounts),
      datasets: [{ label: 'Events', data: Object.values(deviceCounts), borderWidth: 0 }],
    },
    options: baseChartOptions('Device Throughput', true),
  });

  const dwellBuckets = { '0 sec': 0, '1-5 sec': 0, '6-15 sec': 0, '16-30 sec': 0, '30+ sec': 0 };
  events.forEach(ev => {
    const d = ev.dwell_time_sec || 0;
    if (d === 0) dwellBuckets['0 sec']++;
    else if (d <= 5) dwellBuckets['1-5 sec']++;
    else if (d <= 15) dwellBuckets['6-15 sec']++;
    else if (d <= 30) dwellBuckets['16-30 sec']++;
    else dwellBuckets['30+ sec']++;
  });
  chartFactory('dwellChart', {
    type: 'bar',
    data: {
      labels: Object.keys(dwellBuckets),
      datasets: [{ label: 'Events', data: Object.values(dwellBuckets), borderWidth: 0 }],
    },
    options: baseChartOptions('Dwell Distribution', true),
  });
}

function baseChartOptions(title, horizontal = false) {
  return {
    indexAxis: horizontal ? 'y' : 'x',
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { labels: { color: '#dfe9ff' } },
      title: { display: false, text: title, color: '#dfe9ff' },
    },
    scales: {
      x: { ticks: { color: '#92a0bd' }, grid: { color: 'rgba(255,255,255,0.06)' } },
      y: { ticks: { color: '#92a0bd' }, grid: { color: 'rgba(255,255,255,0.06)' }, beginAtZero: true },
    },
  };
}

function doughnutOptions() {
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { position: 'bottom', labels: { color: '#dfe9ff' } },
    },
  };
}

function initMap() {
  state.map = L.map('heatmap', { zoomControl: true }).setView([17.43388, 78.42669], 13);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; OpenStreetMap contributors',
  }).addTo(state.map);
  state.markerLayer = L.layerGroup().addTo(state.map);
}

function renderMap() {
  if (!state.map) initMap();
  const events = filteredEvents().filter(ev => typeof ev.lat === 'number' && typeof ev.lon === 'number');
  const points = events.map(ev => [ev.lat, ev.lon, Math.max(0.2, Math.min(1, ((ev.count || 1) + 2) / 10))]);

  if (state.heatLayer) state.map.removeLayer(state.heatLayer);
  state.markerLayer.clearLayers();

  if (!points.length) return;
  state.heatLayer = L.heatLayer(points, { radius: 25, blur: 20, maxZoom: 17 }).addTo(state.map);

  const bounds = [];
  events.slice(0, 100).forEach(ev => {
    const popup = `
      <strong>${ev.device_id || '-'}</strong><br/>
      ${ev.event_type || '-'}<br/>
      RSSI: ${ev.rssi ?? '-'}<br/>
      Dwell: ${ev.dwell_time_sec || 0}s<br/>
      ${fmtTs(ev.timestamp)}
    `;
    L.circleMarker([ev.lat, ev.lon], { radius: 5 }).bindPopup(popup).addTo(state.markerLayer);
    bounds.push([ev.lat, ev.lon]);
  });

  if (bounds.length) state.map.fitBounds(bounds, { padding: [20, 20] });
}

function renderAssetPerformance() {
  const events = filteredEvents();
  const grouped = new Map();
  events.forEach(ev => {
    const key = `${ev.asset_id || '-'}|${ev.creative_id || '-'}|${ev.campaign_id || '-'}|${ev.activation_name || '-'}`;
    const current = grouped.get(key) || { asset_id: ev.asset_id || '-', creative_id: ev.creative_id || '-', campaign_id: ev.campaign_id || '-', activation_name: ev.activation_name || '-', events: 0 };
    current.events += ev.count || 1;
    grouped.set(key, current);
  });
  const rows = [...grouped.values()].sort((a, b) => b.events - a.events).slice(0, 8);
  dom.assetPerformance.innerHTML = `
    <div class="list-row header"><div>Asset</div><div>Creative</div><div>Campaign</div><div>Activation</div><div>Events</div></div>
    ${rows.map(r => `<div class="list-row"><div>${r.asset_id}</div><div>${r.creative_id}</div><div>${r.campaign_id}</div><div>${r.activation_name}</div><div>${fmtNumber(r.events)}</div></div>`).join('') || '<div class="list-row"><div>-</div><div>-</div><div>-</div><div>-</div><div>0</div></div>'}
  `;
}

function renderTable() {
  const events = filteredEvents();
  dom.tableMeta.textContent = `${events.length} rows`;
  dom.eventsTableBody.innerHTML = events.map(ev => `
    <tr>
      <td>${ev.id ?? '-'}</td>
      <td>${fmtTs(ev.timestamp || ev.created_at)}</td>
      <td>${ev.device_id || '-'}</td>
      <td>${eventBadge(ev.event_type)}</td>
      <td>${ev.count ?? 1}</td>
      <td>${ev.rssi ?? '-'}</td>
      <td>${ev.lat ?? '-'}</td>
      <td>${ev.lon ?? '-'}</td>
      <td title="${ev.mac_hash || ''}">${truncate(ev.mac_hash || '-')}</td>
      <td>${ev.campaign_id ?? '-'}</td>
      <td>${ev.asset_id || '-'}</td>
      <td>${ev.creative_id || '-'}</td>
      <td>${ev.dwell_time_sec || 0}s</td>
    </tr>
  `).join('');
}

function scheduleRefresh() {
  if (state.refreshTimer) clearInterval(state.refreshTimer);
  state.refreshTimer = setInterval(loadData, Number(dom.refreshInterval.value));
}

function getAdminKey() {
  return window.localStorage.getItem('footfall_admin_key') || '';
}

function bindEvents() {
  dom.refreshBtn.addEventListener('click', loadData);
  dom.deviceFilter.addEventListener('change', () => { renderKpis(); renderCharts(); renderMap(); renderAssetPerformance(); renderTable(); });
  dom.eventTypeFilter.addEventListener('change', () => { renderKpis(); renderCharts(); renderMap(); renderAssetPerformance(); renderTable(); });
  dom.refreshInterval.addEventListener('change', scheduleRefresh);
  dom.fitBoundsBtn.addEventListener('click', () => renderMap());
  dom.exportBtn.addEventListener('click', () => {
    const current = getAdminKey();
    const adminKey = window.prompt('Enter the admin export key', current);
    if (adminKey === null) return;
    window.localStorage.setItem('footfall_admin_key', adminKey);
    window.open(`/api/export.csv?admin_key=${encodeURIComponent(adminKey)}`, '_blank');
  });
}

bindEvents();
scheduleRefresh();
loadData();
