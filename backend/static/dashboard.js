/* ═══════════════════════════════════════════════════════
   Footfall Intelligence — Dashboard JS
   Full pipeline: Overview → Audience → Partners → DSP
   ═══════════════════════════════════════════════════════ */

const S = {
  stats: null,
  events: [],
  pipelineStats: null,
  audienceData: [],
  audienceStats: null,
  partnerExports: [],
  fsqResults: [],
  impressions: [],
  charts: {},
  refreshTimer: null,
  map: null,
  heatLayer: null,
  markerLayer: null,
  activeTab: 'overview',
};

// ─── DOM refs ───
const $ = id => document.getElementById(id);
const dom = {
  totalEvents: $('totalEvents'),
  uniqueMacHashes: $('uniqueMacHashes'),
  audienceCount: $('audienceCount'),
  partnerExports: $('partnerExports'),
  dspImpressions: $('dspImpressions'),
  totalDwell: $('totalDwell'),
  apiHealthBadge: $('apiHealthBadge'),
  dbHealthBadge: $('dbHealthBadge'),
  fsqHealthBadge: $('fsqHealthBadge'),
  lastSyncText: $('lastSyncText'),
  refreshInterval: $('refreshInterval'),
  refreshBtn: $('refreshBtn'),
  exportBtn: $('exportBtn'),
  fitBoundsBtn: $('fitBoundsBtn'),
  deviceFilter: $('deviceFilter'),
  eventTypeFilter: $('eventTypeFilter'),
  eventsTableBody: $('eventsTableBody'),
  tableMeta: $('tableMeta'),
  assetPerformance: $('assetPerformance'),
  // Pipeline
  runPipelineBtn: $('runPipelineBtn'),
  pipelineStatus: $('pipelineStatus'),
  // Audience
  audTotal: $('aud-total'),
  audAvgConf: $('aud-avg-conf'),
  audAvgDwell: $('aud-avg-dwell'),
  audienceTable: $('audienceTable'),
  // Partners
  enrichFsqBtn: $('enrichFsqBtn'),
  pushLrBtn: $('pushLrBtn'),
  pushGtBtn: $('pushGtBtn'),
  partnerActionStatus: $('partnerActionStatus'),
  fsqTable: $('fsqTable'),
  allExportsTable: $('allExportsTable'),
  // DSP
  dspDeviceHash: $('dspDeviceHash'),
  dspCampaignId: $('dspCampaignId'),
  loadAdBtn: $('loadAdBtn'),
  dspAdSlot: $('dspAdSlot'),
  dspRawResponse: $('dspRawResponse'),
  impressionTable: $('impressionTable'),
};

// ─── Utilities ───
const fmt = v => new Intl.NumberFormat().format(v ?? 0);
const fmtTs = ts => { if (!ts) return '-'; const d = new Date(ts * 1000); return d.toLocaleString(); };
const trunc = (t, m=14) => !t ? '-' : t.length > m ? t.slice(0, m) + '…' : t;
const badge = (type, text) => `<span class="badge badge-${type || 'heartbeat'}">${text || type || '-'}</span>`;

async function api(url, opts = {}) {
  const res = await fetch(url, { cache: 'no-store', ...opts });
  if (!res.ok) throw new Error(`${url} → ${res.status}`);
  return res.json();
}

function filteredEvents() {
  const d = dom.deviceFilter?.value || '';
  const t = dom.eventTypeFilter?.value || '';
  return S.events.filter(ev => (!d || ev.device_id === d) && (!t || ev.event_type === t));
}

// ─── Tab navigation ───
document.querySelectorAll('.nav-item[data-tab]').forEach(el => {
  el.addEventListener('click', e => {
    e.preventDefault();
    const tab = el.dataset.tab;
    S.activeTab = tab;
    document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
    el.classList.add('active');
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    const panel = document.getElementById(`tab-${tab}`);
    if (panel) panel.classList.add('active');
    // Lazy-load tab data
    if (tab === 'geospatial') setTimeout(() => renderMap(), 100);
    if (tab === 'audience') loadAudience();
    if (tab === 'partners') loadPartners();
    if (tab === 'pipeline') loadPipeline();
    if (tab === 'dsp') loadImpressions();
  });
});

// ═════════════════════════════════════════════════════════
//  DATA LOADING
// ═════════════════════════════════════════════════════════
async function loadData() {
  try {
    const [health, stats, events] = await Promise.all([
      api('/api/health'),
      api('/api/stats'),
      api('/api/events?limit=500'),
    ]);
    S.stats = stats;
    S.events = Array.isArray(events) ? events : [];
    renderHealth(health);
    syncFilters();
    renderOverviewKpis();
    renderOverviewCharts();
    if (S.activeTab === 'geospatial') renderMap();
    if (S.activeTab === 'events') { renderAssetPerformance(); renderTable(); }
    if (S.activeTab === 'pipeline') loadPipeline();
  } catch (err) {
    console.error(err);
    dom.apiHealthBadge.className = 'pill pill-bad'; dom.apiHealthBadge.textContent = 'Error';
    dom.dbHealthBadge.className = 'pill pill-bad'; dom.dbHealthBadge.textContent = 'Error';
  }
}

async function loadPipeline() {
  try {
    const ps = await api('/api/pipeline/stats');
    S.pipelineStats = ps;
    renderPipeline(ps);
  } catch (e) { console.error('Pipeline stats:', e); }
}

async function loadAudience() {
  try {
    const [candidates, stats] = await Promise.all([
      api('/api/audience?limit=200'),
      api('/api/audience/stats'),
    ]);
    S.audienceData = candidates;
    S.audienceStats = stats;
    renderAudience();
  } catch (e) { console.error('Audience:', e); }
}

async function loadPartners() {
  try {
    const [exports, fsq] = await Promise.all([
      api('/api/partners/exports?limit=200'),
      api('/api/partners/foursquare/results?limit=100'),
    ]);
    S.partnerExports = exports;
    S.fsqResults = fsq;
    renderPartners();
  } catch (e) { console.error('Partners:', e); }
}

async function loadImpressions() {
  try {
    const imps = await api('/api/simulate/dsp/impressions?limit=100');
    S.impressions = imps;
    renderImpressions();
  } catch (e) { console.error('Impressions:', e); }
}

// ═════════════════════════════════════════════════════════
//  RENDER: HEALTH
// ═════════════════════════════════════════════════════════
function renderHealth(h) {
  dom.apiHealthBadge.className = h.ok ? 'pill pill-ok' : 'pill pill-bad';
  dom.apiHealthBadge.textContent = h.ok ? 'Online' : 'Offline';
  dom.dbHealthBadge.className = h.database === 'ok' ? 'pill pill-ok' : 'pill pill-bad';
  dom.dbHealthBadge.textContent = h.database || '?';
  dom.fsqHealthBadge.className = h.foursquare_configured ? 'pill pill-ok' : 'pill pill-warn';
  dom.fsqHealthBadge.textContent = h.foursquare_configured ? 'Active' : 'No Key';
  dom.lastSyncText.textContent = new Date().toLocaleTimeString();
}

function syncFilters() {
  const devices = [...new Set(S.events.map(e => e.device_id).filter(Boolean))].sort();
  const cur = dom.deviceFilter?.value;
  if (dom.deviceFilter) {
    dom.deviceFilter.innerHTML = '<option value="">All devices</option>' + devices.map(d => `<option value="${d}">${d}</option>`).join('');
    dom.deviceFilter.value = devices.includes(cur) ? cur : '';
  }
}

// ═════════════════════════════════════════════════════════
//  RENDER: OVERVIEW
// ═════════════════════════════════════════════════════════
function renderOverviewKpis() {
  const s = S.stats || {};
  dom.totalEvents.textContent = fmt(s.total_events);
  dom.uniqueMacHashes.textContent = fmt(s.unique_mac_hashes);
  dom.audienceCount.textContent = fmt(s.audience_candidates);
  dom.partnerExports.textContent = fmt(s.partner_exports);
  dom.dspImpressions.textContent = fmt(s.mock_impressions);
  dom.totalDwell.textContent = fmt(s.total_dwell_time_sec) + 's';
}

function chartFactory(id, config) {
  const ctx = document.getElementById(id);
  if (!ctx) return;
  if (S.charts[id]) S.charts[id].destroy();
  S.charts[id] = new Chart(ctx, config);
}

function chartOpts(horiz = false) {
  return {
    indexAxis: horiz ? 'y' : 'x',
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { labels: { color: '#7b8aa5', font: { family: "'DM Sans'" } } } },
    scales: {
      x: { ticks: { color: '#5a6882' }, grid: { color: 'rgba(255,255,255,0.04)' } },
      y: { ticks: { color: '#5a6882' }, grid: { color: 'rgba(255,255,255,0.04)' }, beginAtZero: true },
    },
  };
}

function renderOverviewCharts() {
  const events = filteredEvents();
  // Trend
  const sorted = [...events].sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));
  const buckets = new Map();
  sorted.forEach(ev => {
    const d = new Date((ev.timestamp || ev.created_at || 0) * 1000);
    d.setSeconds(0, 0);
    const key = d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    buckets.set(key, (buckets.get(key) || 0) + (ev.count || 1));
  });
  chartFactory('trendChart', {
    type: 'line',
    data: {
      labels: [...buckets.keys()],
      datasets: [{
        label: 'Events', data: [...buckets.values()],
        tension: 0.4, fill: true,
        backgroundColor: 'rgba(56,189,248,0.08)',
        borderColor: '#38bdf8', borderWidth: 2, pointRadius: 1,
      }],
    },
    options: chartOpts(),
  });
  // Mix
  const mix = { presence: 0, dwell: 0, heartbeat: 0 };
  events.forEach(ev => mix[ev.event_type] = (mix[ev.event_type] || 0) + 1);
  chartFactory('eventMixChart', {
    type: 'doughnut',
    data: {
      labels: Object.keys(mix),
      datasets: [{ data: Object.values(mix), borderWidth: 0, backgroundColor: ['#38bdf8', '#818cf8', '#475569'] }],
    },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom', labels: { color: '#7b8aa5' } } } },
  });
}

// ═════════════════════════════════════════════════════════
//  RENDER: PIPELINE
// ═════════════════════════════════════════════════════════
function renderPipeline(ps) {
  if (!ps || !ps.funnel) return;
  const f = ps.funnel;
  const set = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = fmt(v); };
  set('funnel-raw', f.raw_events);
  set('funnel-qualified', f.audience_qualified);
  set('funnel-enriched', f.foursquare_enriched);
  set('funnel-lr', f.liveramp_exported);
  set('funnel-gt', f.groundtruth_exported);
  set('funnel-impressions', f.dsp_impressions);

  // Recent exports table
  const exEl = document.getElementById('recentExportsTable');
  if (exEl && ps.recent_exports && ps.recent_exports.length) {
    let html = '<table><thead><tr><th>Partner</th><th>MAC Hash</th><th>Campaign</th><th>Segment/Audience</th><th>Confidence</th><th>Status</th><th>Time</th></tr></thead><tbody>';
    ps.recent_exports.forEach(r => {
      html += `<tr>
        <td>${badge(r.partner, r.partner)}</td>
        <td title="${r.mac_hash}">${trunc(r.mac_hash)}</td>
        <td>${r.campaign_id || '-'}</td>
        <td>${trunc(r.segment_id || r.audience_id || '-', 20)}</td>
        <td>${r.match_confidence ? (r.match_confidence * 100).toFixed(0) + '%' : '-'}</td>
        <td>${badge(r.status === 'accepted' ? 'presence' : 'heartbeat', r.status)}</td>
        <td>${fmtTs(r.created_at)}</td>
      </tr>`;
    });
    html += '</tbody></table>';
    exEl.innerHTML = html;
  }

  // Confidence chart from audience
  if (ps.recent_audience && ps.recent_audience.length) {
    const confBuckets = { '0-30%': 0, '30-50%': 0, '50-70%': 0, '70-90%': 0, '90-100%': 0 };
    // We'll use the audience stats endpoint instead
    api('/api/audience?limit=500').then(data => {
      data.forEach(r => {
        const c = (r.confidence_score || 0) * 100;
        if (c < 30) confBuckets['0-30%']++;
        else if (c < 50) confBuckets['30-50%']++;
        else if (c < 70) confBuckets['50-70%']++;
        else if (c < 90) confBuckets['70-90%']++;
        else confBuckets['90-100%']++;
      });
      chartFactory('confidenceChart', {
        type: 'bar',
        data: {
          labels: Object.keys(confBuckets),
          datasets: [{ label: 'Candidates', data: Object.values(confBuckets), backgroundColor: '#818cf8', borderWidth: 0, borderRadius: 6 }],
        },
        options: chartOpts(),
      });
    }).catch(() => {});
  }
}

// ═════════════════════════════════════════════════════════
//  RENDER: AUDIENCE
// ═════════════════════════════════════════════════════════
function renderAudience() {
  const st = S.audienceStats || {};
  dom.audTotal.textContent = fmt(st.total);
  dom.audAvgConf.textContent = st.avg_confidence ? (st.avg_confidence * 100).toFixed(1) + '%' : '0%';
  dom.audAvgDwell.textContent = (st.avg_dwell_sec || 0).toFixed(0) + 's';

  const data = S.audienceData;
  if (!data.length) { dom.audienceTable.innerHTML = '<p class="muted-text">No audience candidates yet. Device events with dwell ≥ 5s and valid GPS will appear here.</p>'; return; }

  let html = '<table><thead><tr><th>ID</th><th>MAC Hash</th><th>Campaign</th><th>RSSI</th><th>Dwell</th><th>Confidence</th><th>Repeats</th><th>Status</th><th>Time</th></tr></thead><tbody>';
  data.forEach(r => {
    const confColor = r.confidence_score >= 0.7 ? 'var(--success)' : r.confidence_score >= 0.5 ? 'var(--warning)' : 'var(--danger)';
    html += `<tr>
      <td>${r.id}</td>
      <td title="${r.mac_hash}" class="clickable-hash" data-hash="${r.mac_hash}">${trunc(r.mac_hash, 16)}</td>
      <td>${r.campaign_id || '-'}</td>
      <td>${r.rssi ?? '-'}</td>
      <td>${r.dwell_time_sec || 0}s</td>
      <td><span style="color:${confColor};font-weight:700">${(r.confidence_score * 100).toFixed(0)}%</span></td>
      <td>${r.repeat_count || 1}</td>
      <td>${badge('presence', r.audience_status)}</td>
      <td>${fmtTs(r.created_at)}</td>
    </tr>`;
  });
  html += '</tbody></table>';
  dom.audienceTable.innerHTML = html;

  // Click to copy hash to DSP viewer
  dom.audienceTable.querySelectorAll('.clickable-hash').forEach(el => {
    el.style.cursor = 'pointer';
    el.title = 'Click to copy to DSP viewer';
    el.addEventListener('click', () => {
      dom.dspDeviceHash.value = el.dataset.hash;
      // Switch to DSP tab
      document.querySelector('[data-tab="dsp"]').click();
    });
  });
}

// ═════════════════════════════════════════════════════════
//  RENDER: PARTNERS
// ═════════════════════════════════════════════════════════
function renderPartners() {
  // FSQ table
  if (S.fsqResults.length) {
    let html = '<table><thead><tr><th>MAC Hash</th><th>Place</th><th>Address</th><th>Categories</th><th>Distance</th><th>Time</th></tr></thead><tbody>';
    S.fsqResults.forEach(r => {
      html += `<tr>
        <td title="${r.mac_hash}">${trunc(r.mac_hash)}</td>
        <td>${r.place_name || '-'}</td>
        <td>${trunc(r.place_address || '-', 30)}</td>
        <td>${trunc(r.place_categories || '-', 25)}</td>
        <td>${r.distance_m != null ? r.distance_m + 'm' : '-'}</td>
        <td>${fmtTs(r.created_at)}</td>
      </tr>`;
    });
    html += '</tbody></table>';
    dom.fsqTable.innerHTML = html;
  }

  // All exports table
  if (S.partnerExports.length) {
    let html = '<table><thead><tr><th>ID</th><th>Partner</th><th>MAC Hash</th><th>Campaign</th><th>Segment/Audience</th><th>Match Conf</th><th>Status</th><th>Time</th></tr></thead><tbody>';
    S.partnerExports.forEach(r => {
      html += `<tr>
        <td>${r.id}</td>
        <td>${badge(r.partner, r.partner.replace('_sim', ''))}</td>
        <td title="${r.mac_hash}">${trunc(r.mac_hash)}</td>
        <td>${r.campaign_id || '-'}</td>
        <td>${trunc(r.segment_id || r.audience_id || '-', 22)}</td>
        <td>${r.match_confidence ? (r.match_confidence * 100).toFixed(0) + '%' : '-'}</td>
        <td>${badge(r.status === 'accepted' ? 'presence' : 'heartbeat', r.status)}</td>
        <td>${fmtTs(r.created_at)}</td>
      </tr>`;
    });
    html += '</tbody></table>';
    dom.allExportsTable.innerHTML = html;
  } else {
    dom.allExportsTable.innerHTML = '<p class="muted-text">No partner exports yet. Use the buttons above or run the pipeline.</p>';
  }

  // Breakdown chart
  const counts = {};
  S.partnerExports.forEach(r => { counts[r.partner] = (counts[r.partner] || 0) + 1; });
  if (Object.keys(counts).length) {
    chartFactory('partnerBreakdownChart', {
      type: 'doughnut',
      data: {
        labels: Object.keys(counts).map(k => k.replace('_sim', '')),
        datasets: [{ data: Object.values(counts), borderWidth: 0, backgroundColor: ['#34d399', '#fbbf24', '#38bdf8', '#818cf8'] }],
      },
      options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom', labels: { color: '#7b8aa5' } } } },
    });
  }
}

// ═════════════════════════════════════════════════════════
//  RENDER: DSP
// ═════════════════════════════════════════════════════════
function renderImpressions() {
  if (!S.impressions.length) { dom.impressionTable.innerHTML = '<p class="muted-text">No impressions yet.</p>'; return; }
  let html = '<table><thead><tr><th>Impression</th><th>Device</th><th>Campaign</th><th>Creative</th><th>Partner</th><th>Served</th></tr></thead><tbody>';
  S.impressions.forEach(r => {
    html += `<tr>
      <td>${trunc(r.impression_id, 16)}</td>
      <td title="${r.device_hash}">${trunc(r.device_hash)}</td>
      <td>${r.campaign_id || '-'}</td>
      <td>${r.creative_id || '-'}</td>
      <td>${r.source_partner || '-'}</td>
      <td>${fmtTs(r.served_at)}</td>
    </tr>`;
  });
  html += '</tbody></table>';
  dom.impressionTable.innerHTML = html;
}

// ═════════════════════════════════════════════════════════
//  RENDER: GEOSPATIAL
// ═════════════════════════════════════════════════════════
function initMap() {
  S.map = L.map('heatmap', { zoomControl: true }).setView([17.43388, 78.42669], 13);
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    maxZoom: 19, attribution: '&copy; OpenStreetMap &copy; CARTO',
  }).addTo(S.map);
  S.markerLayer = L.layerGroup().addTo(S.map);
}

function renderMap() {
  if (!S.map) initMap();
  setTimeout(() => S.map.invalidateSize(), 200);
  const events = filteredEvents().filter(ev => typeof ev.lat === 'number' && typeof ev.lon === 'number' && (ev.lat !== 0 || ev.lon !== 0));
  const points = events.map(ev => [ev.lat, ev.lon, Math.max(0.3, Math.min(1, ((ev.count || 1) + 2) / 10))]);
  if (S.heatLayer) S.map.removeLayer(S.heatLayer);
  S.markerLayer.clearLayers();
  if (!points.length) return;
  S.heatLayer = L.heatLayer(points, { radius: 25, blur: 20, maxZoom: 17, gradient: { 0.2: '#0ea5e9', 0.5: '#818cf8', 0.8: '#f472b6', 1: '#f87171' } }).addTo(S.map);
  const bounds = [];
  events.slice(0, 80).forEach(ev => {
    L.circleMarker([ev.lat, ev.lon], { radius: 4, color: '#38bdf8', fillOpacity: 0.7 })
      .bindPopup(`<strong>${ev.device_id || '-'}</strong><br>${ev.event_type}<br>RSSI: ${ev.rssi ?? '-'}<br>Dwell: ${ev.dwell_time_sec || 0}s<br>${fmtTs(ev.timestamp)}`)
      .addTo(S.markerLayer);
    bounds.push([ev.lat, ev.lon]);
  });
  if (bounds.length) S.map.fitBounds(bounds, { padding: [30, 30] });
}

function renderAssetPerformance() {
  const events = filteredEvents();
  const grouped = new Map();
  events.forEach(ev => {
    const key = `${ev.asset_id || '-'}|${ev.creative_id || '-'}|${ev.campaign_id || '-'}|${ev.activation_name || '-'}`;
    const c = grouped.get(key) || { asset_id: ev.asset_id || '-', creative_id: ev.creative_id || '-', campaign_id: ev.campaign_id || '-', activation_name: ev.activation_name || '-', events: 0 };
    c.events += ev.count || 1;
    grouped.set(key, c);
  });
  const rows = [...grouped.values()].sort((a, b) => b.events - a.events).slice(0, 8);
  dom.assetPerformance.innerHTML = `
    <div class="list-row header"><div>Asset</div><div>Creative</div><div>Campaign</div><div>Activation</div><div>Events</div></div>
    ${rows.map(r => `<div class="list-row"><div>${r.asset_id}</div><div>${r.creative_id}</div><div>${r.campaign_id}</div><div>${r.activation_name}</div><div>${fmt(r.events)}</div></div>`).join('') || '<div class="list-row"><div>—</div><div></div><div></div><div></div><div>0</div></div>'}
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
      <td>${badge(ev.event_type)}</td>
      <td>${ev.rssi ?? '-'}</td>
      <td>${ev.lat ?? '-'}</td>
      <td>${ev.lon ?? '-'}</td>
      <td title="${ev.mac_hash || ''}">${trunc(ev.mac_hash || '-')}</td>
      <td>${ev.campaign_id ?? '-'}</td>
      <td>${ev.asset_id || '-'}</td>
      <td>${ev.creative_id || '-'}</td>
      <td>${ev.dwell_time_sec || 0}s</td>
    </tr>
  `).join('');
}

// ═════════════════════════════════════════════════════════
//  ACTIONS
// ═════════════════════════════════════════════════════════
function showStatus(el, msg, type = '') {
  el.style.display = 'block';
  el.className = 'pipeline-status ' + type;
  el.textContent = msg;
}

// Pipeline
dom.runPipelineBtn?.addEventListener('click', async () => {
  dom.runPipelineBtn.disabled = true;
  dom.runPipelineBtn.textContent = 'Running…';
  showStatus(dom.pipelineStatus, '⏳ Running full pipeline: Foursquare → LiveRamp → GroundTruth…');
  try {
    const res = await api('/api/pipeline/run', { method: 'POST' });
    const p = res.pipeline || {};
    let msg = '✅ Pipeline complete!\n\n';
    if (p.foursquare) msg += `Foursquare: enriched ${p.foursquare.enriched ?? 0} candidates\n`;
    if (p.liveramp) msg += `LiveRamp (sim): accepted ${p.liveramp.accepted ?? 0} exports\n`;
    if (p.groundtruth) msg += `GroundTruth (sim): accepted ${p.groundtruth.accepted ?? 0} exports\n`;
    showStatus(dom.pipelineStatus, msg, 'success');
    loadPipeline();
    loadData();
  } catch (e) {
    showStatus(dom.pipelineStatus, '❌ Pipeline failed: ' + e.message, 'error');
  }
  dom.runPipelineBtn.disabled = false;
  dom.runPipelineBtn.textContent = 'Run Full Pipeline';
});

// Partner individual actions
async function partnerAction(url, statusEl) {
  showStatus(statusEl, '⏳ Processing…');
  try {
    const res = await api(url, { method: 'POST' });
    const count = res.enriched ?? res.accepted ?? 0;
    showStatus(statusEl, `✅ Done — ${count} processed`, 'success');
    loadPartners();
    loadPipeline();
  } catch (e) {
    showStatus(statusEl, '❌ ' + e.message, 'error');
  }
}

dom.enrichFsqBtn?.addEventListener('click', () => partnerAction('/api/partners/foursquare/enrich?limit=20', dom.partnerActionStatus));
dom.pushLrBtn?.addEventListener('click', () => partnerAction('/api/simulate/liveramp?limit=50', dom.partnerActionStatus));
dom.pushGtBtn?.addEventListener('click', () => partnerAction('/api/simulate/groundtruth?limit=50', dom.partnerActionStatus));

// DSP ad load
dom.loadAdBtn?.addEventListener('click', async () => {
  const hash = dom.dspDeviceHash.value.trim();
  const camp = dom.dspCampaignId.value.trim();
  if (!hash) { alert('Enter a device_hash'); return; }

  const url = `/api/simulate/dsp/ad?device_hash=${encodeURIComponent(hash)}${camp ? '&campaign_id=' + encodeURIComponent(camp) : ''}`;

  try {
    const data = await api(url);
    dom.dspRawResponse.textContent = JSON.stringify(data, null, 2);

    if (!data.ok || !data.matched) {
      dom.dspAdSlot.className = 'dsp-ad-slot';
      dom.dspAdSlot.innerHTML = `<p style="font-size:18px;font-weight:700;color:var(--warning)">No Match</p><p class="muted-text">This device_hash is not in the retargetable audience. Make sure you've run the pipeline first, and the device has been exported to a partner.</p>`;
      return;
    }

    const ad = data.ad;
    dom.dspAdSlot.className = 'dsp-ad-slot has-ad';
    dom.dspAdSlot.innerHTML = `
      <h2>${ad.ad_title}</h2>
      <p>${ad.ad_body}</p>
      <p class="ad-meta">Campaign: ${ad.campaign_id} · Creative: ${ad.creative_id} · Source: ${ad.source_partner} · Segment: ${ad.source_segment || '-'}</p>
      <a href="${ad.click_url}" target="_blank" rel="noopener">Open landing page →</a>
      <img src="${ad.image_url}" alt="Ad creative" />
    `;
    loadImpressions();
  } catch (e) {
    dom.dspRawResponse.textContent = JSON.stringify({ error: e.message }, null, 2);
  }
});

// Export CSV
dom.exportBtn?.addEventListener('click', () => {
  const stored = localStorage.getItem('footfall_admin_key') || '';
  const key = prompt('Enter admin export key', stored);
  if (key === null) return;
  localStorage.setItem('footfall_admin_key', key);
  window.open(`/api/export.csv?admin_key=${encodeURIComponent(key)}`, '_blank');
});

// Filters
dom.deviceFilter?.addEventListener('change', () => { renderOverviewCharts(); renderTable(); renderAssetPerformance(); });
dom.eventTypeFilter?.addEventListener('change', () => { renderOverviewCharts(); renderTable(); renderAssetPerformance(); });
dom.fitBoundsBtn?.addEventListener('click', () => renderMap());

// Refresh
function scheduleRefresh() {
  if (S.refreshTimer) clearInterval(S.refreshTimer);
  S.refreshTimer = setInterval(loadData, Number(dom.refreshInterval?.value || 10000));
}
dom.refreshBtn?.addEventListener('click', loadData);
dom.refreshInterval?.addEventListener('change', scheduleRefresh);

// ─── Boot ───
scheduleRefresh();
loadData();
