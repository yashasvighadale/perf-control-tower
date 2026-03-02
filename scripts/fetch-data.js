#!/usr/bin/env node
// Daily Meta Ads snapshot fetcher.
// Usage:
//   node scripts/fetch-data.js                        # fetches yesterday
//   node scripts/fetch-data.js --date 2026-03-01      # backfill a specific date
//
// Required env vars:
//   META_TOKEN    — Meta access token (ads_read + read_insights)
//   ACCOUNT_ID    — Ad account ID (numbers only, no "act_" prefix)
// Optional:
//   SHEET_CSV_URL — Google Sheet CSV URL for creative taxonomy

'use strict';
const { writeFileSync, readFileSync, mkdirSync, existsSync } = require('fs');
const { join } = require('path');

const TOKEN      = process.env.META_TOKEN;
const ACCOUNT_ID = process.env.ACCOUNT_ID;
const CSV_URL    = process.env.SHEET_CSV_URL || '';
const API_VER    = 'v21.0';

// ── Date helpers ────────────────────────────────────────────
function yesterday() {
  const d = new Date();
  d.setDate(d.getDate() - 1);
  return d.toISOString().split('T')[0];
}

function parseArgs() {
  const idx = process.argv.indexOf('--date');
  if (idx !== -1 && process.argv[idx + 1]) return process.argv[idx + 1];
  return yesterday();
}

// ── Meta Graph API ───────────────────────────────────────────
async function metaPaginateAll(url) {
  const all = [];
  let next = url;
  let page = 0;
  while (next && page < 20) {
    const res = await fetch(next);
    const json = await res.json();
    if (json.error) throw new Error('Meta API: ' + json.error.message);
    if (Array.isArray(json.data)) all.push(...json.data);
    next = json.paging?.next || null;
    page++;
    if (next) process.stdout.write(`  page ${page + 1}...\r`);
  }
  return all;
}

async function fetchMetaAdLevel(date) {
  const base = `https://graph.facebook.com/${API_VER}/act_${ACCOUNT_ID}/insights`;
  const tok  = encodeURIComponent(TOKEN);
  const tr   = encodeURIComponent(JSON.stringify({ since: date, until: date }));
  const fields = encodeURIComponent(
    'ad_id,ad_name,impressions,spend,clicks,cpc,ctr,cpp,reach,frequency,purchase_roas'
  );
  const url = `${base}?access_token=${tok}&fields=${fields}&level=ad&limit=500&time_range=${tr}`;
  return metaPaginateAll(url);
}

// ── Google Sheets CSV ────────────────────────────────────────
function parseCsv(text) {
  const lines = text.trim().split(/\r?\n/);
  if (lines.length < 2) return [];
  function parseRow(line) {
    const r = []; let cur = ''; let inQ = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (c === '"') { if (inQ && line[i + 1] === '"') { cur += '"'; i++; } else inQ = !inQ; }
      else if (c === ',' && !inQ) { r.push(cur.trim()); cur = ''; }
      else cur += c;
    }
    r.push(cur.trim()); return r;
  }
  const headers = parseRow(lines[0]).map(h => h.toLowerCase().trim());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const vals = parseRow(lines[i]);
    if (vals.every(v => !v)) continue;
    const row = {}; headers.forEach((h, idx) => { row[h] = vals[idx] || ''; });
    rows.push(row);
  }
  return rows;
}

async function fetchSheetData() {
  if (!CSV_URL) return [];
  const url = CSV_URL + (CSV_URL.includes('?') ? '&' : '?') + '_t=' + Date.now();
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Google Sheets fetch failed (HTTP ${res.status})`);
  const text = await res.text();
  if (text.trim().startsWith('<')) throw new Error('Google Sheets returned HTML — check URL');
  return parseCsv(text);
}

// ── Data normalization ───────────────────────────────────────
const TAXONOMY_DEFAULTS = {
  visual_style: 'Unknown', hook: 'Unknown', tone: 'Unknown',
  narrative: 'Unknown', vo: 'Unknown', production: 'Unknown',
  format: 'Unknown', funnel: 'Unknown', product: 'Unknown',
  created_by: 'Unknown', offer: 'Unknown', prices: 'Unknown',
  rtb: '', starring: '', language: '', season: ''
};

function normalizeMetaAd(raw) {
  const adName = (raw.ad_name || '').trim();
  const match  = adName.match(/NK-\d+/i);
  const roasEntry = (raw.purchase_roas || []).find(r => r.action_type === 'omni_purchase');
  const roas = roasEntry ? parseFloat(roasEntry.value) || 0 : 0;
  return {
    ad_id:       raw.ad_id || '',
    ad_code:     match ? match[0].toUpperCase() : adName,
    ad_name:     adName,
    spend:       parseFloat(raw.spend)      || 0,
    clicks:      parseInt(raw.clicks)       || 0,
    impressions: parseInt(raw.impressions)  || 0,
    ctr:         parseFloat(raw.ctr)        || 0,
    cpc:         parseFloat(raw.cpc)        || 0,
    cpp:         parseFloat(raw.cpp)        || 0,
    reach:       parseInt(raw.reach)        || 0,
    frequency:   parseFloat(raw.frequency)  || 0,
    cpv:         0,
    roas
  };
}

function mergeData(metaAds, sheetRows) {
  const map = {};
  sheetRows.forEach(r => { const k = (r.ad_code || '').trim(); if (k) map[k] = r; });
  return metaAds.map(raw => {
    const meta  = normalizeMetaAd(raw);
    const sheet = map[meta.ad_code] || {};
    return { ...TAXONOMY_DEFAULTS, ...sheet, ...meta, ad_code: meta.ad_code };
  });
}

// ── data/index.json management ───────────────────────────────
function loadIndex(indexPath) {
  if (!existsSync(indexPath)) return { dates: [] };
  try { return JSON.parse(readFileSync(indexPath, 'utf8')); }
  catch { return { dates: [] }; }
}

function saveIndex(indexPath, index) {
  index.dates = [...new Set(index.dates)].sort();
  writeFileSync(indexPath, JSON.stringify(index, null, 2));
}

// ── Main ─────────────────────────────────────────────────────
async function main() {
  if (!TOKEN)      { console.error('❌  META_TOKEN env var is required'); process.exit(1); }
  if (!ACCOUNT_ID) { console.error('❌  ACCOUNT_ID env var is required'); process.exit(1); }

  const date = parseArgs();
  console.log(`📅 Fetching Meta Ads data for ${date}…`);

  // Fetch Meta ad-level data
  console.log('  Fetching ad-level insights…');
  const adsRaw = await fetchMetaAdLevel(date);
  console.log(`  ✓ ${adsRaw.length} ad records from Meta`);

  // Fetch Google Sheet creative taxonomy
  let sheetRows = [];
  if (CSV_URL) {
    console.log('  Fetching creative taxonomy from Google Sheets…');
    try {
      sheetRows = await fetchSheetData();
      console.log(`  ✓ ${sheetRows.length} rows from Google Sheets`);
    } catch (err) {
      console.warn(`  ⚠ Google Sheets fetch failed: ${err.message} — continuing without taxonomy`);
    }
  } else {
    console.log('  ℹ No SHEET_CSV_URL set — skipping creative taxonomy');
  }

  // Merge
  const ads = mergeData(adsRaw, sheetRows);
  console.log(`  ✓ Merged ${ads.length} ads`);

  // Write snapshot
  const root         = join(__dirname, '..');
  const snapshotsDir = join(root, 'data', 'snapshots');
  const indexPath    = join(root, 'data', 'index.json');
  mkdirSync(snapshotsDir, { recursive: true });

  const snapshot = {
    date,
    fetched_at: new Date().toISOString(),
    ads
  };
  const snapshotPath = join(snapshotsDir, `${date}.json`);
  writeFileSync(snapshotPath, JSON.stringify(snapshot));
  console.log(`  ✓ Written ${snapshotPath}`);

  // Update index.json
  const index = loadIndex(indexPath);
  if (!index.dates.includes(date)) index.dates.push(date);
  saveIndex(indexPath, index);
  console.log(`  ✓ Updated data/index.json (${index.dates.length} dates total)`);

  console.log(`✅ Done — snapshot saved for ${date}`);
}

main().catch(err => {
  console.error('❌ Fatal error:', err.message);
  process.exit(1);
});
