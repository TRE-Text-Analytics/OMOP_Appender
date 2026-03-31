/**
 * OMOP Merge Tool — Frontend Application
 *
 * Organised into sections:
 *  1. State
 *  2. Navigation
 *  3. Connections
 *  4. Table selector
 *  5. Conflict rules
 *  6. Scan
 *  7. Run / merge
 *  8. Utilities
 *  9. Init
 */

'use strict';

/* ==========================================================================
   1. State
   ========================================================================== */

const S = {
  /** Table metadata fetched from /api/tables */
  tables: {},
  /** Set of selected table names */
  selected: new Set(),
  /** Last scan_complete event payload */
  scanResult: null,
  /** ID mapping rows from the most recent merge run */
  mapData: [],
};

const DEFAULT_SELECTED = [
  'person', 'death', 'visit_occurrence',
  'condition_occurrence', 'drug_exposure', 'measurement', 'observation',
];

const DOMAIN_LABELS = {
  core:    'Core person',
  visit:   'Visits',
  clinical:'Clinical events',
  derived: 'Derived / ERA tables',
};
const DOMAIN_ORDER = ['core', 'visit', 'clinical', 'derived'];

/* ==========================================================================
   2. Navigation
   ========================================================================== */

function goStep(n) {
  document.querySelectorAll('.panel').forEach((el, i) => el.classList.toggle('active', i === n));
  document.querySelectorAll('.nav-item').forEach((el, i) => {
    el.classList.toggle('active', i === n);
    if (i < n) el.classList.add('done');
  });
  if (n === 4) updateRunSummary();
}

/* ==========================================================================
   3. Connections
   ========================================================================== */

/** Build a DBConfig object from the form fields for the given side ('src'|'tgt'). */
function getDbConfig(side) {
  return {
    host:        document.getElementById(`${side}-host`).value.trim(),
    port:        parseInt(document.getElementById(`${side}-port`).value, 10),
    database:    document.getElementById(`${side}-db`).value.trim(),
    schema_name: document.getElementById(`${side}-schema`).value.trim(),
    username:    document.getElementById(`${side}-user`).value.trim(),
    password:    document.getElementById(`${side}-pass`).value,
  };
}

async function testConn(side) {
  const statusEl = document.getElementById(`${side}-cs`);
  statusEl.className = 'cs pend';
  statusEl.textContent = 'Testing…';
  try {
    const res  = await postJSON('/api/test-connection', { config: getDbConfig(side) });
    const data = await res.json();
    if (data.ok) {
      statusEl.className = 'cs ok';
      statusEl.textContent = 'Connected ✓';
    } else {
      statusEl.className = 'cs err';
      statusEl.textContent = data.error || 'Failed';
    }
  } catch {
    statusEl.className = 'cs err';
    statusEl.textContent = 'Unreachable';
  }
}

/* ==========================================================================
   4. Table selector
   ========================================================================== */

async function initTables() {
  try {
    const res = await fetch('/api/tables');
    S.tables = await res.json();
  } catch {
    S.tables = {};
  }
  DEFAULT_SELECTED.forEach(t => S.selected.add(t));
  renderTables();
}

function renderTables() {
  const byDomain = {};
  for (const [name, meta] of Object.entries(S.tables)) {
    (byDomain[meta.domain] ??= []).push({ name, ...meta });
  }

  document.getElementById('table-sel').innerHTML = DOMAIN_ORDER.map(domain => {
    const tables = byDomain[domain] ?? [];
    if (!tables.length) return '';

    const cards = tables.map(t => {
      const selected = S.selected.has(t.name);
      return `
        <div class="tcard ${selected ? 'sel' : ''}" id="tc-${t.name}" onclick="toggleTable('${t.name}')">
          <input type="checkbox" ${selected ? 'checked' : ''}
                 onclick="event.stopPropagation(); toggleTable('${t.name}')">
          <div>
            <div class="tname">${t.name}</div>
            <div class="tdesc">${t.description}</div>
          </div>
        </div>`;
    }).join('');

    return `
      <div class="domain-group">
        <div class="dlabel">${DOMAIN_LABELS[domain] ?? domain}</div>
        <div class="tgrid">${cards}</div>
      </div>`;
  }).join('');
}

function toggleTable(name) {
  S.selected.has(name) ? S.selected.delete(name) : S.selected.add(name);
  const card = document.getElementById(`tc-${name}`);
  if (!card) return;
  card.classList.toggle('sel', S.selected.has(name));
  card.querySelector('input').checked = S.selected.has(name);
}

function selAll(on) {
  for (const name of Object.keys(S.tables)) {
    on ? S.selected.add(name) : S.selected.delete(name);
    const card = document.getElementById(`tc-${name}`);
    if (card) {
      card.classList.toggle('sel', on);
      card.querySelector('input').checked = on;
    }
  }
}

/* ==========================================================================
   5. Conflict rules
   ========================================================================== */

function selConflict(value) {
  for (const v of ['skip', 'upsert', 'abort']) {
    const el = document.getElementById(`co-${v}`);
    el.classList.toggle('sel', v === value);
    el.querySelector('input').checked = v === value;
  }
}

function toggleOffsetRow(strategy) {
  document.getElementById('off-row').classList.toggle('hidden', strategy !== 'offset');
}

/* ==========================================================================
   6. Scan
   ========================================================================== */

async function startScan() {
  const btn    = document.getElementById('btn-scan');
  const inline = document.getElementById('scan-inline');
  btn.disabled = true;
  inline.classList.remove('hidden');
  document.getElementById('scan-results').classList.add('hidden');

  const tableProgress = {};

  try {
    const stream = await postJSON('/api/scan', {
      source: getDbConfig('src'),
      target: getDbConfig('tgt'),
      tables: [...S.selected],
    });

    await readNDJSON(stream, ev => {
      if (ev.type === 'table_scan') {
        tableProgress[ev.table] = ev;
        renderTableProgress(tableProgress);
      } else if (ev.type === 'scan_complete') {
        S.scanResult = ev;
        renderScanComplete(ev);
      }
    });
  } catch (err) {
    console.error('Scan failed:', err);
  }

  btn.disabled = false;
  inline.classList.add('hidden');
}

function renderTableProgress(progress) {
  document.getElementById('scan-results').classList.remove('hidden');

  document.getElementById('table-results').innerHTML = Object.entries(progress).map(([tbl, ev]) => `
    <div class="tresult">
      <span class="tn">${tbl}</span>
      ${ev.missing
        ? '<span class="tskip">not found in DB</span>'
        : `<span class="trows">${ev.new_rows.toLocaleString()} new rows</span>
           <span class="tpats">${ev.affected_patients.toLocaleString()} patients</span>`}
    </div>`).join('');
}

function renderScanComplete(ev) {
  const patients = ev.patients ?? [];
  const newPats  = patients.filter(p => p.is_new_patient).length;

  // Summary stats
  document.getElementById('sc-total').textContent    = patients.length > 0
    ? Math.max(...Object.values(ev.table_totals ?? {}).map(t => t.affected_patients ?? 0))
    : '—';
  document.getElementById('sc-affected').textContent = ev.total_patients_with_new_data;
  document.getElementById('sc-rows').textContent     = ev.total_new_rows.toLocaleString();
  document.getElementById('sc-newpats').textContent  = newPats;

  document.getElementById('pat-count-label').textContent =
    `${patients.length} patient${patients.length !== 1 ? 's' : ''}`;

  // Patient table
  const rows = patients.slice(0, 200).map(p => {
    const tag = p.is_new_patient
      ? '<span class="new-tag">new</span>'
      : '<span class="exist-tag">existing</span>';
    return `<tr>
      <td>${p.source_person_id}</td>
      <td class="text-muted">${p.source_value ?? '—'}</td>
      <td>${tag}</td>
      <td class="align-right" style="color:var(--green)">${p.total_new_rows.toLocaleString()}</td>
    </tr>`;
  });

  if (patients.length > 200) {
    rows.push(`<tr><td colspan="4" style="color:var(--text3);font-style:italic">
      … and ${patients.length - 200} more
    </td></tr>`);
  }

  document.getElementById('pat-tbody').innerHTML = rows.join('');

  // Unlock the proceed button if there is anything to do
  const proceedBtn = document.getElementById('btn-to-run');
  proceedBtn.disabled = ev.total_new_rows === 0;
}

/* ==========================================================================
   7. Run / merge
   ========================================================================== */

function updateMode() {
  const dry    = document.getElementById('dry-tog').checked;
  const banner = document.getElementById('mode-banner');
  const badge  = document.getElementById('run-badge');

  if (dry) {
    banner.className   = 'mode-banner';
    banner.textContent = '⬡ Dry-run — no data will be written to the target';
    badge.className    = 'run-badge show';
    badge.textContent  = 'dry-run';
  } else {
    banner.className   = 'mode-banner live';
    banner.textContent = '⚠ Live mode — changes will be committed to target';
    badge.className    = 'run-badge live show';
    badge.textContent  = 'live';
  }
}

function updateRunSummary() {
  const sr = S.scanResult;
  document.getElementById('r-pats').textContent   = sr ? sr.total_patients_with_new_data : '—';
  document.getElementById('r-tables').textContent = S.selected.size;
  document.getElementById('r-rows').textContent   = sr ? sr.total_new_rows.toLocaleString() : '—';
  document.getElementById('r-conf').textContent   = '—';
  updateMode();
}

function appendLogLine(message, level = '') {
  const el  = document.getElementById('run-log');
  const ts  = new Date().toTimeString().slice(0, 8);
  const row = document.createElement('div');
  row.className = 'll';
  row.innerHTML = `<span class="lts">${ts}</span><span class="lm ${level}">${message}</span>`;
  el.appendChild(row);
  el.scrollTop = el.scrollHeight;
}

async function startRun() {
  const dry = document.getElementById('dry-tog').checked;

  const mergeConfig = {
    source:           getDbConfig('src'),
    target:           getDbConfig('tgt'),
    tables:           [...S.selected],
    person_conflict:  document.querySelector('input[name="pc"]:checked')?.value ?? 'skip',
    dedup_enabled:    document.getElementById('dedup-tog').checked,
    id_strategy:      document.getElementById('id-strat').value,
    id_offset:        parseInt(document.getElementById('id-off').value, 10) || 0,
    dry_run:          dry,
  };

  // Reset UI
  document.getElementById('run-log').innerHTML = '';
  setProgressBar(0);
  document.getElementById('rpbar').classList.remove('done', 'err');
  document.getElementById('btn-run').disabled = true;
  document.getElementById('map-section').classList.add('hidden');
  document.getElementById('btn-export').classList.add('hidden');

  appendLogLine(`Starting ${dry ? 'dry run' : 'live merge'} across ${S.selected.size} tables…`);

  try {
    const stream = await postJSON('/api/merge', mergeConfig);
    await readNDJSON(stream, handleRunEvent);
  } catch (err) {
    appendLogLine(`Network error: ${err.message}`, 'err');
  }

  document.getElementById('btn-run').disabled = false;
}

function handleRunEvent(ev) {
  switch (ev.type) {
    case 'log':
      appendLogLine(ev.msg, ev.level ?? '');
      break;

    case 'progress':
      setProgressBar(Math.round((ev.step / ev.total) * 100));
      break;

    case 'summary': {
      setProgressBar(100);
      document.getElementById('rpbar').classList.add('done');
      document.getElementById('r-rows').textContent = ev.inserted.toLocaleString();
      document.getElementById('r-conf').textContent = ev.conflicts;
      S.mapData = ev.mapping ?? [];
      if (S.mapData.length > 0) {
        renderMapTable();
        document.getElementById('map-section').classList.remove('hidden');
        document.getElementById('btn-export').classList.remove('hidden');
      }
      break;
    }

    case 'error':
      appendLogLine(ev.msg, 'err');
      document.getElementById('rpbar').classList.add('err');
      break;
  }
}

function setProgressBar(pct) {
  document.getElementById('rpbar').style.width = `${pct}%`;
}

function renderMapTable() {
  const rows = S.mapData.slice(0, 500).map(
    r => `<tr><td>${r.table}</td><td>${r.source_id}</td><td>${r.target_id}</td></tr>`
  );

  if (S.mapData.length > 500) {
    rows.push(`<tr><td colspan="3" style="color:var(--text3)">
      … ${(S.mapData.length - 500).toLocaleString()} more (export CSV for full list)
    </td></tr>`);
  }

  document.getElementById('map-tbody').innerHTML = rows.join('');
}

function exportMap() {
  const header = 'table,source_id,target_id';
  const lines  = S.mapData.map(r => `${r.table},${r.source_id},${r.target_id}`);
  const csv    = [header, ...lines].join('\n');
  const date   = new Date().toISOString().slice(0, 10);

  const a   = document.createElement('a');
  a.href    = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }));
  a.download = `omop_merge_map_${date}.csv`;
  a.click();
}

/* ==========================================================================
   8. Utilities
   ========================================================================== */

/** POST JSON to a URL and return the raw Response (for streaming). */
async function postJSON(url, body) {
  return fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
}

/**
 * Read a streaming Response as newline-delimited JSON.
 * Calls onEvent(parsedObject) for each complete JSON line received.
 */
async function readNDJSON(response, onEvent) {
  const reader  = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer    = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop(); // keep incomplete trailing line

    for (const line of lines) {
      if (!line.trim()) continue;
      try { onEvent(JSON.parse(line)); } catch { /* skip malformed lines */ }
    }
  }
}

/* ==========================================================================
   9. Init
   ========================================================================== */

document.addEventListener('DOMContentLoaded', () => {
  initTables();
  updateMode();
});
