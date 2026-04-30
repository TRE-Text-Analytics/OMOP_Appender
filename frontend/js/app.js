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
  if (n === 4) {
    updateRunSummary();
    updateSampleBanner();
  }
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

/** Get the currently selected patient_scope value. */
function getPatientScope() {
  return document.querySelector('input[name="ps"]:checked')?.value ?? 'existing_and_new';
}

/**
 * Toggle the patient_scope radio cards and dim the person-conflict card when
 * existing-only mode is active (the conflict rules don't apply because the
 * person table won't be touched).
 */
function selPatientScope(value) {
  for (const v of ['existing_and_new', 'existing_only']) {
    const el = document.getElementById(`ps-${v.replaceAll('_', '-')}`);
    el.classList.toggle('sel', v === value);
    el.querySelector('input').checked = v === value;
  }
  document.getElementById('card-person-conflict')
    .classList.toggle('is-disabled', value === 'existing_only');
}

/**
 * Show or hide the patient-limit number input based on the sample-mode
 * toggle. Also updates the run-panel banner so the user is reminded
 * that sampling is active when they reach the Run step.
 */
function toggleSampleMode() {
  const on = document.getElementById('sample-tog').checked;
  document.getElementById('sample-row').classList.toggle('hidden', !on);
  updateSampleBanner();
}

/** Update the sample-mode banner shown above run stats in Step 04. */
function updateSampleBanner() {
  const limit  = getPatientLimit();
  const banner = document.getElementById('sample-banner');
  if (limit !== null) {
    document.getElementById('sample-banner-n').textContent = limit.toLocaleString();
    banner.classList.remove('hidden');
  } else {
    banner.classList.add('hidden');
  }
}

/**
 * Returns the current patient_limit value (positive integer) or null when
 * the sample-mode toggle is off. Backend treats null / 0 as "no limit".
 */
function getPatientLimit() {
  if (!document.getElementById('sample-tog').checked) return null;
  const n = parseInt(document.getElementById('sample-limit').value, 10);
  return Number.isFinite(n) && n > 0 ? n : null;
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
      patient_scope: getPatientScope(),
      patient_limit: getPatientLimit(),
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
    patient_scope:    getPatientScope(),
    patient_limit:    getPatientLimit(),
  };

  // Reset UI
  document.getElementById('run-log').innerHTML = '';
  setProgressBar(0);
  document.getElementById('rpbar').classList.remove('done', 'err');
  document.getElementById('btn-run').disabled = true;
  document.getElementById('map-section').classList.add('hidden');
  document.getElementById('btn-export').classList.add('hidden');
  document.getElementById('audit-section').classList.add('hidden');
  document.getElementById('btn-export-audit').classList.add('hidden');
  hideErrorPanel();

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

      // v1.5.3: person identity audit. Independent of the row-level
      // mapping log — answers "did source patient X correctly link to
      // target patient Y via person_source_value?".
      S.personAudit       = ev.person_audit ?? [];
      S.personAuditCounts = ev.person_audit_counts ?? {};
      S.personAuditTotal  = ev.person_audit_total ?? S.personAudit.length;
      S.personAuditTruncated = ev.person_audit_truncated ?? false;
      if (S.personAudit.length > 0) {
        renderPersonAuditSummary();
        document.getElementById('btn-export-audit').classList.remove('hidden');
      }
      break;
    }

    case 'error':
      appendLogLine(ev.msg, 'err');
      document.getElementById('rpbar').classList.add('err');
      // v1.4: structured row-level errors carry table/sql/row context.
      // Fall back to plain log-only display for older error events.
      if (ev.error_kind === 'row_insert') renderRichError(ev);
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

/**
 * Render counts-by-match-type so the user can spot-check person matching
 * at a glance — without opening the CSV. Shown above the ID-mapping
 * table in the run panel.
 */
function renderPersonAuditSummary() {
  const c = S.personAuditCounts ?? {};
  const labels = {
    matched_existing:           'Matched existing',
    inserted_new:               'Inserted new',
    source_only_skipped:        'Source-only skipped',
    unmatched_no_source_value:  'No source_value',
    unmatched:                  'Unmatched',
  };
  const rows = Object.entries(labels)
    .filter(([k]) => (c[k] ?? 0) > 0)
    .map(([k, label]) =>
      `<div class="audit-pill audit-${k.replaceAll('_', '-')}">
         <span class="audit-label">${label}</span>
         <span class="audit-count">${c[k].toLocaleString()}</span>
       </div>`)
    .join('');
  const truncMsg = S.personAuditTruncated
    ? `<div class="audit-trunc">
         Audit export limited to ${S.personAudit.length.toLocaleString()}
         of ${S.personAuditTotal.toLocaleString()} patients
       </div>` : '';
  document.getElementById('audit-summary').innerHTML = rows + truncMsg;
  document.getElementById('audit-section').classList.remove('hidden');
}

/**
 * Export the person identity audit as CSV. Columns:
 *   source_person_id, target_person_id, person_source_value, match_type
 * Use this CSV to verify that source patients linked to target patients
 * with matching person_source_value, not by accident.
 */
function exportPersonAudit() {
  const header = 'source_person_id,target_person_id,person_source_value,match_type';
  // CSV-escape values: wrap in quotes and double any embedded quotes.
  const esc = v => {
    if (v === null || v === undefined) return '';
    const s = String(v);
    return /[",\n\r]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
  };
  const lines = S.personAudit.map(r => [
    esc(r.source_person_id),
    esc(r.target_person_id),
    esc(r.person_source_value),
    esc(r.match_type),
  ].join(','));
  const csv  = [header, ...lines].join('\n');
  const date = new Date().toISOString().slice(0, 10);

  const a   = document.createElement('a');
  a.href    = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }));
  a.download = `omop_person_audit_${date}.csv`;
  a.click();
}

/**
 * Render a structured backend error (MergeRowError) inline above the run
 * stats: PG message, sqlstate, the SQL that failed, and the offending
 * row's column→value mapping. Backward compatible — the run-log line is
 * still emitted by the caller, this just adds detail.
 */
function renderRichError(ev) {
  const panel = document.getElementById('error-panel');
  const row   = ev.row ?? {};
  const cols  = Object.keys(row);
  // Backend (v1.5.2) sends likely_column when it can detect the offender
  // from a varchar-length error. Fall back to pg_column then null.
  const likely = ev.likely_column ?? ev.pg_column ?? null;
  const oversize = ev.oversize_columns ?? [];
  // Mark every detected oversize column for highlighting, plus the
  // single likely_column when present.
  const highlightSet = new Set(oversize.map(o => o.column));
  if (likely) highlightSet.add(likely);

  const formatVal = v => {
    if (v === null || v === undefined) return '<span class="ep-null">NULL</span>';
    return escapeHtml(String(v));
  };

  // Reorder columns so highlighted ones come first. This is the fix for
  // the previous behaviour where 18-column INSERTs scrolled the actual
  // offender out of view in the panel's max-height region.
  const orderedCols = [
    ...cols.filter(c => highlightSet.has(c)),
    ...cols.filter(c => !highlightSet.has(c)),
  ];

  const rowRows = orderedCols.length === 0
    ? `<tr><td colspan="2" class="ep-null">no row data</td></tr>`
    : orderedCols.map(c => {
        const isLikely = highlightSet.has(c);
        return `<tr class="${isLikely ? 'ep-likely' : ''}">
          <td class="ep-col-name">${escapeHtml(c)}</td>
          <td>${formatVal(row[c])}</td>
        </tr>`;
      }).join('');

  // Oversize summary block — pulled out of the row table so it's
  // immediately visible without scrolling.
  let oversizeHtml = '';
  if (oversize.length > 0) {
    const items = oversize.map(o => `
      <div class="ep-oversize-row">
        <div class="ep-oversize-col">${escapeHtml(o.column)}</div>
        <div class="ep-oversize-len">
          ${o.length} chars
          <span class="ep-oversize-arrow">→</span>
          target accepts ${o.max_length}
        </div>
        <div class="ep-oversize-preview">${escapeHtml(o.preview ?? '')}</div>
      </div>`).join('');
    oversizeHtml = `
      <div>
        <div class="ep-section-label">
          Oversize value${oversize.length > 1 ? 's' : ''} detected
        </div>
        <div class="ep-oversize-list">${items}</div>
      </div>`;
  }

  // Build the meta dl only with fields that are present.
  const metaPairs = [
    ['PG message',  ev.pg_message,    'ep-message'],
    ['SQLSTATE',    ev.sqlstate,      ''],
    ['Detail',      ev.pg_detail,     ''],
    ['Column',      ev.pg_column ?? ev.likely_column, ''],
    ['Constraint',  ev.pg_constraint, ''],
  ].filter(([, v]) => v != null && v !== '');

  const metaHtml = metaPairs.map(([k, v, cls]) =>
    `<dt>${k}</dt><dd class="${cls}">${escapeHtml(String(v))}</dd>`
  ).join('');

  panel.innerHTML = `
    <div class="error-panel">
      <div class="error-panel-head">
        <span class="ep-icon">⨯</span>
        <span class="ep-title">Insert failed</span>
        <span class="ep-table">${escapeHtml(ev.table ?? '?')}</span>
        <button class="ep-dismiss" onclick="hideErrorPanel()">Dismiss</button>
      </div>
      <div class="error-panel-body">
        <dl class="ep-meta">${metaHtml}</dl>

        ${oversizeHtml}

        <div>
          <div class="ep-section-label">
            Offending row${highlightSet.size ? ` — flagged column${highlightSet.size > 1 ? 's' : ''} pinned to top` : ''}
          </div>
          <table class="ep-row-table">
            <thead><tr><th>Column</th><th>Value</th></tr></thead>
            <tbody>${rowRows}</tbody>
          </table>
        </div>

        <div>
          <div class="ep-section-label">Statement</div>
          <div class="ep-sql">${escapeHtml(ev.sql ?? '')}</div>
        </div>
      </div>
    </div>`;
  panel.classList.remove('hidden');
  panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function hideErrorPanel() {
  const panel = document.getElementById('error-panel');
  panel.classList.add('hidden');
  panel.innerHTML = '';
}

/** Escape arbitrary text for safe injection into HTML. */
function escapeHtml(s) {
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
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

/**
 * Opens a specific tool from the main menu and updates the header.
 * @param {string} toolId - The ID suffix of the tool wrapper (e.g., 'appender')
 * @param {string} subtitle - The text to display in the header
 */
function openTool(toolId, subtitle, isPushState = true) {
  // 1. Reset visibility
  document.querySelectorAll('.view-panel').forEach(panel => {
    panel.classList.add('hidden');
    panel.classList.remove('active');
  });
  
  // 2. Show the tool
  const targetTool = document.getElementById(`tool-${toolId}`);
  if (targetTool) {
    targetTool.classList.remove('hidden');
    targetTool.classList.add('active');
  }
  
  // 3. Update UI
  document.getElementById('btn-menu').classList.remove('hidden');
  document.getElementById('header-subtitle').innerText = subtitle;
  
  // 4. Wizard Init
  if (toolId === 'appender') {
    goStep(0); 
    updateMode();
  }

  // 5. Update Browser History (only if this isn't triggered by a back button)
  if (isPushState) {
    history.pushState({ view: 'tool', toolId, subtitle }, "");
  }
}

/**
 * Returns to Main Menu and updates history if needed
 */
function showMainMenu(isPushState = true) {
  document.querySelectorAll('.view-panel').forEach(panel => {
    panel.classList.add('hidden');
    panel.classList.remove('active');
  });
  
  const mainMenu = document.getElementById('main-menu');
  if (mainMenu) {
    mainMenu.classList.remove('hidden');
    mainMenu.classList.add('active');
  }
  
  document.getElementById('btn-menu').classList.add('hidden');
  document.getElementById('header-subtitle').innerText = 'Select a utility';
  document.getElementById('run-badge').classList.remove('show');

  // Update Browser History
  if (isPushState) {
    history.pushState({ view: 'menu' }, "");
  }
}

/* ==========================================================================
   History Listener (Browser Back/Forward Buttons)
   ========================================================================== */

window.onpopstate = function(event) {
  if (event.state && event.state.view === 'tool') {
    // If the state says we were in a tool, open it without pushing a new state
    openTool(event.state.toolId, event.state.subtitle, false);
  } else {
    // Otherwise, default back to the menu
    showMainMenu(false);
  }
};

/* ==========================================================================
   9. Init
   ========================================================================== */

document.addEventListener('DOMContentLoaded', () => {
  initTables();
  updateMode();
  
  // Save the initial state as the "Menu" view
  history.replaceState({ view: 'menu' }, "");
});