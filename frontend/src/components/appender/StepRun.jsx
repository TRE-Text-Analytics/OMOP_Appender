import { useState, useRef, useEffect } from 'react'
import { postJSON, readNDJSON } from '../../lib/api.js'

/* --------------------------------------------------------------------------
   Error panel
   -------------------------------------------------------------------------- */

function ErrorPanel({ ev, onDismiss }) {
  if (!ev) return null

  const row        = ev.row ?? {}
  const oversize   = ev.oversize_columns ?? []
  const likely     = ev.likely_column ?? ev.pg_column ?? null
  const highlight  = new Set([...oversize.map(o => o.column), ...(likely ? [likely] : [])])
  const cols       = Object.keys(row)
  const ordered    = [...cols.filter(c => highlight.has(c)), ...cols.filter(c => !highlight.has(c))]

  const metaPairs = [
    ['PG message',  ev.pg_message,               'ep-message'],
    ['SQLSTATE',    ev.sqlstate,                  ''],
    ['Detail',      ev.pg_detail,                 ''],
    ['Column',      ev.pg_column ?? ev.likely_column, ''],
    ['Constraint',  ev.pg_constraint,             ''],
  ].filter(([, v]) => v != null && v !== '')

  return (
    <div className="error-panel" style={{ marginBottom: '0.875rem' }}>
      <div className="error-panel-head">
        <span className="ep-icon">⨯</span>
        <span className="ep-title">Insert failed</span>
        <span className="ep-table">{ev.table ?? '?'}</span>
        <button className="ep-dismiss" onClick={onDismiss}>Dismiss</button>
      </div>
      <div className="error-panel-body">
        <dl className="ep-meta">
          {metaPairs.map(([k, v, cls]) => (
            <>
              <dt key={k + '-dt'}>{k}</dt>
              <dd key={k + '-dd'} className={cls}>{String(v)}</dd>
            </>
          ))}
        </dl>

        {oversize.length > 0 && (
          <div>
            <div className="ep-section-label">Oversize value{oversize.length > 1 ? 's' : ''} detected</div>
            <div className="ep-oversize-list">
              {oversize.map((o, i) => (
                <div key={i} className="ep-oversize-row">
                  <div className="ep-oversize-col">{o.column}</div>
                  <div className="ep-oversize-len">
                    {o.length} chars <span className="ep-oversize-arrow">→</span> target accepts {o.max_length}
                  </div>
                  <div className="ep-oversize-preview">{o.preview ?? ''}</div>
                </div>
              ))}
            </div>
          </div>
        )}

        <div>
          <div className="ep-section-label">
            Offending row{highlight.size ? ` — flagged column${highlight.size > 1 ? 's' : ''} pinned to top` : ''}
          </div>
          <table className="ep-row-table">
            <thead><tr><th>Column</th><th>Value</th></tr></thead>
            <tbody>
              {ordered.length === 0
                ? <tr><td colSpan={2} className="ep-null">no row data</td></tr>
                : ordered.map(c => (
                    <tr key={c} className={highlight.has(c) ? 'ep-likely' : ''}>
                      <td className="ep-col-name">{c}</td>
                      <td>{row[c] == null ? <span className="ep-null">NULL</span> : String(row[c])}</td>
                    </tr>
                  ))
              }
            </tbody>
          </table>
        </div>

        <div>
          <div className="ep-section-label">Statement</div>
          <div className="ep-sql">{ev.sql ?? ''}</div>
        </div>
      </div>
    </div>
  )
}

/* --------------------------------------------------------------------------
   Person audit summary
   -------------------------------------------------------------------------- */

const AUDIT_LABELS = {
  matched_existing:          'Matched existing',
  inserted_new:              'Inserted new',
  source_only_skipped:       'Source-only skipped',
  unmatched_no_source_value: 'No source_value',
  unmatched:                 'Unmatched',
}

function PersonAuditSummary({ counts, total, truncated, shown }) {
  return (
    <div className="audit-summary">
      {Object.entries(AUDIT_LABELS)
        .filter(([k]) => (counts[k] ?? 0) > 0)
        .map(([k, label]) => (
          <div key={k} className={`audit-pill audit-${k.replaceAll('_', '-')}`}>
            <span className="audit-label">{label}</span>
            <span className="audit-count">{counts[k].toLocaleString()}</span>
          </div>
        ))
      }
      {truncated && (
        <div className="audit-trunc">
          Audit export limited to {shown.toLocaleString()} of {total.toLocaleString()} patients
        </div>
      )}
    </div>
  )
}

/* --------------------------------------------------------------------------
   Main component
   -------------------------------------------------------------------------- */

export default function StepRun({
  srcConfig, tgtConfig, selected, scanResult,
  personConflict, dedupEnabled, idStrategy, idOffset,
  patientScope, sampleMode, patientLimit,
  dryRun, setDryRun,
  goStep,
}) {
  const [running, setRunning]     = useState(false)
  const [logLines, setLogLines]   = useState([])
  const [progress, setProgress]   = useState(0)
  const [progState, setProgState] = useState('idle')   // idle | running | done | err
  const [stats, setStats]         = useState({ rows: '—', conflicts: '—' })
  const [mapData, setMapData]     = useState([])
  const [audit, setAudit]         = useState(null)
  const [rowError, setRowError]   = useState(null)
  const logRef = useRef(null)

  // Auto-scroll log
  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight
  }, [logLines])

  function addLog(msg, level = '') {
    const ts = new Date().toTimeString().slice(0, 8)
    setLogLines(prev => [...prev, { ts, msg, level }])
  }

  async function startRun() {
    setRunning(true)
    setLogLines([])
    setProgress(0)
    setProgState('running')
    setMapData([])
    setAudit(null)
    setRowError(null)
    setStats({ rows: '—', conflicts: '—' })

    const mergeConfig = {
      source:          srcConfig,
      target:          tgtConfig,
      tables:          [...selected],
      person_conflict: personConflict,
      dedup_enabled:   dedupEnabled,
      id_strategy:     idStrategy,
      id_offset:       idOffset,
      dry_run:         dryRun,
      patient_scope:   patientScope,
      patient_limit:   sampleMode && patientLimit > 0 ? patientLimit : null,
    }

    addLog(`Starting ${dryRun ? 'dry run' : 'live merge'} across ${selected.size} tables…`)

    try {
      const stream = await postJSON('/omop-appender/merge', mergeConfig)
      await readNDJSON(stream, ev => {
        switch (ev.type) {
          case 'log':
            addLog(ev.msg, ev.level ?? '')
            break
          case 'progress':
            setProgress(Math.round((ev.step / ev.total) * 100))
            break
          case 'summary':
            setProgress(100)
            setProgState('done')
            setStats({ rows: ev.inserted.toLocaleString(), conflicts: ev.conflicts })
            setMapData(ev.mapping ?? [])
            setAudit({
              counts:    ev.person_audit_counts ?? {},
              data:      ev.person_audit ?? [],
              total:     ev.person_audit_total ?? 0,
              truncated: ev.person_audit_truncated ?? false,
            })
            break
          case 'error':
            addLog(ev.msg, 'err')
            setProgState('err')
            if (ev.error_kind === 'row_insert') setRowError(ev)
            break
        }
      })
    } catch (err) {
      addLog(`Network error: ${err.message}`, 'err')
      setProgState('err')
    }

    setRunning(false)
  }

  function exportCSV(rows, filename, header, mapper) {
    const csv = [header, ...rows.map(mapper)].join('\n')
    const a   = document.createElement('a')
    a.href    = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }))
    a.download = filename
    a.click()
  }

  function exportMap() {
    exportCSV(
      mapData,
      `omop_appender_map_${new Date().toISOString().slice(0, 10)}.csv`,
      'table,source_id,target_id',
      r => `${r.table},${r.source_id},${r.target_id}`,
    )
  }

  function exportAudit() {
    const esc = v => {
      if (v == null) return ''
      const s = String(v)
      return /[",\n\r]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s
    }
    exportCSV(
      audit.data,
      `omop_person_audit_${new Date().toISOString().slice(0, 10)}.csv`,
      'source_person_id,target_person_id,person_source_value,match_type',
      r => [esc(r.source_person_id), esc(r.target_person_id), esc(r.person_source_value), esc(r.match_type)].join(','),
    )
  }

  const bannerClass = dryRun ? 'mode-banner' : 'mode-banner live'

  return (
    <div className="panel active">
      <div className="ptitle">Step 05</div>
      <div className="phead">Execute merge</div>

      <div className={bannerClass}>
        {dryRun ? '⬡ Dry-run — no data will be written to the target' : '⚠ Live mode — changes will be committed to target'}
      </div>

      {sampleMode && patientLimit > 0 && (
        <div className="mode-banner sample" style={{ marginTop: '-0.5rem' }}>
          ◇ Sample mode — limited to first {patientLimit.toLocaleString()} patients
        </div>
      )}

      {rowError && <ErrorPanel ev={rowError} onDismiss={() => setRowError(null)} />}

      <div className="stat-grid">
        <div className="scard">
          <div className="sl">Affected patients</div>
          <div className="sv">{scanResult ? scanResult.total_patients_with_new_data : '—'}</div>
        </div>
        <div className="scard">
          <div className="sl">Tables</div>
          <div className="sv">{selected.size}</div>
        </div>
        <div className="scard">
          <div className="sl">Rows inserted</div>
          <div className="sv green">{stats.rows}</div>
        </div>
        <div className="scard">
          <div className="sl">Person conflicts</div>
          <div className="sv amber">{stats.conflicts}</div>
        </div>
      </div>

      <div className="pbar-wrap">
        <div
          className={`pbar${progState === 'done' ? ' done' : progState === 'err' ? ' err' : ''}`}
          style={{ width: `${progress}%` }}
        />
      </div>

      <div className="log-wrap" ref={logRef}>
        {logLines.length === 0
          ? <div className="ll"><span className="lm">Ready — configure options below and press Run.</span></div>
          : logLines.map((l, i) => (
              <div key={i} className="ll">
                <span className="lts">{l.ts}</span>
                <span className={`lm${l.level ? ' ' + l.level : ''}`}>{l.msg}</span>
              </div>
            ))
        }
      </div>

      <div className="card">
        <div className="run-controls">
          <div className="toggle-wrap">
            <label className="toggle">
              <input type="checkbox" checked={dryRun} onChange={e => setDryRun(e.target.checked)} />
              <span className="tslider" />
            </label>
            <span className="tlabel" onClick={() => setDryRun(!dryRun)}>Dry run (no writes)</span>
          </div>
          <span className="sp" />
          <button className="btn" onClick={startRun} disabled={running}>▶ Run</button>
          {audit?.data?.length > 0 && (
            <button className="btn success" onClick={exportAudit}>↓ Export person audit</button>
          )}
          {mapData.length > 0 && (
            <button className="btn success" onClick={exportMap}>↓ Export ID mapping</button>
          )}
        </div>
      </div>

      {/* Person identity audit */}
      {audit?.data?.length > 0 && (
        <div className="card flush-card">
          <div className="card-section-header">
            <span>Person identity audit</span>
            <span className="muted-label">how source patients matched to target</span>
          </div>
          <PersonAuditSummary
            counts={audit.counts}
            total={audit.total}
            truncated={audit.truncated}
            shown={audit.data.length}
          />
        </div>
      )}

      {/* ID mapping log */}
      {mapData.length > 0 && (
        <div className="card flush-card">
          <div className="card-section-header">ID mapping log</div>
          <div className="maptable-wrap">
            <table className="maptable">
              <thead>
                <tr><th>Table</th><th>Source ID</th><th>Target ID</th></tr>
              </thead>
              <tbody>
                {mapData.slice(0, 500).map((r, i) => (
                  <tr key={i}>
                    <td>{r.table}</td><td>{r.source_id}</td><td>{r.target_id}</td>
                  </tr>
                ))}
                {mapData.length > 500 && (
                  <tr>
                    <td colSpan={3} style={{ color: 'var(--text3)' }}>
                      … {(mapData.length - 500).toLocaleString()} more (export CSV for full list)
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <div className="brow">
        <button className="btn" onClick={() => goStep(3)}>← Back</button>
      </div>
    </div>
  )
}
