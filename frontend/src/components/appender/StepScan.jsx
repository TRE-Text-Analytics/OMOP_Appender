import { useState } from 'react'
import { postJSON, readNDJSON } from '../../lib/api.js'

export default function StepScan({
  srcConfig, tgtConfig, selected,
  patientScope, sampleMode, patientLimit,
  setScanResult, goStep,
}) {
  const [scanning, setScanning]           = useState(false)
  const [tableProgress, setTableProgress] = useState({})
  const [scanDone, setScanDone]           = useState(null)   // scan_complete payload

  async function startScan() {
    setScanning(true)
    setTableProgress({})
    setScanDone(null)
    setScanResult(null)

    try {
      const stream = await postJSON('/omop-appender/scan', {
        source:        srcConfig,
        target:        tgtConfig,
        tables:        [...selected],
        patient_scope: patientScope,
        patient_limit: sampleMode && patientLimit > 0 ? patientLimit : null,
      })

      await readNDJSON(stream, ev => {
        if (ev.type === 'table_scan') {
          setTableProgress(prev => ({ ...prev, [ev.table]: ev }))
        } else if (ev.type === 'scan_complete') {
          setScanDone(ev)
          setScanResult(ev)
        }
      })
    } catch (err) {
      console.error('Scan failed:', err)
    }

    setScanning(false)
  }

  const patients   = scanDone?.patients ?? []
  const newPats    = patients.filter(p => p.is_new_patient).length
  const hasResults = scanDone !== null || Object.keys(tableProgress).length > 0

  return (
    <div className="panel active">
      <div className="ptitle">Step 04</div>
      <div className="phead">Scan &amp; preview</div>

      <div className="card">
        <p className="hint bottom-gap">
          The scan compares both databases across your selected tables and identifies every patient
          with new rows to import — without writing anything. Run this before each merge to
          preview exactly what will change.
        </p>
        <div className="inline-row">
          <button className="btn primary" onClick={startScan} disabled={scanning}>
            {scanning ? '…' : '▶ Run scan'}
          </button>
          {scanning && (
            <span className="scan-status-inline">
              <span className="dot-pulse" /> Scanning…
            </span>
          )}
        </div>
      </div>

      {hasResults && (
        <>
          {/* Per-table progress (live during scan) */}
          {Object.keys(tableProgress).length > 0 && (
            <div className="card flush-card">
              <div className="card-section-header"><span>Per-table results</span></div>
              <div>
                {Object.entries(tableProgress).map(([tbl, ev]) => (
                  <div key={tbl} className="tresult">
                    <span className="tn">{tbl}</span>
                    {ev.missing
                      ? <span className="tskip">not found in DB</span>
                      : <>
                          <span className="trows">{ev.new_rows.toLocaleString()} new rows</span>
                          <span className="tpats">{ev.affected_patients.toLocaleString()} patients</span>
                        </>
                    }
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Summary stats — only after scan_complete */}
          {scanDone && (
            <>
              <div className="stat-grid">
                <div className="scard">
                  <div className="sl">With new data</div>
                  <div className="sv green">{scanDone.total_patients_with_new_data}</div>
                </div>
                <div className="scard">
                  <div className="sl">New rows total</div>
                  <div className="sv green">{scanDone.total_new_rows.toLocaleString()}</div>
                </div>
                <div className="scard">
                  <div className="sl">New patients</div>
                  <div className="sv amber">{newPats}</div>
                </div>
              </div>

              <div className="card flush-card">
                <div className="card-section-header">
                  <span>Patients with new data</span>
                  <span className="muted-label">{patients.length} patient{patients.length !== 1 ? 's' : ''}</span>
                </div>
                <div className="ptable-wrap">
                  <table className="ptable">
                    <thead>
                      <tr>
                        <th>Source ID</th>
                        <th>Source value</th>
                        <th>Status</th>
                        <th className="align-right">New rows</th>
                      </tr>
                    </thead>
                    <tbody>
                      {patients.slice(0, 200).map((p, i) => (
                        <tr key={i}>
                          <td>{p.source_person_id}</td>
                          <td style={{ color: 'var(--text2)' }}>{p.source_value ?? '—'}</td>
                          <td>
                            {p.is_new_patient
                              ? <span className="new-tag">new</span>
                              : <span className="exist-tag">existing</span>
                            }
                          </td>
                          <td className="align-right" style={{ color: 'var(--green)' }}>
                            {p.total_new_rows.toLocaleString()}
                          </td>
                        </tr>
                      ))}
                      {patients.length > 200 && (
                        <tr>
                          <td colSpan={4} style={{ color: 'var(--text3)', fontStyle: 'italic' }}>
                            … and {patients.length - 200} more
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </>
          )}
        </>
      )}

      <div className="brow">
        <button className="btn" onClick={() => goStep(2)}>← Back</button>
        <span className="sp" />
        <button
          className="btn primary"
          onClick={() => goStep(4)}
          disabled={!scanDone || scanDone.total_new_rows === 0}
        >
          Proceed to run →
        </button>
      </div>
    </div>
  )
}
