function RadioCard({ id, selected, onSelect, title, desc }) {
  return (
    <div className={`copt${selected ? ' sel' : ''}`} onClick={() => onSelect(id)}>
      <input type="radio" readOnly checked={selected} />
      <div>
        <div className="ctit">{title}</div>
        <div className="cdesc">{desc}</div>
      </div>
    </div>
  )
}

function Toggle({ checked, onChange, label }) {
  return (
    <div className="toggle-wrap">
      <label className="toggle">
        <input type="checkbox" checked={checked} onChange={e => onChange(e.target.checked)} />
        <span className="tslider" />
      </label>
      <span className="tlabel" onClick={() => onChange(!checked)}>{label}</span>
    </div>
  )
}

export default function StepConflicts({
  personConflict, setPersonConflict,
  dedupEnabled, setDedupEnabled,
  idStrategy, setIdStrategy,
  idOffset, setIdOffset,
  patientScope, setPatientScope,
  sampleMode, setSampleMode,
  patientLimit, setPatientLimit,
  goStep,
}) {
  return (
    <div className="panel active">
      <div className="ptitle">Step 03</div>
      <div className="phead">Conflict &amp; ID strategy</div>

      {/* Patient scope */}
      <div className="card">
        <div className="ctitle">Patient scope</div>
        <RadioCard
          id="existing_and_new"
          selected={patientScope === 'existing_and_new'}
          onSelect={setPatientScope}
          title="Existing + create new patients"
          desc="Import clinical data for existing target patients and add brand-new person records for source-only patients. Default behaviour."
        />
        <RadioCard
          id="existing_only"
          selected={patientScope === 'existing_only'}
          onSelect={setPatientScope}
          title="Existing patients only"
          desc="Only import clinical rows for patients already in the target (matched by person_source_value). Source-only patients and the person table are skipped entirely."
        />
      </div>

      {/* Sample mode */}
      <div className="card">
        <div className="ctitle">Sample mode</div>
        <Toggle
          checked={sampleMode}
          onChange={setSampleMode}
          label="Limit to first N patients (for fast iteration)"
        />
        {sampleMode && (
          <div className="frow" style={{ marginTop: '0.75rem' }}>
            <div>
              <label>Patient limit</label>
              <input
                type="number"
                value={patientLimit}
                min={1}
                onChange={e => setPatientLimit(+e.target.value)}
              />
            </div>
          </div>
        )}
        <p className="hint top-gap">
          Deterministic by person_id ascending. Filter pushes into SQL so a 100-patient sample of a
          169M-row measurement returns in seconds rather than minutes.
        </p>
      </div>

      {/* Person conflict */}
      <div className={`card${patientScope === 'existing_only' ? ' is-disabled' : ''}`} id="card-person-conflict">
        <div className="ctitle">Person conflict strategy</div>
        {[
          { id: 'skip',   title: 'Skip',   desc: 'Leave existing target person rows untouched. Only import rows for patients that are new to the target. Default — safest option.' },
          { id: 'upsert', title: 'Upsert', desc: 'Overwrite the target person row with the source version for matched patients. Use when you want demographics to follow the source.' },
          { id: 'abort',  title: 'Abort',  desc: 'Stop the merge immediately if any source patient already exists in the target. Use when you want to guarantee a clean import.' },
        ].map(o => (
          <RadioCard
            key={o.id}
            id={o.id}
            selected={personConflict === o.id}
            onSelect={setPersonConflict}
            title={o.title}
            desc={o.desc}
          />
        ))}
      </div>

      {/* Dedup */}
      <div className="card">
        <div className="ctitle">Deduplication</div>
        <Toggle
          checked={dedupEnabled}
          onChange={setDedupEnabled}
          label="Skip rows already in target (recommended)"
        />
        <p className="hint top-gap">
          Compares each source row against target using table-specific dedup keys before inserting.
          Disable only if you intentionally want duplicate rows.
        </p>
      </div>

      {/* ID strategy */}
      <div className="card">
        <div className="ctitle">ID strategy</div>
        <div className="frow">
          <div>
            <label>Strategy</label>
            <select value={idStrategy} onChange={e => setIdStrategy(e.target.value)}>
              <option value="auto">Auto-assign — next available ID in target (safest)</option>
              <option value="preserve">Preserve source IDs — only if namespaces don't overlap</option>
              <option value="offset">Apply fixed offset to source IDs</option>
            </select>
          </div>
        </div>
        {idStrategy === 'offset' && (
          <div className="frow">
            <div>
              <label>Offset value</label>
              <input type="number" value={idOffset} onChange={e => setIdOffset(+e.target.value)} />
            </div>
          </div>
        )}
        <p className="hint">A source→target ID mapping CSV is exported after every run.</p>
      </div>

      <div className="brow">
        <button className="btn" onClick={() => goStep(1)}>← Back</button>
        <span className="sp" />
        <button className="btn primary" onClick={() => goStep(3)}>Next → Scan &amp; preview</button>
      </div>
    </div>
  )
}
