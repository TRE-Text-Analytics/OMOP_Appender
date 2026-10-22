const DOMAIN_LABELS = {
  core:    'Core person',
  visit:   'Visits',
  clinical:'Clinical events',
  derived: 'Derived / ERA tables',
  admin:   'Admin / reference',
}
const DOMAIN_ORDER = ['core', 'visit', 'clinical', 'derived', 'admin']

export default function StepTables({ tables, selected, toggleTable, selectAll, goStep }) {
  const byDomain = {}
  for (const [name, meta] of Object.entries(tables)) {
    ;(byDomain[meta.domain] ??= []).push({ name, ...meta })
  }

  return (
    <div className="panel active">
      <div className="ptitle">Step 02</div>
      <div className="phead">Table selection</div>

      <div className="card">
        <div className="card-header-row">
          <div className="ctitle flush">Which tables to sync</div>
          <div className="inline-row">
            <button className="btn sm" onClick={() => selectAll(true)}>All</button>
            <button className="btn sm" onClick={() => selectAll(false)}>None</button>
          </div>
        </div>
        <p className="hint bottom-gap">
          The tool will scan both databases and import only rows that don't already exist in the
          target, for every patient where new data is found.
        </p>

        {DOMAIN_ORDER.map(domain => {
          const domainTables = byDomain[domain] ?? []
          if (!domainTables.length) return null
          return (
            <div key={domain} className="domain-group">
              <div className="dlabel">{DOMAIN_LABELS[domain] ?? domain}</div>
              <div className="tgrid">
                {domainTables.map(t => {
                  const sel = selected.has(t.name)
                  return (
                    <div
                      key={t.name}
                      className={`tcard${sel ? ' sel' : ''}`}
                      onClick={() => toggleTable(t.name)}
                    >
                      <input
                        type="checkbox"
                        checked={sel}
                        onChange={() => toggleTable(t.name)}
                        onClick={e => e.stopPropagation()}
                      />
                      <div>
                        <div className="tname">{t.name}</div>
                        <div className="tdesc">{t.description}</div>
                      </div>
                    </div>
                  )
                })}
              </div>
            </div>
          )
        })}
      </div>

      <div className="brow">
        <button className="btn" onClick={() => goStep(0)}>← Back</button>
        <span className="sp" />
        <button className="btn primary" onClick={() => goStep(2)}>Next → Conflict rules</button>
      </div>
    </div>
  )
}
