import { useState, useEffect } from 'react'
import { getJSON } from '../../lib/api.js'
import StepConnections from './StepConnections.jsx'
import StepTables from './StepTables.jsx'
import StepConflicts from './StepConflicts.jsx'
import StepScan from './StepScan.jsx'
import StepRun from './StepRun.jsx'

const STEPS = [
  { label: 'Connections' },
  { label: 'Tables' },
  { label: 'Conflict rules' },
  { label: 'Scan & preview' },
  { label: 'Run' },
]

const DEFAULT_SELECTED = new Set([
  'person', 'death', 'visit_occurrence',
  'condition_occurrence', 'drug_exposure', 'measurement', 'observation',
])

/**
 * AppenderTool owns all wizard state and passes slices down to each step.
 * Steps are pure — they receive props and call callbacks; they don't fetch
 * or mutate state directly. This makes each step easy to test or replace.
 */
export default function AppenderTool({ dryRun, setDryRun }) {
  const [step, setStep]           = useState(0)
  const [tables, setTables]       = useState({})
  const [selected, setSelected]   = useState(new Set(DEFAULT_SELECTED))
  const [scanResult, setScanResult] = useState(null)

  // DB config state — lifted here so it persists across step navigation
  const [srcConfig, setSrcConfig] = useState({ host: 'localhost', port: 5432, schema_name: 'cdm', database: '', username: '', password: '' })
  const [tgtConfig, setTgtConfig] = useState({ host: 'localhost', port: 5432, schema_name: 'cdm', database: '', username: '', password: '' })

  // Conflict / ID strategy state
  const [personConflict, setPersonConflict] = useState('skip')
  const [dedupEnabled, setDedupEnabled]     = useState(true)
  const [idStrategy, setIdStrategy]         = useState('auto')
  const [idOffset, setIdOffset]             = useState(1000000)
  const [patientScope, setPatientScope]     = useState('existing_and_new')
  const [sampleMode, setSampleMode]         = useState(false)
  const [patientLimit, setPatientLimit]     = useState(100)

  useEffect(() => {
    getJSON('/omop-appender/tables').then(setTables).catch(() => setTables({}))
  }, [])

  function toggleTable(name) {
    setSelected(prev => {
      const next = new Set(prev)
      next.has(name) ? next.delete(name) : next.add(name)
      return next
    })
  }

  function selectAll(on) {
    setSelected(on ? new Set(Object.keys(tables)) : new Set())
  }

  const sharedProps = {
    srcConfig, setSrcConfig,
    tgtConfig, setTgtConfig,
    tables, selected, toggleTable, selectAll,
    personConflict, setPersonConflict,
    dedupEnabled, setDedupEnabled,
    idStrategy, setIdStrategy,
    idOffset, setIdOffset,
    patientScope, setPatientScope,
    sampleMode, setSampleMode,
    patientLimit, setPatientLimit,
    scanResult, setScanResult,
    dryRun, setDryRun,
    goStep: setStep,
  }

  const panels = [
    <StepConnections {...sharedProps} />,
    <StepTables      {...sharedProps} />,
    <StepConflicts   {...sharedProps} />,
    <StepScan        {...sharedProps} />,
    <StepRun         {...sharedProps} />,
  ]

  return (
    <div id="appender-view" className="view-panel">
      <nav>
        <div className="nav-label">Workflow</div>
        {STEPS.map((s, i) => (
          <div
            key={i}
            className={`nav-item${step === i ? ' active' : ''}${step > i ? ' done' : ''}`}
            onClick={() => setStep(i)}
          >
            <span className="sn">0{i + 1}</span>
            {s.label}
            <span className="ck">✓</span>
          </div>
        ))}
      </nav>

      <main>
        {panels[step]}
      </main>
    </div>
  )
}
