import { useState } from 'react'
import AppenderTool from './components/appender/AppenderTool.jsx'

/**
 * App is the root component. It owns the current view state and renders
 * the shell (header + content area). Adding a new tool means:
 *  1. Adding a new view key here
 *  2. Creating a new component under components/<feature>/
 *  3. Adding a card to MainMenu and a case in renderView()
 */
export default function App() {
  const [view, setView] = useState('appender')      // 'menu' | 'appender' | 'vocabulary' | ...
  const [subtitle] = useState('OMOP Appender')
  const [dryRun, setDryRun] = useState(true)    // lifted so header badge can read it

  const showBadge = 'appender'

  function renderView() {
    return <AppenderTool dryRun={dryRun} setDryRun={setDryRun} />
  }

  return (
    <div className="shell">
      <header>
        <div className="header-left">
          <span className="logo">
            OMOP<span className="logo-sep">/</span>Appender
          </span>
        </div>
        <span className="logo-sub">{subtitle}</span>
        <span className="header-spacer" />
        {showBadge && (
          <span className={`run-badge show${!dryRun ? ' live' : ''}`}>
            {dryRun ? 'dry-run' : 'live'}
          </span>
        )}
      </header>

      {renderView()}
    </div>
  )
}
