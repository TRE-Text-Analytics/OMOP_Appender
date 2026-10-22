import { useState } from 'react'
import { postJSON } from '../../lib/api.js'
import { InfoMsg } from '../shared/InfoMsg.jsx'

function DbForm({ label, config, onChange }) {
  const set = (field) => (e) => onChange({ ...config, [field]: field === 'port' ? +e.target.value : e.target.value })

  return (
    <div className="card">
      <div className="ctitle">{label}</div>
      <div className="frow c3">
        <div><label>Host</label><input type="text" value={config.host} onChange={set('host')} /></div>
        <div><label>Port</label><input type="number" value={config.port} onChange={set('port')} /></div>
        <div><label>Schema</label><input type="text" value={config.schema_name} onChange={set('schema_name')} /></div>
      </div>
      <div className="frow c2">
        <div><label>Database</label><input type="text" value={config.database} onChange={set('database')} placeholder={label === 'Source database' ? 'omop_source' : 'omop_target'} /></div>
        <div><label>Username</label><input type="text" value={config.username} onChange={set('username')} placeholder="postgres" /></div>
      </div>
      <div className="frow">
        <div><label>Password</label><input type="password" value={config.password} onChange={set('password')} /></div>
      </div>
    </div>
  )
}

function ConnStatus({ status }) {
  if (!status) return null
  const cls = status === 'testing' ? 'pend' : status === 'ok' ? 'ok' : 'err'
  const text = status === 'testing' ? 'Testing…' : status === 'ok' ? 'Connected ✓' : status
  return <span className={`cs ${cls}`}>{text}</span>
}

export default function StepConnections({ srcConfig, setSrcConfig, tgtConfig, setTgtConfig, goStep }) {
  const [srcStatus, setSrcStatus] = useState(null)
  const [tgtStatus, setTgtStatus] = useState(null)

  async function testConn(config, setStatus) {
    setStatus('testing')
    try {
      const res = await postJSON('/omop-appender/test-connection', { config })
      const data = await res.json()
      setStatus(data.ok ? 'ok' : data.error || 'Failed')
    } catch {
      setStatus('Unreachable')
    }
  }

  return (
    <div className="panel active">
      <div className="ptitle">Step 01</div>
      <div className="phead">Database connections</div>
      <InfoMsg>
        Running this app in Docker alongside database containers? Try <code className="accent">host.docker.internal</code> instead of localhost.
      </InfoMsg>


      <DbForm label="Source database" config={srcConfig} onChange={setSrcConfig} />
      <div className="inline-row" style={{ marginBottom: '0.75rem' }}>
        <button className="btn sm" onClick={() => testConn(srcConfig, setSrcStatus)}>Test connection</button>
        <ConnStatus status={srcStatus} />
      </div>

      <DbForm label="Target database" config={tgtConfig} onChange={setTgtConfig} />
      <div className="inline-row" style={{ marginBottom: '0.75rem' }}>
        <button className="btn sm" onClick={() => testConn(tgtConfig, setTgtStatus)}>Test connection</button>
        <ConnStatus status={tgtStatus} />
      </div>

      <div className="brow">
        <span className="sp" />
        <button className="btn primary" onClick={() => goStep(1)}>Next → Tables</button>
      </div>
    </div>
  )
}
