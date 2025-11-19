import React, {useState} from 'react'

function Panel({title, children, open}){
  return (
    <div className={`panel ${open? 'open':''}`}>
      <div className="panel-inner">
        <h4>{title}</h4>
        <div className="panel-content">{children}</div>
      </div>
    </div>
  )
}

export default function Diagnostics(){
  const [openPanel, setOpenPanel] = useState(null)
  const toggle = (name) => setOpenPanel(openPanel === name ? null : name)

  return (
    <section className="diagnostics">
      <h3>Diagnostics</h3>
      <div className="diag-buttons">
        <button onClick={()=>toggle('anomaly')}>Run Anomaly Detection</button>
        <button onClick={()=>toggle('rca')}>Run RCA</button>
        <button onClick={()=>toggle('ai')}>Run AI Diagnosis</button>
      </div>

      <Panel title="Anomaly Detection" open={openPanel === 'anomaly'}>
        <div className="result">No anomalies detected. (sample)</div>
      </Panel>

      <Panel title="Root Cause Analysis" open={openPanel === 'rca'}>
        <div className="result">RCA completed: CPU spike due to process X.</div>
      </Panel>

      <Panel title="AI Diagnosis" open={openPanel === 'ai'}>
        <div className="result">AI suggests checking power supply and interface configs.</div>
      </Panel>
    </section>
  )
}
