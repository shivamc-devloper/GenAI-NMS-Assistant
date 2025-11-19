import React from 'react'

function Card({title, percent, type='bar'}){
  return (
    <div className="metric-card">
      <div className="metric-header">
        <span>{title}</span>
        <span className="metric-value">{percent}%</span>
      </div>
      <div className="metric-body">
        {type === 'circle' ? (
          <div className="progress-circle" style={{['--percent']: `${percent}%`}} aria-hidden>
            <div className="progress-center">{percent}%</div>
          </div>
        ):(
          <div className="progress-bar">
            <div className="progress-fill" style={{width:`${percent}%`}} />
          </div>
        )}
      </div>
    </div>
  )
}

export default function MetricsSection(){
  return (
    <section className="metrics">
      <Card title="CPU Usage" percent={58} />
      <Card title="Memory Usage" percent={73} />
      <Card title="Uptime" percent={99} type="circle" />
    </section>
  )
}
