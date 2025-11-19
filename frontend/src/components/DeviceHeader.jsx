import React, {useState} from 'react'

export default function DeviceHeader(){
  const [rotating, setRotating] = useState(false)
  const refresh = () => {
    setRotating(true)
    setTimeout(()=> setRotating(false), 900)
    // In real app, trigger data refresh here
  }

  return (
    <header className="device-header">
      <div>
        <h2 className="device-name">Device name: <span className="device-value">Edge Switch 01</span></h2>
        <p className="ip">IP: <span className="device-value">192.168.1.42</span></p>
      </div>
      <div className="header-right">
        <div className="status-row">
          <span className="status-badge green" />
          <span className="last-seen">Last seen: <strong>2m ago</strong></span>
        </div>
        <button className={`refresh-btn ${rotating? 'rotating':''}`} onClick={refresh} title="Refresh">
          ⟳
        </button>
      </div>
    </header>
  )
}
