import React from 'react'
import DeviceHeader from './components/DeviceHeader'
import MetricsSection from './components/MetricsSection'
import InterfacesTable from './components/InterfacesTable'
import Diagnostics from './components/Diagnostics'

export default function App(){
  return (
    <div className="app-root">
      <div className="container">
        <DeviceHeader />
        <MetricsSection />
        <InterfacesTable />
        <Diagnostics />
      </div>
    </div>
  )
}
