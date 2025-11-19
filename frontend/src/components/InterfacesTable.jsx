import React from 'react'

const rows = [
  {port:'eth0', status:'up', dl:'120 Mbps', ul:'45 Mbps'},
  {port:'eth1', status:'up', dl:'80 Mbps', ul:'12 Mbps'},
  {port:'eth2', status:'down', dl:'0 Mbps', ul:'0 Mbps'},
]

export default function InterfacesTable(){
  return (
    <section className="interfaces">
      <h3>Interfaces</h3>
      <table className="interfaces-table">
        <thead>
          <tr>
            <th>Port Name</th>
            <th>Status</th>
            <th>Download</th>
            <th>Upload</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r,i)=> (
            <tr key={r.port} style={{animationDelay:`${i*80}ms`}}>
              <td>{r.port}</td>
              <td><span className={`status-badge ${r.status === 'up' ? 'green' : 'red'}`} /></td>
              <td>{r.dl}</td>
              <td>{r.ul}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  )
}
