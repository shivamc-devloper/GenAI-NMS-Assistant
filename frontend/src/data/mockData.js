// Mock data for network monitoring dashboard

// Device types
export const DEVICE_TYPES = {
  ROUTER: 'router',
  SWITCH: 'switch',
  SERVER: 'server',
  CAMERA: 'camera',
  ACCESS_POINT: 'access_point',
  VIRTUAL: 'virtual'
};

// Device status
export const DEVICE_STATUS = {
  HEALTHY: 'healthy',
  WARNING: 'warning',
  CRITICAL: 'critical',
  OFFLINE: 'offline'
};

// Alert severity
export const ALERT_SEVERITY = {
  CRITICAL: 'critical',
  MAJOR: 'major',
  MINOR: 'minor',
  INFO: 'info'
};

// Generate realistic network devices
export const devices = [
  {
    id: 'device-1',
    name: 'Core-Router-1',
    type: DEVICE_TYPES.ROUTER,
    status: DEVICE_STATUS.HEALTHY,
    ip: '192.168.1.1',
    mac: '00:1A:2B:3C:4D:5E',
    vendor: 'Cisco',
    location: 'Data Center A',
    cpu: 45,
    memory: 62,
    latency: 12,
    packetLoss: 0.2,
    uptime: '45 days',
    interfaces: [
      { name: 'eth0', status: 'up', traffic: 850, errors: 0 },
      { name: 'eth1', status: 'up', traffic: 620, errors: 0 },
      { name: 'eth2', status: 'down', traffic: 0, errors: 2 }
    ],
    position: { x: 250, y: 100 }
  },
  {
    id: 'device-2',
    name: 'Switch-2',
    type: DEVICE_TYPES.SWITCH,
    status: DEVICE_STATUS.CRITICAL,
    ip: '192.168.1.10',
    mac: '00:1A:2B:3C:4D:6F',
    vendor: 'Juniper',
    location: 'Floor 3',
    cpu: 89,
    memory: 78,
    latency: 45,
    packetLoss: 2.5,
    uptime: '12 days',
    interfaces: [
      { name: 'eth0', status: 'up', traffic: 920, errors: 15 },
      { name: 'eth1', status: 'up', traffic: 880, errors: 8 },
      { name: 'eth2', status: 'up', traffic: 760, errors: 3 }
    ],
    position: { x: 100, y: 250 }
  },
  {
    id: 'device-3',
    name: 'Server-DB-01',
    type: DEVICE_TYPES.SERVER,
    status: DEVICE_STATUS.WARNING,
    ip: '192.168.2.50',
    mac: '00:1A:2B:3C:4D:7G',
    vendor: 'Dell',
    location: 'Data Center B',
    cpu: 72,
    memory: 85,
    latency: 8,
    packetLoss: 0.1,
    uptime: '120 days',
    interfaces: [
      { name: 'eth0', status: 'up', traffic: 1200, errors: 1 }
    ],
    position: { x: 400, y: 250 }
  },
  {
    id: 'device-4',
    name: 'Camera-Lobby',
    type: DEVICE_TYPES.CAMERA,
    status: DEVICE_STATUS.HEALTHY,
    ip: '192.168.3.100',
    mac: '00:1A:2B:3C:4D:8H',
    vendor: 'Hikvision',
    location: 'Building A - Lobby',
    cpu: 25,
    memory: 40,
    latency: 15,
    packetLoss: 0.0,
    uptime: '90 days',
    interfaces: [
      { name: 'eth0', status: 'up', traffic: 450, errors: 0 }
    ],
    position: { x: 550, y: 150 }
  },
  {
    id: 'device-5',
    name: 'AP-Floor2',
    type: DEVICE_TYPES.ACCESS_POINT,
    status: DEVICE_STATUS.HEALTHY,
    ip: '192.168.4.20',
    mac: '00:1A:2B:3C:4D:9I',
    vendor: 'Ubiquiti',
    location: 'Floor 2',
    cpu: 35,
    memory: 50,
    latency: 10,
    packetLoss: 0.5,
    uptime: '30 days',
    interfaces: [
      { name: 'wlan0', status: 'up', traffic: 680, errors: 2 }
    ],
    position: { x: 250, y: 350 }
  },
  {
    id: 'device-6',
    name: 'Virtual-Gateway',
    type: DEVICE_TYPES.VIRTUAL,
    status: DEVICE_STATUS.WARNING,
    ip: '10.0.0.1',
    mac: '00:1A:2B:3C:4D:AJ',
    vendor: 'VMware',
    location: 'Cloud',
    cpu: 68,
    memory: 75,
    latency: 22,
    packetLoss: 1.2,
    uptime: '15 days',
    interfaces: [
      { name: 'veth0', status: 'up', traffic: 920, errors: 5 }
    ],
    position: { x: 400, y: 400 }
  },
  {
    id: 'device-7',
    name: 'Switch-Main',
    type: DEVICE_TYPES.SWITCH,
    status: DEVICE_STATUS.HEALTHY,
    ip: '192.168.1.20',
    mac: '00:1A:2B:3C:4D:BK',
    vendor: 'Cisco',
    location: 'Data Center A',
    cpu: 42,
    memory: 55,
    latency: 9,
    packetLoss: 0.1,
    uptime: '60 days',
    interfaces: [
      { name: 'eth0', status: 'up', traffic: 780, errors: 0 },
      { name: 'eth1', status: 'up', traffic: 650, errors: 1 }
    ],
    position: { x: 100, y: 100 }
  },
  {
    id: 'device-8',
    name: 'Router-Edge',
    type: DEVICE_TYPES.ROUTER,
    status: DEVICE_STATUS.OFFLINE,
    ip: '192.168.5.1',
    mac: '00:1A:2B:3C:4D:CL',
    vendor: 'Juniper',
    location: 'Edge Network',
    cpu: 0,
    memory: 0,
    latency: 999,
    packetLoss: 100,
    uptime: '0 days',
    interfaces: [],
    position: { x: 50, y: 400 }
  }
];

// Network topology connections
export const connections = [
  { source: 'device-1', target: 'device-2' },
  { source: 'device-1', target: 'device-7' },
  { source: 'device-1', target: 'device-3' },
  { source: 'device-2', target: 'device-5' },
  { source: 'device-2', target: 'device-6' },
  { source: 'device-7', target: 'device-2' },
  { source: 'device-3', target: 'device-4' },
  { source: 'device-6', target: 'device-3' },
  { source: 'device-8', target: 'device-7' }
];

// Generate alerts
export const alerts = [
  {
    id: 'alert-1',
    severity: ALERT_SEVERITY.CRITICAL,
    title: 'High CPU usage on Switch-2',
    description: 'CPU usage has exceeded 85% threshold',
    deviceId: 'device-2',
    deviceName: 'Switch-2',
    timestamp: new Date(Date.now() - 1000 * 60 * 15).toISOString(),
    category: 'Performance',
    acknowledged: false,
    relatedAlerts: ['alert-2', 'alert-3']
  },
  {
    id: 'alert-2',
    severity: ALERT_SEVERITY.CRITICAL,
    title: 'High latency detected',
    description: 'Network latency increased to 45ms on Switch-2',
    deviceId: 'device-2',
    deviceName: 'Switch-2',
    timestamp: new Date(Date.now() - 1000 * 60 * 12).toISOString(),
    category: 'Network',
    acknowledged: false,
    relatedAlerts: ['alert-1']
  },
  {
    id: 'alert-3',
    severity: ALERT_SEVERITY.CRITICAL,
    title: 'Interface errors on Switch-2',
    description: '15 packet errors detected on eth0',
    deviceId: 'device-2',
    deviceName: 'Switch-2',
    timestamp: new Date(Date.now() - 1000 * 60 * 10).toISOString(),
    category: 'Interface',
    acknowledged: false,
    relatedAlerts: ['alert-1']
  },
  {
    id: 'alert-4',
    severity: ALERT_SEVERITY.CRITICAL,
    title: 'Device offline',
    description: 'Router-Edge is not responding',
    deviceId: 'device-8',
    deviceName: 'Router-Edge',
    timestamp: new Date(Date.now() - 1000 * 60 * 180).toISOString(),
    category: 'Connectivity',
    acknowledged: false,
    relatedAlerts: []
  },
  {
    id: 'alert-5',
    severity: ALERT_SEVERITY.CRITICAL,
    title: 'Memory threshold exceeded',
    description: 'Server-DB-01 memory usage at 85%',
    deviceId: 'device-3',
    deviceName: 'Server-DB-01',
    timestamp: new Date(Date.now() - 1000 * 60 * 5).toISOString(),
    category: 'Performance',
    acknowledged: false,
    relatedAlerts: []
  },
  {
    id: 'alert-6',
    severity: ALERT_SEVERITY.MAJOR,
    title: 'Packet loss detected',
    description: '2.5% packet loss on Switch-2',
    deviceId: 'device-2',
    deviceName: 'Switch-2',
    timestamp: new Date(Date.now() - 1000 * 60 * 8).toISOString(),
    category: 'Network',
    acknowledged: false,
    relatedAlerts: ['alert-1']
  },
  {
    id: 'alert-7',
    severity: ALERT_SEVERITY.MAJOR,
    title: 'High CPU usage',
    description: 'Server-DB-01 CPU at 72%',
    deviceId: 'device-3',
    deviceName: 'Server-DB-01',
    timestamp: new Date(Date.now() - 1000 * 60 * 20).toISOString(),
    category: 'Performance',
    acknowledged: true,
    relatedAlerts: []
  },
  {
    id: 'alert-8',
    severity: ALERT_SEVERITY.MINOR,
    description: 'Virtual-Gateway showing elevated latency',
    title: 'Elevated latency',
    deviceId: 'device-6',
    deviceName: 'Virtual-Gateway',
    timestamp: new Date(Date.now() - 1000 * 60 * 30).toISOString(),
    category: 'Network',
    acknowledged: true,
    relatedAlerts: []
  },
  // Add more minor alerts
  ...Array.from({ length: 12 }, (_, i) => ({
    id: `alert-${9 + i}`,
    severity: ALERT_SEVERITY.MINOR,
    title: `Minor alert ${i + 1}`,
    description: `Minor network issue detected on device`,
    deviceId: devices[i % devices.length].id,
    deviceName: devices[i % devices.length].name,
    timestamp: new Date(Date.now() - 1000 * 60 * (40 + i * 10)).toISOString(),
    category: 'Monitoring',
    acknowledged: i % 2 === 0,
    relatedAlerts: []
  }))
];

// Historical metrics data for charts
export const generateMetricsHistory = (hours = 24) => {
  const data = [];
  const now = Date.now();
  const interval = (hours * 60 * 60 * 1000) / 48; // 48 data points
  
  for (let i = 48; i >= 0; i--) {
    const timestamp = new Date(now - (i * interval));
    data.push({
      time: timestamp.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' }),
      timestamp: timestamp.toISOString(),
      traffic: 600 + Math.random() * 400,
      latency: 15 + Math.random() * 30,
      cpu: 40 + Math.random() * 30,
      memory: 50 + Math.random() * 20,
      bandwidth: 700 + Math.random() * 300
    });
  }
  
  return data;
};

// Device logs
export const deviceLogs = [
  {
    id: 'log-1',
    timestamp: new Date(Date.now() - 1000 * 60 * 5).toISOString(),
    level: 'ERROR',
    deviceId: 'device-2',
    deviceName: 'Switch-2',
    message: 'High CPU usage detected - threshold exceeded',
    source: 'cpu_monitor'
  },
  {
    id: 'log-2',
    timestamp: new Date(Date.now() - 1000 * 60 * 8).toISOString(),
    level: 'WARNING',
    deviceId: 'device-2',
    deviceName: 'Switch-2',
    message: 'Interface eth0 experiencing packet loss',
    source: 'interface_monitor'
  },
  {
    id: 'log-3',
    timestamp: new Date(Date.now() - 1000 * 60 * 10).toISOString(),
    level: 'ERROR',
    deviceId: 'device-2',
    deviceName: 'Switch-2',
    message: 'Packet errors detected on eth0',
    source: 'interface_monitor'
  },
  {
    id: 'log-4',
    timestamp: new Date(Date.now() - 1000 * 60 * 15).toISOString(),
    level: 'INFO',
    deviceId: 'device-3',
    deviceName: 'Server-DB-01',
    message: 'Database backup completed successfully',
    source: 'system'
  },
  {
    id: 'log-5',
    timestamp: new Date(Date.now() - 1000 * 60 * 20).toISOString(),
    level: 'WARNING',
    deviceId: 'device-3',
    deviceName: 'Server-DB-01',
    message: 'Memory usage approaching threshold',
    source: 'memory_monitor'
  },
  {
    id: 'log-6',
    timestamp: new Date(Date.now() - 1000 * 60 * 180).toISOString(),
    level: 'CRITICAL',
    deviceId: 'device-8',
    deviceName: 'Router-Edge',
    message: 'Device not responding to health checks',
    source: 'connectivity_monitor'
  }
];

// RCA (Root Cause Analysis) examples
export const rcaExamples = [
  {
    id: 'rca-1',
    title: 'Switch-2 High CPU Usage',
    deviceId: 'device-2',
    deviceName: 'Switch-2',
    rootCause: 'VLAN 30 broadcast storm causing CPU spike',
    confidence: 92,
    timestamp: new Date(Date.now() - 1000 * 60 * 15).toISOString(),
    causeChain: [
      { step: 1, description: 'High CPU usage detected on Switch-2', severity: 'critical' },
      { step: 2, description: 'Elevated network traffic on eth0 interface', severity: 'major' },
      { step: 3, description: 'Broadcast packets flooding VLAN 30', severity: 'major' },
      { step: 4, description: 'Misconfigured device sending continuous broadcasts', severity: 'root' }
    ],
    evidence: [
      'CPU usage: 89%',
      'Interface eth0 traffic: 920 Mbps',
      'Broadcast packets: 15,000/sec',
      'VLAN 30 devices: 45 affected'
    ],
    affectedDevices: ['device-2', 'device-5', 'device-6'],
    impactLevel: 'high',
    suggestedFixes: [
      'Identify and isolate misconfigured device on VLAN 30',
      'Apply broadcast storm control on Switch-2',
      'Review VLAN 30 configuration and device list'
    ]
  },
  {
    id: 'rca-2',
    title: 'Router-Edge Connectivity Loss',
    deviceId: 'device-8',
    deviceName: 'Router-Edge',
    rootCause: 'Interface failure due to hardware malfunction',
    confidence: 87,
    timestamp: new Date(Date.now() - 1000 * 60 * 180).toISOString(),
    causeChain: [
      { step: 1, description: 'Router-Edge not responding', severity: 'critical' },
      { step: 2, description: 'Physical interface down', severity: 'major' },
      { step: 3, description: 'Hardware diagnostics show module failure', severity: 'root' }
    ],
    evidence: [
      'Device offline for 3 hours',
      'No response to ICMP ping',
      'Last known status: Interface error',
      'Hardware logs indicate module failure'
    ],
    affectedDevices: ['device-8'],
    impactLevel: 'medium',
    suggestedFixes: [
      'Replace faulty network interface module',
      'Schedule maintenance window for hardware replacement',
      'Configure backup routing path'
    ]
  }
];

// Automation rules
export const automationRules = [
  {
    id: 'rule-1',
    name: 'Auto-restart on interface flapping',
    description: 'Automatically restart interface if flapping detected',
    enabled: true,
    trigger: 'Interface flapping > 5 times in 10 minutes',
    action: 'Restart interface',
    appliedCount: 12,
    successRate: 94
  },
  {
    id: 'rule-2',
    name: 'Clear ARP cache on high CPU',
    description: 'Clear ARP cache when CPU exceeds 90%',
    enabled: true,
    trigger: 'CPU usage > 90%',
    action: 'Clear ARP cache',
    appliedCount: 8,
    successRate: 88
  },
  {
    id: 'rule-3',
    name: 'Restart services on memory threshold',
    description: 'Restart non-critical services when memory > 95%',
    enabled: false,
    trigger: 'Memory usage > 95%',
    action: 'Restart services',
    appliedCount: 3,
    successRate: 67
  }
];

// Automation actions ready for execution
export const pendingAutomations = [
  {
    id: 'auto-1',
    deviceId: 'device-2',
    deviceName: 'Switch-2',
    issue: 'High CPU usage - broadcast storm',
    recommendedAction: 'Apply broadcast storm control',
    confidence: 92,
    estimatedImpact: 'Low',
    estimatedTime: '30 seconds'
  },
  {
    id: 'auto-2',
    deviceId: 'device-3',
    deviceName: 'Server-DB-01',
    issue: 'High memory usage',
    recommendedAction: 'Clear cache and restart non-critical services',
    confidence: 85,
    estimatedImpact: 'Medium',
    estimatedTime: '2 minutes'
  },
  {
    id: 'auto-3',
    deviceId: 'device-6',
    deviceName: 'Virtual-Gateway',
    issue: 'Elevated latency',
    recommendedAction: 'Optimize routing table',
    confidence: 78,
    estimatedImpact: 'Low',
    estimatedTime: '1 minute'
  }
];

// AI chat history (mock)
export const aiChatHistory = [
  {
    id: 'chat-1',
    role: 'user',
    message: 'Why is there high CPU usage on Switch-2?',
    timestamp: new Date(Date.now() - 1000 * 60 * 10).toISOString()
  },
  {
    id: 'chat-2',
    role: 'assistant',
    message: 'High CPU usage is caused by a broadcast storm on VLAN 30. A misconfigured device is sending continuous broadcast packets at 15,000/sec, overwhelming the switch processor.',
    timestamp: new Date(Date.now() - 1000 * 60 * 9.5).toISOString(),
    context: {
      deviceId: 'device-2',
      metrics: { cpu: 89, traffic: 920 },
      suggestion: 'Apply broadcast storm control'
    }
  }
];

// Network summary stats
export const networkSummary = {
  overallHealth: 78,
  totalDevices: devices.length,
  onlineDevices: devices.filter(d => d.status !== DEVICE_STATUS.OFFLINE).length,
  offlineDevices: devices.filter(d => d.status === DEVICE_STATUS.OFFLINE).length,
  criticalAlerts: alerts.filter(a => a.severity === ALERT_SEVERITY.CRITICAL).length,
  majorAlerts: alerts.filter(a => a.severity === ALERT_SEVERITY.MAJOR).length,
  minorAlerts: alerts.filter(a => a.severity === ALERT_SEVERITY.MINOR).length,
  averageCpu: Math.round(devices.reduce((sum, d) => sum + d.cpu, 0) / devices.length),
  averageLatency: Math.round(devices.filter(d => d.status !== DEVICE_STATUS.OFFLINE).reduce((sum, d) => sum + d.latency, 0) / devices.filter(d => d.status !== DEVICE_STATUS.OFFLINE).length),
  currentLatency: 85
};
