export interface Device {
  device_id: number;
  inserted?: string;
  hostname?: string;
  sysName?: string;
  display?: string | null;
  ip?: string;
  overwrite_ip?: string | null;
  community?: string;
  snmpver?: string;
  port?: number;
  transport?: string;
  snmp_disable?: boolean;
  sysObjectID?: string;
  sysDescr?: string;
  sysContact?: string;
  version?: string;
  hardware?: string;
  location_id?: number;
  os?: string;
  status?: boolean;
  status_reason?: string;
  ignore?: boolean;
  disabled?: boolean;
  uptime?: number;
  agent_uptime?: number;
  last_polled?: string;
  last_poll_attempted?: string | null;
  last_polled_timetaken?: number;
  last_discovered?: string;
  last_discovered_timetaken?: number;
  last_ping?: string;
  last_ping_timetaken?: number;
  type?: string;
  serial?: string | null;
  icon?: string;
  location?: string;
  lat?: number;
  lng?: number;
}

export interface DeviceDetailResponse {
  status: string;
  devices: Device[];
  count: number;
}

export interface MemoryUsage {
  type: string;
  used_gb: number;
  free_gb: number;
  total_gb: number;
  usage_percent: number;
}

export interface CpuCoreUsage {
  core: string;
  usage: number;
}

export interface CpuUsage {
  average: number;
  cores: CpuCoreUsage[];
}

export interface ApiError {
  status?: number;
  message: string;
}
