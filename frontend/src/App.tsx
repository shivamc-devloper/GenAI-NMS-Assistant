import { useEffect, useState, useMemo, type ReactNode, type FormEvent, type ChangeEvent } from 'react';
import type { Device, CpuUsage, MemoryUsage, ApiError } from '@/types';
import { deviceApi } from './services/api';
import { API } from './config/api.config';

// Helper Components
const InfoRow = ({ label, value }: { label: string; value?: string | number | null }) => (
  <div className="flex flex-col gap-0.5">
    <span className="text-xs font-medium text-muted">{label}</span>
    <span className="font-medium">{value ?? 'N/A'}</span>
  </div>
);

const MetricCard = ({
  title,
  children,
  loading,
}: {
  title: string;
  loading: boolean;
  children: ReactNode;
}) => (
  <div className="rounded-2xl border border-border bg-white p-6 shadow-soft">
    <h3 className="mb-4 text-sm font-medium uppercase tracking-wide text-muted">
      {title}
    </h3>
    {loading ? <SkeletonLines /> : children}
  </div>
);

const SkeletonLines = ({ count = 3 }: { count?: number }) => (
  <div className="space-y-3">
    {Array.from({ length: count }).map((_, i) => (
      <div key={i} className="h-4 w-full animate-pulse rounded bg-slate-100"></div>
    ))}
  </div>
);

// Format uptime for display
const formatUptime = (seconds?: number) => {
  if (!seconds || seconds <= 0) return 'Unknown';
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const parts = [
    days > 0 ? `${days}d` : null,
    hours > 0 ? `${hours}h` : null,
    minutes > 0 ? `${minutes}m` : null,
  ].filter(Boolean);
  return parts.join(' ') || `${Math.floor(seconds)}s`;
};

// Status color based on device status
const statusColor = (status?: boolean, disabled?: boolean) => {
  if (disabled) return 'bg-gray-400';
  if (status === false) return 'bg-red-500';
  return 'bg-green-500';
};

function App() {
  const [queryHost] = useState(API.getNetworkConfig().DEFAULT_IP);
  const [device, setDevice] = useState<Device | null>(null);
  const [cpu, setCpu] = useState<CpuUsage | null>(null);
  const [memory, setMemory] = useState<MemoryUsage[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<ApiError | null>(null);

  // Fetch device details and metrics
  useEffect(() => {
    let isMounted = true;
    
    const fetchDeviceData = async () => {
      if (!isMounted) return;
      
      try {
        setLoading(true);
        setError(null);
        
        // 1. Fetch device details
        const deviceData = await deviceApi.fetchDetails(queryHost);
        if (!isMounted) return;
        
        if (deviceData.devices?.length > 0) {
          const device = deviceData.devices[0];
          setDevice(device);
          
          // 2. Fetch memory and CPU usage in parallel
          try {
            const [memoryRes, cpuRes] = await Promise.all([
              deviceApi.fetchMemoryUsage(device.device_id),
              deviceApi.fetchCpuUsage(device.device_id),
            ]);

            if (!isMounted) return;
            setMemory(memoryRes ?? []);
            setCpu(cpuRes);
          } catch (apiError) {
            console.error('Error fetching metrics:', apiError);
            setError({ message: 'Failed to fetch device metrics' } as ApiError);
          }
        } else {
          setError({ message: 'No device found with the specified IP' } as ApiError);
        }
      } catch (err) {
        console.error('Error fetching device data:', err);
        setError({ 
          message: err instanceof Error ? err.message : 'Failed to fetch device data' 
        } as ApiError);
      } finally {
        if (isMounted) {
          setLoading(false);
        }
      }
    };

    fetchDeviceData();
    // Set up polling every 30 seconds
    const interval = setInterval(fetchDeviceData, 30000);
    
    return () => {
      isMounted = false;
      clearInterval(interval);
    };
  }, [queryHost]);

  const lastUpdated = useMemo(() => {
    if (!device?.last_polled) return 'N/A';
    return new Date(device.last_polled).toLocaleString();
  }, [device?.last_polled]);

  return (
    <main className="min-h-screen bg-background text-slate-900">
      <div className="mx-auto flex w-full max-w-6xl flex-col gap-8 px-4 py-8 md:px-8">
        <header className="rounded-3xl bg-card p-6 shadow-soft transition-transform duration-300 hover:-translate-y-1 hover:shadow-xl">
          <div className="flex flex-col gap-6 md:flex-row md:items-center md:justify-between">
            <div>
              <p className="text-sm font-medium uppercase tracking-wide text-muted">
                Device Detail Page
              </p>
              <h1 className="text-3xl font-semibold text-slate-900 md:text-4xl">
                {device?.sysName || device?.hostname || 'Device Details'}
              </h1>
            </div>
          </div>
          <div className="mt-6 grid gap-4 rounded-2xl border border-border bg-slate-50/60 p-4 sm:grid-cols-[auto_1fr]">
            <div className="flex flex-col gap-3 text-sm">
              <span className="inline-flex items-center gap-2 rounded-full bg-slate-100 px-3 py-1 text-xs font-medium uppercase tracking-wide text-muted">
                Status
              </span>
              <span className={`inline-flex w-fit items-center gap-2 rounded-full px-3 py-1 text-xs font-medium text-white ${statusColor(device?.status, device?.disabled)}`}>
                <span className="h-2 w-2 rounded-full bg-white" />
                {device?.status ? 'Healthy' : 'Down'}
              </span>
              <p className="text-xs text-muted">Last Updated: {lastUpdated}</p>
            </div>
            <div className="grid grid-cols-1 gap-3 text-sm md:grid-cols-2 lg:grid-cols-3">
              <InfoRow label="IP" value={device?.ip} />
              <InfoRow label="Hostname" value={device?.hostname} />
              <InfoRow label="Location" value={device?.location} />
              <InfoRow label="OS" value={device?.os} />
              <InfoRow label="Version" value={device?.version} />
              <InfoRow label="Uptime" value={formatUptime(device?.uptime)} />
            </div>
          </div>
        </header>

        <nav className="flex flex-wrap gap-3 rounded-3xl bg-card p-3 shadow-soft">
          {['Overview', 'Metrics', 'Interfaces', 'Logs', 'AI Insights', 'Actions'].map(
            (tab) => (
              <span
                key={tab}
                className="inline-flex animate-fadeInUp items-center justify-center rounded-2xl border border-border px-4 py-2 text-sm font-medium text-muted hover:border-primary hover:text-primary"
                style={{ animationDelay: `${Math.random() * 0.3}s` }}
              >
                {tab}
              </span>
            )
          )}
        </nav>

        <section className="grid gap-6 rounded-3xl bg-card p-6 shadow-soft">
          <h2 className="text-xl font-semibold text-slate-900">Metrics</h2>

          {error && (
            <div className="rounded-2xl border border-red-200 bg-red-50 p-4 text-sm text-red-600">
              {error.message || 'Unable to load device information. Please try again.'}
            </div>
          )}

          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            <MetricCard title="CPU Usage" loading={loading}>
              {cpu ? (
                <CpuUsageOverview cpu={cpu} />
              ) : (
                <SkeletonLines count={4} />
              )}
            </MetricCard>
            <MetricCard title="Memory Usage" loading={loading}>
              {memory.length ? (
                <MemoryUsageOverview data={memory} />
              ) : (
                <SkeletonLines count={4} />
              )}
            </MetricCard>
            <MetricCard title="Interface Traffic" loading={loading}>
              <TrafficPlaceholder />
            </MetricCard>
            <MetricCard title="Errors, Drops, Packet Loss" loading={loading}>
              <TrendPlaceholder />
            </MetricCard>
            <MetricCard title="Latency" loading={loading}>
              <TrendPlaceholder />
            </MetricCard>
            <MetricCard title="AI Insights" loading={loading}>
              <div className="space-y-3 text-sm text-muted">
                <p>Most likely issue</p>
                <p>Suggested remediation</p>
                <p className="text-xs">(Insight placeholder)</p>
              </div>
            </MetricCard>
          </div>

          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            {[
              'Restart Device',
              'Reset Interface',
              'Clear Tables',
              'Apply Automatic Fixes',
            ].map((action, index) => (
              <button
                key={action}
                className="group inline-flex items-center justify-center rounded-2xl border border-border bg-slate-50 px-4 py-3 text-sm font-medium text-slate-700 transition-all duration-200 hover:-translate-y-0.5 hover:border-primary hover:bg-white hover:text-primary focus-visible:outline focus-visible:outline-2 focus-visible:outline-primary/60"
                style={{ animation: `fadeInUp 0.45s ease ${0.1 * index}s both` }}
              >
                <span className="relative flex items-center gap-2">
                  <span className="h-2 w-2 rounded-full bg-primary/40 transition-colors group-hover:bg-primary" />
                  {action}
                </span>
              </button>
            ))}
          </div>
        </section>
      </div>
    </main>
  );
}

export default App;

function CpuUsageOverview({ cpu }: { cpu: CpuUsage }) {
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between text-sm">
        <p className="font-medium text-slate-900">Average</p>
        <span className="flex items-center gap-2 text-primary">
          <span className="h-3 w-3 rounded-full bg-primary/50" />
          {cpu.average.toFixed(1)}%
        </span>
      </div>
      <div className="space-y-2 text-sm text-muted">
        {cpu.cores.map((core, index) => (
          <div key={`${core.core}-${index}`} className="flex items-center justify-between">
            <span>{core.core}</span>
            <span className="font-medium text-slate-900">{core.usage.toFixed(1)}%</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function MemoryUsageOverview({ data }: { data: MemoryUsage[] }) {
  return (
    <ul className="space-y-3 text-sm">
      {data.map((item) => (
        <li key={item.type} className="rounded-2xl bg-slate-50/80 p-3">
          <div className="flex items-center justify-between text-slate-900">
            <span className="font-medium">{item.type}</span>
            <span className="text-xs text-muted">{item.usage_percent}%</span>
          </div>
          <div className="mt-2 h-2 w-full rounded-full bg-slate-200">
            <div
              className="h-full rounded-full bg-primary transition-all duration-500"
              style={{ width: `${item.usage_percent}%` }}
            />
          </div>
          <p className="mt-2 text-xs text-muted">
            Used {item.used_gb.toFixed(2)} GB of {item.total_gb.toFixed(2)} GB
          </p>
        </li>
      ))}
    </ul>
  );
}

function TrafficPlaceholder() {
  return (
    <div className="flex h-full flex-col justify-between">
      <div className="space-y-3">
        {[1, 2].map((port) => (
          <div key={port} className="space-y-2">
            <p className="text-sm font-medium text-slate-900">Port {port}</p>
            <div className="flex items-end gap-1">
              {[35, 60, 45, 55].map((height, idx) => (
                <span
                  key={idx}
                  className={`flex-1 rounded-t-xl bg-primary/30 ${height === 60 ? 'animate-pulseGlow' : ''}`}
                  style={{ height: `${height}%` }}
                />
              ))}
            </div>
          </div>
        ))}
      </div>
      <p className="mt-3 text-xs text-muted">Live interface metrics coming soon.</p>
    </div>
  );
}

function TrendPlaceholder() {
  return (
    <div className="relative h-full">
      <svg viewBox="0 0 300 120" className="h-full w-full">
        <path
          d="M10 90 C 60 60, 90 100, 140 70 S 220 50, 280 80"
          className="fill-none stroke-primary/40"
          strokeWidth={8}
          strokeLinecap="round"
        />
        <path
          d="M10 90 C 60 60, 90 100, 140 70 S 220 50, 280 80"
          className="fill-none stroke-primary"
          strokeWidth={3}
          strokeLinecap="round"
        />
      </svg>
      <p className="absolute bottom-0 left-0 text-xs text-muted">
        Historical trend placeholder.
      </p>
    </div>
  );
}
