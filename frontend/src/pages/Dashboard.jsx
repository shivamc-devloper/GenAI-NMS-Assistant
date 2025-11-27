import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { 
  Activity, 
  Server, 
  AlertTriangle, 
  Clock, 
  TrendingUp,
  Wifi,
  ArrowRight
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Progress } from '../components/ui/progress';
import { Badge } from '../components/ui/badge';
import { LineChart, Line, AreaChart, Area, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { networkSummary, alerts, devices, generateMetricsHistory, ALERT_SEVERITY, DEVICE_STATUS } from '../data/mockData';

const Dashboard = () => {
  const navigate = useNavigate();
  const [metricsData, setMetricsData] = useState(generateMetricsHistory(24));
  const [selectedTimeRange, setSelectedTimeRange] = useState('24h');

  useEffect(() => {
    const interval = setInterval(() => {
      setMetricsData(prev => {
        const newData = [...prev.slice(1)];
        const lastTime = new Date();
        newData.push({
          time: lastTime.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' }),
          timestamp: lastTime.toISOString(),
          traffic: 600 + Math.random() * 400,
          latency: 15 + Math.random() * 30,
          cpu: 40 + Math.random() * 30,
          memory: 50 + Math.random() * 20,
          bandwidth: 700 + Math.random() * 300
        });
        return newData;
      });
    }, 5000);

    return () => clearInterval(interval);
  }, []);

  const recentAlerts = alerts.slice(0, 5);
  const topBandwidthDevices = [...devices]
    .filter(d => d.status !== DEVICE_STATUS.OFFLINE)
    .sort((a, b) => b.interfaces[0]?.traffic - a.interfaces[0]?.traffic)
    .slice(0, 5);
  const topCpuDevices = [...devices]
    .filter(d => d.status !== DEVICE_STATUS.OFFLINE)
    .sort((a, b) => b.cpu - a.cpu)
    .slice(0, 5);

  const getSeverityColor = (severity) => {
    switch (severity) {
      case ALERT_SEVERITY.CRITICAL:
        return 'bg-red-500';
      case ALERT_SEVERITY.MAJOR:
        return 'bg-orange-500';
      case ALERT_SEVERITY.MINOR:
        return 'bg-yellow-500';
      default:
        return 'bg-blue-500';
    }
  };

  const getHealthColor = (health) => {
    if (health >= 80) return 'text-teal-400';
    if (health >= 60) return 'text-yellow-400';
    return 'text-red-400';
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold mb-2">Network Dashboard</h1>
        <p className="text-gray-400">Real-time network health and performance monitoring</p>
      </div>

      {/* Network Health Summary */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
        <Card className="bg-[#111419] border-gray-800">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-gray-400">Overall Network Health</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-baseline gap-2">
              <span className={`text-3xl font-bold ${getHealthColor(networkSummary.overallHealth)}`}>
                {networkSummary.overallHealth}%
              </span>
            </div>
            <Progress value={networkSummary.overallHealth} className="mt-3 h-2" />
          </CardContent>
        </Card>

        <Card className="bg-[#111419] border-gray-800">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-gray-400">Device Up/Down Count</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center justify-between">
              <div>
                <div className="text-3xl font-bold text-teal-400">{networkSummary.onlineDevices}</div>
                <div className="text-xs text-gray-500">Online</div>
              </div>
              <div>
                <div className="text-3xl font-bold text-red-400">{networkSummary.offlineDevices}</div>
                <div className="text-xs text-gray-500">Offline</div>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="bg-[#111419] border-gray-800">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-gray-400">Alerts (Critical/Warning/Info)</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center gap-3">
              <div className="flex items-baseline gap-1">
                <span className="text-2xl font-bold text-red-400">{networkSummary.criticalAlerts}</span>
              </div>
              <div className="flex items-baseline gap-1">
                <span className="text-2xl font-bold text-orange-400">{networkSummary.majorAlerts}</span>
              </div>
              <div className="flex items-baseline gap-1">
                <span className="text-2xl font-bold text-yellow-400">{networkSummary.minorAlerts}</span>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="bg-[#111419] border-gray-800">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-gray-400">Average Latency</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-baseline gap-2">
              <span className="text-3xl font-bold text-cyan-400">{networkSummary.averageLatency}</span>
              <span className="text-gray-400 text-sm">ms</span>
            </div>
          </CardContent>
        </Card>

        <Card className="bg-[#111419] border-gray-800">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-gray-400">Average CPU</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-baseline gap-2">
              <span className="text-3xl font-bold text-purple-400">{networkSummary.averageCpu}</span>
              <span className="text-gray-400 text-sm">%</span>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Charts Section */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Traffic Graph */}
        <Card className="bg-[#111419] border-gray-800">
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle className="text-lg">Network Traffic</CardTitle>
              <div className="flex gap-2">
                {['1h', '24h', '7d'].map((range) => (
                  <Button
                    key={range}
                    variant={selectedTimeRange === range ? 'default' : 'ghost'}
                    size="sm"
                    onClick={() => setSelectedTimeRange(range)}
                    className={selectedTimeRange === range ? 'bg-teal-600 hover:bg-teal-500' : ''}
                  >
                    {range}
                  </Button>
                ))}
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={200}>
              <AreaChart data={metricsData}>
                <defs>
                  <linearGradient id="colorTraffic" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#14b8a6" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#14b8a6" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis dataKey="time" stroke="#9ca3af" style={{ fontSize: '12px' }} />
                <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
                  labelStyle={{ color: '#e5e7eb' }}
                />
                <Area type="monotone" dataKey="traffic" stroke="#14b8a6" fill="url(#colorTraffic)" strokeWidth={2} />
              </AreaChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        {/* Latency Trend */}
        <Card className="bg-[#111419] border-gray-800">
          <CardHeader>
            <CardTitle className="text-lg">Latency Trend</CardTitle>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={metricsData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis dataKey="time" stroke="#9ca3af" style={{ fontSize: '12px' }} />
                <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
                  labelStyle={{ color: '#e5e7eb' }}
                />
                <Line type="monotone" dataKey="latency" stroke="#06b6d4" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        {/* Top Bandwidth Consumers */}
        <Card className="bg-[#111419] border-gray-800">
          <CardHeader>
            <CardTitle className="text-lg">Top Bandwidth Consumers</CardTitle>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={topBandwidthDevices}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis dataKey="name" stroke="#9ca3af" style={{ fontSize: '11px' }} angle={-15} textAnchor="end" height={60} />
                <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
                  labelStyle={{ color: '#e5e7eb' }}
                />
                <Bar dataKey="interfaces[0].traffic" fill="#8b5cf6" radius={[8, 8, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        {/* Top CPU Devices */}
        <Card className="bg-[#111419] border-gray-800">
          <CardHeader>
            <CardTitle className="text-lg">Top CPU Usage Devices</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {topCpuDevices.map((device) => (
                <div key={device.id} className="flex items-center justify-between">
                  <div className="flex-1">
                    <div className="text-sm font-medium text-gray-200">{device.name}</div>
                    <Progress value={device.cpu} className="mt-1 h-2" />
                  </div>
                  <span className="ml-3 text-sm font-semibold text-purple-400">{device.cpu}%</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Recent Alerts & AI Recommendations */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Recent Alerts */}
        <Card className="bg-[#111419] border-gray-800">
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle className="text-lg">Recent Alerts</CardTitle>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => navigate('/alerts')}
                className="text-teal-400 hover:text-teal-300"
              >
                View All
                <ArrowRight className="ml-1 h-4 w-4" />
              </Button>
            </div>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {recentAlerts.map((alert) => (
                <div key={alert.id} className="flex items-start gap-3 p-3 bg-gray-900/50 rounded-lg border border-gray-800 hover:border-gray-700 transition-colors">
                  <div className={`w-1 h-full ${getSeverityColor(alert.severity)} rounded-full`}></div>
                  <AlertTriangle className={`h-5 w-5 flex-shrink-0 mt-0.5 ${
                    alert.severity === ALERT_SEVERITY.CRITICAL ? 'text-red-400' :
                    alert.severity === ALERT_SEVERITY.MAJOR ? 'text-orange-400' : 'text-yellow-400'
                  }`} />
                  <div className="flex-1 min-w-0">
                    <div className="text-sm font-medium text-gray-200 truncate">{alert.title}</div>
                    <div className="text-xs text-gray-500 mt-0.5">{alert.deviceName}</div>
                  </div>
                  <div className="text-xs text-gray-500">
                    {new Date(alert.timestamp).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })}
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        {/* AI Recommendations */}
        <Card className="bg-gradient-to-br from-teal-950/30 to-cyan-950/30 border-teal-800/50">
          <CardHeader>
            <CardTitle className="text-lg flex items-center gap-2">
              <Activity className="h-5 w-5 text-teal-400" />
              AI Recommendations
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <div className="p-4 bg-gray-900/50 rounded-lg border border-teal-800/30">
                <div className="flex items-start gap-3">
                  <div className="p-2 bg-teal-600/20 rounded-lg">
                    <AlertTriangle className="h-5 w-5 text-teal-400" />
                  </div>
                  <div className="flex-1">
                    <h4 className="text-sm font-semibold text-gray-200 mb-1">Address Switch-2 Broadcast Storm</h4>
                    <p className="text-xs text-gray-400 mb-2">
                      VLAN 30 experiencing broadcast storm causing 89% CPU usage. Apply broadcast storm control immediately.
                    </p>
                    <div className="flex items-center gap-2">
                      <Badge variant="outline" className="text-xs border-teal-600/50 text-teal-400">Priority: High</Badge>
                      <Badge variant="outline" className="text-xs border-gray-700 text-gray-400">Impact: 3 devices</Badge>
                    </div>
                  </div>
                </div>
              </div>

              <div className="p-4 bg-gray-900/50 rounded-lg border border-yellow-800/30">
                <div className="flex items-start gap-3">
                  <div className="p-2 bg-yellow-600/20 rounded-lg">
                    <Server className="h-5 w-5 text-yellow-400" />
                  </div>
                  <div className="flex-1">
                    <h4 className="text-sm font-semibold text-gray-200 mb-1">Optimize Server-DB-01 Memory</h4>
                    <p className="text-xs text-gray-400 mb-2">
                      Memory usage at 85%. Consider clearing cache or restarting non-critical services.
                    </p>
                    <div className="flex items-center gap-2">
                      <Badge variant="outline" className="text-xs border-yellow-600/50 text-yellow-400">Priority: Medium</Badge>
                    </div>
                  </div>
                </div>
              </div>

              <Button
                onClick={() => navigate('/automation')}
                className="w-full bg-teal-600 hover:bg-teal-500"
              >
                View Automation Options
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
};

export default Dashboard;
