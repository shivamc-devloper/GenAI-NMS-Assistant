import React, { useState } from 'react';
import { BarChart3, Download, Calendar, TrendingUp, TrendingDown, Activity } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../components/ui/select';
import { LineChart, Line, BarChart, Bar, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';
import { generateMetricsHistory, devices, alerts, DEVICE_STATUS, ALERT_SEVERITY } from '../data/mockData';

const Reports = () => {
  const [timeRange, setTimeRange] = useState('week');
  const metricsData = generateMetricsHistory(168); // 7 days

  // Calculate uptime percentage
  const onlineDevices = devices.filter(d => d.status !== DEVICE_STATUS.OFFLINE).length;
  const uptimePercentage = ((onlineDevices / devices.length) * 100).toFixed(1);

  // Weekly latency trend
  const weeklyLatency = metricsData.filter((_, idx) => idx % 7 === 0).map((data, idx) => ({
    day: `Day ${idx + 1}`,
    latency: data.latency
  }));

  // Bandwidth consumption trend
  const bandwidthData = metricsData.filter((_, idx) => idx % 7 === 0).map((data, idx) => ({
    day: `Day ${idx + 1}`,
    bandwidth: data.bandwidth
  }));

  // Device performance trends
  const devicePerformance = devices.slice(0, 6).map(device => ({
    name: device.name,
    performance: device.status === DEVICE_STATUS.OFFLINE ? 0 : 100 - device.cpu,
    cpu: device.cpu,
    memory: device.memory
  }));

  // AI actions success rate
  const aiActionsData = [
    { name: 'Week 1', success: 95, failed: 5 },
    { name: 'Week 2', success: 92, failed: 8 },
    { name: 'Week 3', success: 97, failed: 3 },
    { name: 'Week 4', success: 94, failed: 6 },
  ];

  // Alert distribution
  const alertDistribution = [
    { name: 'Critical', value: alerts.filter(a => a.severity === ALERT_SEVERITY.CRITICAL).length, color: '#ef4444' },
    { name: 'Major', value: alerts.filter(a => a.severity === ALERT_SEVERITY.MAJOR).length, color: '#f59e0b' },
    { name: 'Minor', value: alerts.filter(a => a.severity === ALERT_SEVERITY.MINOR).length, color: '#eab308' },
  ];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold mb-2">Reports & Trends</h1>
          <p className="text-gray-400">Long-term insights and performance analytics</p>
        </div>
        <div className="flex items-center gap-3">
          <Select value={timeRange} onValueChange={setTimeRange}>
            <SelectTrigger className="w-40 bg-[#111419] border-gray-700">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="week">Last 7 Days</SelectItem>
              <SelectItem value="month">Last 30 Days</SelectItem>
              <SelectItem value="quarter">Last 90 Days</SelectItem>
            </SelectContent>
          </Select>
          <Button className="bg-teal-600 hover:bg-teal-500">
            <Download className="h-4 w-4 mr-2" />
            Export Report
          </Button>
        </div>
      </div>

      {/* Key Metrics Summary */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card className="bg-gradient-to-br from-teal-950/30 to-cyan-950/30 border-teal-800/50">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-gray-400">Network Uptime</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-baseline gap-2">
              <span className="text-3xl font-bold text-teal-400">{uptimePercentage}%</span>
              <TrendingUp className="h-5 w-5 text-teal-400" />
            </div>
            <p className="text-xs text-gray-500 mt-2">+2.3% from last week</p>
          </CardContent>
        </Card>

        <Card className="bg-gradient-to-br from-cyan-950/30 to-blue-950/30 border-cyan-800/50">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-gray-400">Avg Latency</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-baseline gap-2">
              <span className="text-3xl font-bold text-cyan-400">24ms</span>
              <TrendingDown className="h-5 w-5 text-cyan-400" />
            </div>
            <p className="text-xs text-gray-500 mt-2">-12% improvement</p>
          </CardContent>
        </Card>

        <Card className="bg-gradient-to-br from-purple-950/30 to-pink-950/30 border-purple-800/50">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-gray-400">Total Alerts</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-baseline gap-2">
              <span className="text-3xl font-bold text-purple-400">{alerts.length}</span>
            </div>
            <p className="text-xs text-gray-500 mt-2">5 critical, 7 major</p>
          </CardContent>
        </Card>

        <Card className="bg-gradient-to-br from-orange-950/30 to-red-950/30 border-orange-800/50">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-gray-400">AI Actions</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-baseline gap-2">
              <span className="text-3xl font-bold text-orange-400">94.5%</span>
            </div>
            <p className="text-xs text-gray-500 mt-2">Success rate</p>
          </CardContent>
        </Card>
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Weekly Latency Trend */}
        <Card className="bg-[#111419] border-gray-800">
          <CardHeader>
            <CardTitle className="text-lg">Weekly Latency Trend</CardTitle>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={250}>
              <AreaChart data={weeklyLatency}>
                <defs>
                  <linearGradient id="colorLatencyTrend" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#06b6d4" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#06b6d4" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis dataKey="day" stroke="#9ca3af" style={{ fontSize: '12px' }} />
                <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
                  labelStyle={{ color: '#e5e7eb' }}
                />
                <Area type="monotone" dataKey="latency" stroke="#06b6d4" fill="url(#colorLatencyTrend)" strokeWidth={2} />
              </AreaChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        {/* Bandwidth Consumption */}
        <Card className="bg-[#111419] border-gray-800">
          <CardHeader>
            <CardTitle className="text-lg">Bandwidth Consumption Trend</CardTitle>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={250}>
              <LineChart data={bandwidthData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis dataKey="day" stroke="#9ca3af" style={{ fontSize: '12px' }} />
                <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
                  labelStyle={{ color: '#e5e7eb' }}
                />
                <Line type="monotone" dataKey="bandwidth" stroke="#f59e0b" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        {/* Device Performance Trends */}
        <Card className="bg-[#111419] border-gray-800">
          <CardHeader>
            <CardTitle className="text-lg">Device Performance Overview</CardTitle>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={250}>
              <BarChart data={devicePerformance}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis dataKey="name" stroke="#9ca3af" style={{ fontSize: '11px' }} angle={-15} textAnchor="end" height={60} />
                <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
                  labelStyle={{ color: '#e5e7eb' }}
                />
                <Bar dataKey="cpu" fill="#14b8a6" radius={[8, 8, 0, 0]} />
                <Bar dataKey="memory" fill="#06b6d4" radius={[8, 8, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        {/* Alert Distribution */}
        <Card className="bg-[#111419] border-gray-800">
          <CardHeader>
            <CardTitle className="text-lg">Alert Distribution</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center justify-center">
              <ResponsiveContainer width="100%" height={250}>
                <PieChart>
                  <Pie
                    data={alertDistribution}
                    cx="50%"
                    cy="50%"
                    innerRadius={60}
                    outerRadius={90}
                    paddingAngle={5}
                    dataKey="value"
                  >
                    {alertDistribution.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip
                    contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
                    labelStyle={{ color: '#e5e7eb' }}
                  />
                </PieChart>
              </ResponsiveContainer>
            </div>
            <div className="flex justify-center gap-6 mt-4">
              {alertDistribution.map((item) => (
                <div key={item.name} className="flex items-center gap-2">
                  <div className="w-3 h-3 rounded-full" style={{ backgroundColor: item.color }}></div>
                  <span className="text-sm text-gray-400">{item.name}: {item.value}</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* AI Actions Success Rate */}
      <Card className="bg-[#111419] border-gray-800">
        <CardHeader>
          <CardTitle className="text-lg">AI Automation Success Rate</CardTitle>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={aiActionsData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="name" stroke="#9ca3af" style={{ fontSize: '12px' }} />
              <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
                labelStyle={{ color: '#e5e7eb' }}
              />
              <Bar dataKey="success" stackId="a" fill="#14b8a6" radius={[8, 8, 0, 0]} />
              <Bar dataKey="failed" stackId="a" fill="#ef4444" radius={[8, 8, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

      {/* Summary Statistics */}
      <Card className="bg-[#111419] border-gray-800">
        <CardHeader>
          <CardTitle className="text-lg">Executive Summary</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            <div>
              <h4 className="text-sm font-semibold text-gray-400 mb-3">Network Health</h4>
              <ul className="space-y-2 text-sm text-gray-300">
                <li className="flex items-center justify-between">
                  <span>Overall uptime</span>
                  <span className="font-semibold text-teal-400">{uptimePercentage}%</span>
                </li>
                <li className="flex items-center justify-between">
                  <span>Active devices</span>
                  <span className="font-semibold text-teal-400">{onlineDevices}/{devices.length}</span>
                </li>
                <li className="flex items-center justify-between">
                  <span>Avg response time</span>
                  <span className="font-semibold text-cyan-400">24ms</span>
                </li>
              </ul>
            </div>

            <div>
              <h4 className="text-sm font-semibold text-gray-400 mb-3">Alert Management</h4>
              <ul className="space-y-2 text-sm text-gray-300">
                <li className="flex items-center justify-between">
                  <span>Total alerts</span>
                  <span className="font-semibold text-purple-400">{alerts.length}</span>
                </li>
                <li className="flex items-center justify-between">
                  <span>Resolved this week</span>
                  <span className="font-semibold text-teal-400">18</span>
                </li>
                <li className="flex items-center justify-between">
                  <span>Avg resolution time</span>
                  <span className="font-semibold text-orange-400">12 min</span>
                </li>
              </ul>
            </div>

            <div>
              <h4 className="text-sm font-semibold text-gray-400 mb-3">AI Performance</h4>
              <ul className="space-y-2 text-sm text-gray-300">
                <li className="flex items-center justify-between">
                  <span>Actions executed</span>
                  <span className="font-semibold text-teal-400">142</span>
                </li>
                <li className="flex items-center justify-between">
                  <span>Success rate</span>
                  <span className="font-semibold text-teal-400">94.5%</span>
                </li>
                <li className="flex items-center justify-between">
                  <span>Time saved</span>
                  <span className="font-semibold text-orange-400">28.5 hrs</span>
                </li>
              </ul>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

export default Reports;
