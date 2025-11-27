import React, { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, Activity, Wifi, HardDrive, Clock, AlertTriangle, Terminal, Play } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../components/ui/tabs';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { devices, deviceLogs, rcaExamples, generateMetricsHistory, DEVICE_STATUS } from '../data/mockData';

const DeviceDetail = () => {
  const { deviceId } = useParams();
  const navigate = useNavigate();
  const [selectedTab, setSelectedTab] = useState('overview');
  
  const device = devices.find(d => d.id === deviceId);
  const deviceRCA = rcaExamples.find(rca => rca.deviceId === deviceId);
  const deviceLogEntries = deviceLogs.filter(log => log.deviceId === deviceId);
  const metricsData = generateMetricsHistory(24);

  if (!device) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="text-center">
          <p className="text-gray-400">Device not found</p>
          <Button onClick={() => navigate('/topology')} className="mt-4 bg-teal-600 hover:bg-teal-500">
            Back to Topology
          </Button>
        </div>
      </div>
    );
  }

  const getStatusColor = (status) => {
    switch (status) {
      case DEVICE_STATUS.HEALTHY:
        return 'bg-teal-600';
      case DEVICE_STATUS.WARNING:
        return 'bg-yellow-600';
      case DEVICE_STATUS.CRITICAL:
        return 'bg-red-600';
      case DEVICE_STATUS.OFFLINE:
        return 'bg-gray-600';
      default:
        return 'bg-gray-600';
    }
  };

  const getLogLevelColor = (level) => {
    switch (level) {
      case 'CRITICAL':
        return 'text-red-400';
      case 'ERROR':
        return 'text-orange-400';
      case 'WARNING':
        return 'text-yellow-400';
      case 'INFO':
        return 'text-blue-400';
      default:
        return 'text-gray-400';
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <Button
            variant="ghost"
            size="icon"
            onClick={() => navigate('/topology')}
            className="hover:bg-gray-800"
          >
            <ArrowLeft className="h-5 w-5" />
          </Button>
          <div>
            <h1 className="text-3xl font-bold">{device.name}</h1>
            <p className="text-gray-400 mt-1">{device.ip} • {device.location}</p>
          </div>
        </div>
        <Badge className={`${getStatusColor(device.status)} text-white px-4 py-2 text-sm`}>
          {device.status.toUpperCase()}
        </Badge>
      </div>

      {/* Tabs */}
      <Tabs value={selectedTab} onValueChange={setSelectedTab} className="w-full">
        <TabsList className="bg-[#111419] border border-gray-800">
          <TabsTrigger value="overview" className="data-[state=active]:bg-teal-600">
            Overview
          </TabsTrigger>
          <TabsTrigger value="metrics" className="data-[state=active]:bg-teal-600">
            Metrics
          </TabsTrigger>
          <TabsTrigger value="interfaces" className="data-[state=active]:bg-teal-600">
            Interfaces
          </TabsTrigger>
          <TabsTrigger value="logs" className="data-[state=active]:bg-teal-600">
            Logs
          </TabsTrigger>
          <TabsTrigger value="insights" className="data-[state=active]:bg-teal-600">
            AI Insights
          </TabsTrigger>
          <TabsTrigger value="actions" className="data-[state=active]:bg-teal-600">
            Actions
          </TabsTrigger>
        </TabsList>

        {/* Overview Tab */}
        <TabsContent value="overview" className="space-y-4 mt-6">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <Card className="bg-[#111419] border-gray-800">
              <CardHeader className="pb-3">
                <CardTitle className="text-sm font-medium text-gray-400">Device Type</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-lg font-semibold text-gray-200 capitalize">
                  {device.type.replace('_', ' ')}
                </div>
              </CardContent>
            </Card>

            <Card className="bg-[#111419] border-gray-800">
              <CardHeader className="pb-3">
                <CardTitle className="text-sm font-medium text-gray-400">Vendor</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-lg font-semibold text-gray-200">{device.vendor}</div>
              </CardContent>
            </Card>

            <Card className="bg-[#111419] border-gray-800">
              <CardHeader className="pb-3">
                <CardTitle className="text-sm font-medium text-gray-400">MAC Address</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-lg font-semibold text-gray-200 font-mono">{device.mac}</div>
              </CardContent>
            </Card>

            <Card className="bg-[#111419] border-gray-800">
              <CardHeader className="pb-3">
                <CardTitle className="text-sm font-medium text-gray-400">Uptime</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-lg font-semibold text-gray-200">{device.uptime}</div>
              </CardContent>
            </Card>
          </div>

          {device.status !== DEVICE_STATUS.OFFLINE && (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
              <Card className="bg-gradient-to-br from-teal-950/30 to-cyan-950/30 border-teal-800/50">
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium text-gray-400 flex items-center gap-2">
                    <Activity className="h-4 w-4" />
                    CPU Usage
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-3xl font-bold text-teal-400">{device.cpu}%</div>
                </CardContent>
              </Card>

              <Card className="bg-gradient-to-br from-cyan-950/30 to-blue-950/30 border-cyan-800/50">
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium text-gray-400 flex items-center gap-2">
                    <HardDrive className="h-4 w-4" />
                    Memory Usage
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-3xl font-bold text-cyan-400">{device.memory}%</div>
                </CardContent>
              </Card>

              <Card className="bg-gradient-to-br from-purple-950/30 to-pink-950/30 border-purple-800/50">
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium text-gray-400 flex items-center gap-2">
                    <Clock className="h-4 w-4" />
                    Latency
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-3xl font-bold text-purple-400">{device.latency}ms</div>
                </CardContent>
              </Card>

              <Card className="bg-gradient-to-br from-orange-950/30 to-red-950/30 border-orange-800/50">
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium text-gray-400 flex items-center gap-2">
                    <Wifi className="h-4 w-4" />
                    Packet Loss
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-3xl font-bold text-orange-400">{device.packetLoss}%</div>
                </CardContent>
              </Card>
            </div>
          )}
        </TabsContent>

        {/* Metrics Tab */}
        <TabsContent value="metrics" className="space-y-4 mt-6">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <Card className="bg-[#111419] border-gray-800">
              <CardHeader>
                <CardTitle className="text-lg">CPU Usage (24h)</CardTitle>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={250}>
                  <LineChart data={metricsData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                    <XAxis dataKey="time" stroke="#9ca3af" style={{ fontSize: '12px' }} />
                    <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} />
                    <Tooltip
                      contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
                      labelStyle={{ color: '#e5e7eb' }}
                    />
                    <Line type="monotone" dataKey="cpu" stroke="#14b8a6" strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>

            <Card className="bg-[#111419] border-gray-800">
              <CardHeader>
                <CardTitle className="text-lg">Memory Usage (24h)</CardTitle>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={250}>
                  <LineChart data={metricsData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                    <XAxis dataKey="time" stroke="#9ca3af" style={{ fontSize: '12px' }} />
                    <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} />
                    <Tooltip
                      contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
                      labelStyle={{ color: '#e5e7eb' }}
                    />
                    <Line type="monotone" dataKey="memory" stroke="#06b6d4" strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>

            <Card className="bg-[#111419] border-gray-800">
              <CardHeader>
                <CardTitle className="text-lg">Latency (24h)</CardTitle>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={250}>
                  <LineChart data={metricsData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                    <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} />
                    <Tooltip
                      contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
                      labelStyle={{ color: '#e5e7eb' }}
                    />
                    <Line type="monotone" dataKey="latency" stroke="#a855f7" strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>

            <Card className="bg-[#111419] border-gray-800">
              <CardHeader>
                <CardTitle className="text-lg">Bandwidth (24h)</CardTitle>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={250}>
                  <LineChart data={metricsData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                    <XAxis dataKey="time" stroke="#9ca3af" style={{ fontSize: '12px' }} />
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
          </div>
        </TabsContent>

        {/* Interfaces Tab */}
        <TabsContent value="interfaces" className="mt-6">
          <Card className="bg-[#111419] border-gray-800">
            <CardHeader>
              <CardTitle className="text-lg">Network Interfaces</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                {device.interfaces.map((iface, idx) => (
                  <div key={idx} className="p-4 bg-gray-900/50 rounded-lg border border-gray-800">
                    <div className="flex items-center justify-between mb-3">
                      <div className="flex items-center gap-3">
                        <Wifi className="h-5 w-5 text-teal-400" />
                        <div>
                          <div className="text-sm font-semibold text-gray-200">{iface.name}</div>
                          <Badge className={`mt-1 ${iface.status === 'up' ? 'bg-teal-600' : 'bg-gray-600'} text-white`}>
                            {iface.status.toUpperCase()}
                          </Badge>
                        </div>
                      </div>
                      <div className="text-right">
                        <div className="text-sm text-gray-400">Traffic</div>
                        <div className="text-lg font-bold text-cyan-400">{iface.traffic} Mbps</div>
                      </div>
                    </div>
                    {iface.errors > 0 && (
                      <div className="flex items-center gap-2 mt-2 p-2 bg-red-950/30 rounded border border-red-800/30">
                        <AlertTriangle className="h-4 w-4 text-red-400" />
                        <span className="text-sm text-red-400">{iface.errors} errors detected</span>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Logs Tab */}
        <TabsContent value="logs" className="mt-6">
          <Card className="bg-[#111419] border-gray-800">
            <CardHeader>
              <CardTitle className="text-lg flex items-center gap-2">
                <Terminal className="h-5 w-5" />
                System Logs
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-2 font-mono text-xs">
                {deviceLogEntries.map((log) => (
                  <div key={log.id} className="p-3 bg-gray-900/50 rounded border border-gray-800 hover:border-gray-700 transition-colors">
                    <div className="flex items-start gap-3">
                      <span className="text-gray-500">
                        {new Date(log.timestamp).toLocaleString()}
                      </span>
                      <span className={`font-semibold ${getLogLevelColor(log.level)}`}>
                        {log.level}
                      </span>
                      <span className="text-gray-400">[{log.source}]</span>
                      <span className="text-gray-300 flex-1">{log.message}</span>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        {/* AI Insights Tab */}
        <TabsContent value="insights" className="mt-6">
          <Card className="bg-[#111419] border-gray-800">
            <CardHeader>
              <CardTitle className="text-lg">AI-Powered Insights</CardTitle>
            </CardHeader>
            <CardContent>
              {deviceRCA ? (
                <div className="space-y-4">
                  <div className="p-4 bg-gradient-to-br from-red-950/30 to-orange-950/30 border border-red-800/30 rounded-lg">
                    <h3 className="text-sm font-semibold text-gray-200 mb-2">Most Likely Issue</h3>
                    <p className="text-gray-300">{deviceRCA.rootCause}</p>
                    <div className="mt-3">
                      <Badge className="bg-teal-600 text-white">Confidence: {deviceRCA.confidence}%</Badge>
                    </div>
                  </div>

                  <div>
                    <h3 className="text-sm font-semibold text-gray-200 mb-3">Suggested Remediation</h3>
                    <div className="space-y-2">
                      {deviceRCA.suggestedFixes.map((fix, idx) => (
                        <div key={idx} className="p-3 bg-gray-900/50 rounded-lg border border-gray-800 flex items-center gap-3">
                          <div className="flex-shrink-0 w-6 h-6 rounded-full bg-teal-600/20 flex items-center justify-center">
                            <span className="text-xs font-bold text-teal-400">{idx + 1}</span>
                          </div>
                          <span className="text-sm text-gray-300">{fix}</span>
                        </div>
                      ))}
                    </div>
                  </div>

                  <div>
                    <h3 className="text-sm font-semibold text-gray-200 mb-3">Historical Issues</h3>
                    <p className="text-sm text-gray-400">
                      This device has experienced similar issues 3 times in the past 30 days. 
                      Consider implementing permanent configuration changes.
                    </p>
                  </div>
                </div>
              ) : (
                <div className="text-center py-8">
                  <p className="text-gray-400">No AI insights available for this device at the moment.</p>
                  <p className="text-sm text-gray-500 mt-2">AI continuously monitors device behavior and will provide insights when patterns are detected.</p>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Actions Tab */}
        <TabsContent value="actions" className="mt-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <Card className="bg-[#111419] border-gray-800">
              <CardHeader>
                <CardTitle className="text-lg">Device Actions</CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <Button className="w-full justify-start bg-teal-600 hover:bg-teal-500">
                  <Play className="h-4 w-4 mr-2" />
                  Restart Device
                </Button>
                <Button className="w-full justify-start" variant="outline">
                  <Activity className="h-4 w-4 mr-2" />
                  Reset Interface
                </Button>
                <Button className="w-full justify-start" variant="outline">
                  <Terminal className="h-4 w-4 mr-2" />
                  Clear Tables
                </Button>
              </CardContent>
            </Card>

            <Card className="bg-[#111419] border-gray-800">
              <CardHeader>
                <CardTitle className="text-lg">Automated Fixes</CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                {deviceRCA ? (
                  <>
                    <div className="p-3 bg-gradient-to-br from-teal-950/30 to-cyan-950/30 border border-teal-800/30 rounded-lg">
                      <p className="text-sm text-gray-300 mb-3">
                        AI has identified {deviceRCA.suggestedFixes.length} automated fixes for current issues.
                      </p>
                      <Button className="w-full bg-teal-600 hover:bg-teal-500">
                        Apply Automatic Fixes
                      </Button>
                    </div>
                    <p className="text-xs text-gray-500">
                      Estimated impact: {deviceRCA.impactLevel} • Confidence: {deviceRCA.confidence}%
                    </p>
                  </>
                ) : (
                  <p className="text-sm text-gray-400">No automated fixes available at this time.</p>
                )}
              </CardContent>
            </Card>
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
};

export default DeviceDetail;
