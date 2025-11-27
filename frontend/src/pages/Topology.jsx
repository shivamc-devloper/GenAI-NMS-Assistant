import React, { useState, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import ReactFlow, {
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  MarkerType,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { Card } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import { Input } from '../components/ui/input';
import { Search, Router, Server, Wifi, Camera, Box, Power } from 'lucide-react';
import { devices, connections, DEVICE_STATUS, DEVICE_TYPES } from '../data/mockData';

const DeviceNode = ({ data }) => {
  const getDeviceIcon = (type) => {
    switch (type) {
      case DEVICE_TYPES.ROUTER:
        return Router;
      case DEVICE_TYPES.SERVER:
        return Server;
      case DEVICE_TYPES.CAMERA:
        return Camera;
      case DEVICE_TYPES.ACCESS_POINT:
        return Wifi;
      default:
        return Box;
    }
  };

  const getStatusColor = (status) => {
    switch (status) {
      case DEVICE_STATUS.HEALTHY:
        return 'border-teal-500 shadow-teal-500/50';
      case DEVICE_STATUS.WARNING:
        return 'border-yellow-500 shadow-yellow-500/50';
      case DEVICE_STATUS.CRITICAL:
        return 'border-red-500 shadow-red-500/50';
      case DEVICE_STATUS.OFFLINE:
        return 'border-gray-600 shadow-gray-600/50';
      default:
        return 'border-gray-500';
    }
  };

  const Icon = getDeviceIcon(data.type);

  return (
    <div
      className={`px-4 py-3 rounded-lg bg-[#111419] border-2 ${getStatusColor(data.status)} shadow-lg transition-all hover:scale-105 cursor-pointer min-w-[140px]`}
      onClick={data.onClick}
    >
      <div className="flex items-center gap-2 mb-1">
        <Icon className="h-4 w-4 text-teal-400" />
        <div className="text-sm font-semibold text-gray-200">{data.label}</div>
      </div>
      <div className="text-xs text-gray-500">{data.ip}</div>
      {data.showMetrics && (
        <div className="mt-2 pt-2 border-t border-gray-700 space-y-0.5">
          <div className="text-xs text-gray-400">CPU: <span className="text-teal-400">{data.cpu}%</span></div>
          <div className="text-xs text-gray-400">Latency: <span className="text-cyan-400">{data.latency}ms</span></div>
        </div>
      )}
    </div>
  );
};

const nodeTypes = {
  deviceNode: DeviceNode,
};

const Topology = () => {
  const navigate = useNavigate();
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedDevice, setSelectedDevice] = useState(null);
  const [hoveredNode, setHoveredNode] = useState(null);

  const initialNodes = devices.map((device) => ({
    id: device.id,
    type: 'deviceNode',
    position: device.position,
    data: {
      label: device.name,
      type: device.type,
      status: device.status,
      ip: device.ip,
      cpu: device.cpu,
      latency: device.latency,
      showMetrics: hoveredNode === device.id,
      onClick: () => handleDeviceClick(device),
    },
  }));

  const initialEdges = connections.map((conn, idx) => {
    const sourceDevice = devices.find(d => d.id === conn.source);
    const targetDevice = devices.find(d => d.id === conn.target);
    
    // Edge color based on device status
    let strokeColor = '#14b8a6'; // teal
    if (sourceDevice?.status === DEVICE_STATUS.CRITICAL || targetDevice?.status === DEVICE_STATUS.CRITICAL) {
      strokeColor = '#ef4444'; // red
    } else if (sourceDevice?.status === DEVICE_STATUS.WARNING || targetDevice?.status === DEVICE_STATUS.WARNING) {
      strokeColor = '#f59e0b'; // orange
    } else if (sourceDevice?.status === DEVICE_STATUS.OFFLINE || targetDevice?.status === DEVICE_STATUS.OFFLINE) {
      strokeColor = '#6b7280'; // gray
    }

    return {
      id: `edge-${idx}`,
      source: conn.source,
      target: conn.target,
      type: 'smoothstep',
      animated: sourceDevice?.status !== DEVICE_STATUS.OFFLINE && targetDevice?.status !== DEVICE_STATUS.OFFLINE,
      style: { stroke: strokeColor, strokeWidth: 2 },
      markerEnd: {
        type: MarkerType.ArrowClosed,
        color: strokeColor,
      },
    };
  });

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  const handleDeviceClick = (device) => {
    setSelectedDevice(device);
  };

  const handleViewDetails = () => {
    if (selectedDevice) {
      navigate(`/device/${selectedDevice.id}`);
    }
  };

  const onNodeMouseEnter = useCallback((event, node) => {
    setHoveredNode(node.id);
    setNodes((nds) =>
      nds.map((n) => {
        if (n.id === node.id) {
          return {
            ...n,
            data: { ...n.data, showMetrics: true },
          };
        }
        return n;
      })
    );
  }, [setNodes]);

  const onNodeMouseLeave = useCallback((event, node) => {
    setHoveredNode(null);
    setNodes((nds) =>
      nds.map((n) => {
        if (n.id === node.id) {
          return {
            ...n,
            data: { ...n.data, showMetrics: false },
          };
        }
        return n;
      })
    );
  }, [setNodes]);

  const filteredDevices = devices.filter((device) =>
    device.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
    device.ip.includes(searchTerm) ||
    device.type.includes(searchTerm)
  );

  const getStatusBadge = (status) => {
    const colors = {
      [DEVICE_STATUS.HEALTHY]: 'bg-teal-600',
      [DEVICE_STATUS.WARNING]: 'bg-yellow-600',
      [DEVICE_STATUS.CRITICAL]: 'bg-red-600',
      [DEVICE_STATUS.OFFLINE]: 'bg-gray-600',
    };
    return <Badge className={`${colors[status]} text-white`}>{status}</Badge>;
  };

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold mb-2">Network Topology</h1>
          <p className="text-gray-400">Interactive visualization of network devices and connections</p>
        </div>
        <div className="flex items-center gap-2">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-gray-500" />
            <Input
              placeholder="Search devices..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="pl-10 w-64 bg-[#111419] border-gray-700"
            />
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-4 gap-4 h-[calc(100vh-200px)]">
        {/* Topology Map */}
        <Card className="lg:col-span-3 bg-[#111419] border-gray-800 overflow-hidden">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onNodeMouseEnter={onNodeMouseEnter}
            onNodeMouseLeave={onNodeMouseLeave}
            nodeTypes={nodeTypes}
            fitView
            className="bg-[#0a0d12]"
          >
            <Background color="#374151" gap={16} />
            <Controls className="bg-[#111419] border-gray-700" />
            <MiniMap
              nodeColor={(node) => {
                const device = devices.find(d => d.id === node.id);
                switch (device?.status) {
                  case DEVICE_STATUS.HEALTHY: return '#14b8a6';
                  case DEVICE_STATUS.WARNING: return '#f59e0b';
                  case DEVICE_STATUS.CRITICAL: return '#ef4444';
                  default: return '#6b7280';
                }
              }}
              className="bg-[#111419] border border-gray-700"
            />
          </ReactFlow>
        </Card>

        {/* Device Details Panel */}
        <Card className="bg-[#111419] border-gray-800 p-4 overflow-auto">
          {selectedDevice ? (
            <div className="space-y-4">
              <div>
                <h3 className="text-lg font-bold text-gray-200 mb-2">{selectedDevice.name}</h3>
                {getStatusBadge(selectedDevice.status)}
              </div>

              <div className="space-y-3">
                <div>
                  <div className="text-xs text-gray-500 mb-1">Device Type</div>
                  <div className="text-sm text-gray-200 capitalize">{selectedDevice.type.replace('_', ' ')}</div>
                </div>

                <div>
                  <div className="text-xs text-gray-500 mb-1">IP Address</div>
                  <div className="text-sm text-gray-200 font-mono">{selectedDevice.ip}</div>
                </div>

                <div>
                  <div className="text-xs text-gray-500 mb-1">MAC Address</div>
                  <div className="text-sm text-gray-200 font-mono">{selectedDevice.mac}</div>
                </div>

                <div>
                  <div className="text-xs text-gray-500 mb-1">Vendor</div>
                  <div className="text-sm text-gray-200">{selectedDevice.vendor}</div>
                </div>

                <div>
                  <div className="text-xs text-gray-500 mb-1">Location</div>
                  <div className="text-sm text-gray-200">{selectedDevice.location}</div>
                </div>

                {selectedDevice.status !== DEVICE_STATUS.OFFLINE && (
                  <>
                    <div className="pt-3 border-t border-gray-700">
                      <div className="text-xs text-gray-500 mb-2">Performance Metrics</div>
                      <div className="space-y-2">
                        <div>
                          <div className="flex justify-between text-xs mb-1">
                            <span className="text-gray-400">CPU Usage</span>
                            <span className="text-teal-400">{selectedDevice.cpu}%</span>
                          </div>
                        </div>
                        <div>
                          <div className="flex justify-between text-xs mb-1">
                            <span className="text-gray-400">Memory Usage</span>
                            <span className="text-cyan-400">{selectedDevice.memory}%</span>
                          </div>
                        </div>
                        <div>
                          <div className="flex justify-between text-xs mb-1">
                            <span className="text-gray-400">Latency</span>
                            <span className="text-purple-400">{selectedDevice.latency}ms</span>
                          </div>
                        </div>
                        <div>
                          <div className="flex justify-between text-xs mb-1">
                            <span className="text-gray-400">Packet Loss</span>
                            <span className="text-orange-400">{selectedDevice.packetLoss}%</span>
                          </div>
                        </div>
                      </div>
                    </div>

                    <div className="pt-3 border-t border-gray-700">
                      <div className="text-xs text-gray-500 mb-1">Uptime</div>
                      <div className="text-sm text-gray-200">{selectedDevice.uptime}</div>
                    </div>
                  </>
                )}
              </div>

              <Button
                onClick={handleViewDetails}
                className="w-full bg-teal-600 hover:bg-teal-500"
              >
                View Full Details
              </Button>
            </div>
          ) : (
            <div className="flex flex-col items-center justify-center h-full text-center">
              <Power className="h-12 w-12 text-gray-600 mb-3" />
              <p className="text-gray-400 text-sm">Click on a device to view details</p>
            </div>
          )}
        </Card>
      </div>
    </div>
  );
};

export default Topology;
