import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { AlertTriangle, Filter, ChevronDown, ChevronRight, Clock, X } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import { Input } from '../components/ui/input';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../components/ui/select';
import { alerts, ALERT_SEVERITY, devices } from '../data/mockData';

const Alerts = () => {
  const navigate = useNavigate();
  const [selectedSeverity, setSelectedSeverity] = useState('all');
  const [selectedDevice, setSelectedDevice] = useState('all');
  const [searchTerm, setSearchTerm] = useState('');
  const [expandedGroups, setExpandedGroups] = useState(new Set());
  const [selectedAlert, setSelectedAlert] = useState(null);

  // Group alerts by device and related alerts
  const groupAlerts = () => {
    const groups = {};
    
    alerts.forEach(alert => {
      if (alert.relatedAlerts && alert.relatedAlerts.length > 0) {
        // Check if this alert is already in a group
        const existingGroupKey = Object.keys(groups).find(key => {
          const group = groups[key];
          return group.some(a => a.id === alert.id || alert.relatedAlerts.includes(a.id));
        });

        if (existingGroupKey) {
          // Add to existing group if not already there
          if (!groups[existingGroupKey].some(a => a.id === alert.id)) {
            groups[existingGroupKey].push(alert);
          }
        } else {
          // Create new group
          groups[alert.id] = [alert];
          // Add related alerts
          alert.relatedAlerts.forEach(relatedId => {
            const relatedAlert = alerts.find(a => a.id === relatedId);
            if (relatedAlert && !groups[alert.id].some(a => a.id === relatedId)) {
              groups[alert.id].push(relatedAlert);
            }
          });
        }
      } else {
        // Single alert
        groups[alert.id] = [alert];
      }
    });

    return Object.values(groups);
  };

  const alertGroups = groupAlerts();

  const filteredGroups = alertGroups.filter(group => {
    const mainAlert = group[0];
    
    if (selectedSeverity !== 'all' && mainAlert.severity !== selectedSeverity) {
      return false;
    }
    
    if (selectedDevice !== 'all' && mainAlert.deviceId !== selectedDevice) {
      return false;
    }
    
    if (searchTerm && !mainAlert.title.toLowerCase().includes(searchTerm.toLowerCase()) &&
        !mainAlert.description.toLowerCase().includes(searchTerm.toLowerCase())) {
      return false;
    }
    
    return true;
  });

  const toggleGroup = (groupId) => {
    const newExpanded = new Set(expandedGroups);
    if (newExpanded.has(groupId)) {
      newExpanded.delete(groupId);
    } else {
      newExpanded.add(groupId);
    }
    setExpandedGroups(newExpanded);
  };

  const getSeverityColor = (severity) => {
    switch (severity) {
      case ALERT_SEVERITY.CRITICAL:
        return 'bg-red-600';
      case ALERT_SEVERITY.MAJOR:
        return 'bg-orange-600';
      case ALERT_SEVERITY.MINOR:
        return 'bg-yellow-600';
      default:
        return 'bg-blue-600';
    }
  };

  const getSeverityBorder = (severity) => {
    switch (severity) {
      case ALERT_SEVERITY.CRITICAL:
        return 'border-red-600';
      case ALERT_SEVERITY.MAJOR:
        return 'border-orange-600';
      case ALERT_SEVERITY.MINOR:
        return 'border-yellow-600';
      default:
        return 'border-blue-600';
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold mb-2">Network Alerts</h1>
        <p className="text-gray-400">Monitor and manage network alerts with AI-powered grouping</p>
      </div>

      {/* Filters */}
      <Card className="bg-[#111419] border-gray-800">
        <CardContent className="p-4">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div className="relative">
              <Input
                placeholder="Search alerts..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="bg-gray-900 border-gray-700"
              />
            </div>
            
            <Select value={selectedSeverity} onValueChange={setSelectedSeverity}>
              <SelectTrigger className="bg-gray-900 border-gray-700">
                <SelectValue placeholder="Severity" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Severities</SelectItem>
                <SelectItem value={ALERT_SEVERITY.CRITICAL}>Critical</SelectItem>
                <SelectItem value={ALERT_SEVERITY.MAJOR}>Major</SelectItem>
                <SelectItem value={ALERT_SEVERITY.MINOR}>Minor</SelectItem>
              </SelectContent>
            </Select>

            <Select value={selectedDevice} onValueChange={setSelectedDevice}>
              <SelectTrigger className="bg-gray-900 border-gray-700">
                <SelectValue placeholder="Device" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Devices</SelectItem>
                {devices.map(device => (
                  <SelectItem key={device.id} value={device.id}>{device.name}</SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Button variant="outline" className="border-gray-700 hover:bg-gray-800">
              <Filter className="h-4 w-4 mr-2" />
              More Filters
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Alert Summary */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <Card className="bg-gradient-to-br from-red-950/30 to-orange-950/30 border-red-800/50">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-gray-400">Critical Alerts</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold text-red-400">
              {alerts.filter(a => a.severity === ALERT_SEVERITY.CRITICAL).length}
            </div>
          </CardContent>
        </Card>

        <Card className="bg-gradient-to-br from-orange-950/30 to-yellow-950/30 border-orange-800/50">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-gray-400">Major Alerts</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold text-orange-400">
              {alerts.filter(a => a.severity === ALERT_SEVERITY.MAJOR).length}
            </div>
          </CardContent>
        </Card>

        <Card className="bg-gradient-to-br from-yellow-950/30 to-amber-950/30 border-yellow-800/50">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-gray-400">Minor Alerts</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold text-yellow-400">
              {alerts.filter(a => a.severity === ALERT_SEVERITY.MINOR).length}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Grouped Alerts List */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2 space-y-3">
          {filteredGroups.map((group, idx) => {
            const mainAlert = group[0];
            const isExpanded = expandedGroups.has(mainAlert.id);
            const isGroup = group.length > 1;

            return (
              <Card
                key={mainAlert.id}
                className={`bg-[#111419] border-l-4 ${getSeverityBorder(mainAlert.severity)} border-gray-800 hover:bg-gray-900/50 transition-colors cursor-pointer`}
                onClick={() => setSelectedAlert(mainAlert)}
              >
                <CardContent className="p-4">
                  <div className="flex items-start gap-3">
                    {isGroup && (
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          toggleGroup(mainAlert.id);
                        }}
                        className="flex-shrink-0 mt-1 hover:bg-gray-800 p-1 rounded"
                      >
                        {isExpanded ? (
                          <ChevronDown className="h-4 w-4 text-gray-400" />
                        ) : (
                          <ChevronRight className="h-4 w-4 text-gray-400" />
                        )}
                      </button>
                    )}
                    
                    <AlertTriangle
                      className={`h-5 w-5 flex-shrink-0 mt-0.5 ${
                        mainAlert.severity === ALERT_SEVERITY.CRITICAL ? 'text-red-400' :
                        mainAlert.severity === ALERT_SEVERITY.MAJOR ? 'text-orange-400' : 'text-yellow-400'
                      }`}
                    />
                    
                    <div className="flex-1 min-w-0">
                      <div className="flex items-start justify-between gap-2 mb-1">
                        <div>
                          <h3 className="text-sm font-semibold text-gray-200">{mainAlert.title}</h3>
                          {isGroup && (
                            <Badge variant="outline" className="mt-1 text-xs border-gray-600 text-gray-400">
                              {group.length} related alerts
                            </Badge>
                          )}
                        </div>
                        <Badge className={`${getSeverityColor(mainAlert.severity)} text-white text-xs`}>
                          {mainAlert.severity}
                        </Badge>
                      </div>
                      
                      <p className="text-xs text-gray-400 mb-2">{mainAlert.description}</p>
                      
                      <div className="flex items-center gap-3 text-xs text-gray-500">
                        <span>{mainAlert.deviceName}</span>
                        <span>•</span>
                        <span className="flex items-center gap-1">
                          <Clock className="h-3 w-3" />
                          {new Date(mainAlert.timestamp).toLocaleString()}
                        </span>
                        <span>•</span>
                        <span>{mainAlert.category}</span>
                      </div>
                    </div>
                  </div>

                  {/* Expanded Related Alerts */}
                  {isGroup && isExpanded && (
                    <div className="mt-3 ml-11 space-y-2 pl-3 border-l-2 border-gray-800">
                      {group.slice(1).map(relatedAlert => (
                        <div key={relatedAlert.id} className="p-2 bg-gray-900/50 rounded">
                          <div className="text-xs font-medium text-gray-300">{relatedAlert.title}</div>
                          <div className="text-xs text-gray-500 mt-0.5">{relatedAlert.description}</div>
                        </div>
                      ))}
                    </div>
                  )}
                </CardContent>
              </Card>
            );
          })}
        </div>

        {/* Alert Details Drawer */}
        <Card className="bg-[#111419] border-gray-800 lg:sticky lg:top-6 h-fit">
          <CardHeader className="border-b border-gray-800">
            <div className="flex items-center justify-between">
              <CardTitle className="text-lg">Alert Details</CardTitle>
              {selectedAlert && (
                <Button
                  variant="ghost"
                  size="icon"
                  onClick={() => setSelectedAlert(null)}
                  className="hover:bg-gray-800"
                >
                  <X className="h-4 w-4" />
                </Button>
              )}
            </div>
          </CardHeader>
          <CardContent className="p-4">
            {selectedAlert ? (
              <div className="space-y-4">
                <div>
                  <Badge className={`${getSeverityColor(selectedAlert.severity)} text-white mb-3`}>
                    {selectedAlert.severity.toUpperCase()}
                  </Badge>
                  <h3 className="text-lg font-semibold text-gray-200 mb-2">{selectedAlert.title}</h3>
                  <p className="text-sm text-gray-400">{selectedAlert.description}</p>
                </div>

                <div className="pt-3 border-t border-gray-800">
                  <div className="text-xs text-gray-500 mb-1">Device</div>
                  <Button
                    variant="link"
                    className="p-0 h-auto text-teal-400 hover:text-teal-300"
                    onClick={() => navigate(`/device/${selectedAlert.deviceId}`)}
                  >
                    {selectedAlert.deviceName}
                  </Button>
                </div>

                <div>
                  <div className="text-xs text-gray-500 mb-1">Category</div>
                  <div className="text-sm text-gray-200">{selectedAlert.category}</div>
                </div>

                <div>
                  <div className="text-xs text-gray-500 mb-1">Timestamp</div>
                  <div className="text-sm text-gray-200">
                    {new Date(selectedAlert.timestamp).toLocaleString()}
                  </div>
                </div>

                <div>
                  <div className="text-xs text-gray-500 mb-2">AI Explanation</div>
                  <div className="p-3 bg-gradient-to-br from-teal-950/30 to-cyan-950/30 border border-teal-800/30 rounded-lg">
                    <p className="text-xs text-gray-300">
                      This alert indicates a performance issue that requires immediate attention. 
                      The AI system has analyzed historical patterns and recommends addressing this issue within the next hour.
                    </p>
                  </div>
                </div>

                {selectedAlert.relatedAlerts && selectedAlert.relatedAlerts.length > 0 && (
                  <div>
                    <div className="text-xs text-gray-500 mb-2">Related Alerts</div>
                    <div className="text-sm text-gray-400">{selectedAlert.relatedAlerts.length} related alerts detected</div>
                  </div>
                )}

                <div className="pt-3 border-t border-gray-800 space-y-2">
                  <Button className="w-full bg-teal-600 hover:bg-teal-500">
                    View RCA
                  </Button>
                  <Button variant="outline" className="w-full border-gray-700 hover:bg-gray-800">
                    Acknowledge Alert
                  </Button>
                  <Button variant="outline" className="w-full border-gray-700 hover:bg-gray-800">
                    Suppress Alerts
                  </Button>
                </div>
              </div>
            ) : (
              <div className="flex flex-col items-center justify-center py-12 text-center">
                <AlertTriangle className="h-12 w-12 text-gray-600 mb-3" />
                <p className="text-gray-400 text-sm">Select an alert to view details</p>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
};

export default Alerts;
