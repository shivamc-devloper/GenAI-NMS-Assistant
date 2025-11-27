import React, { useState } from 'react';
import { Zap, Play, Pause, CheckCircle, XCircle, Clock, TrendingUp, Settings } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import { Switch } from '../components/ui/switch';
import { Progress } from '../components/ui/progress';
import { automationRules, pendingAutomations } from '../data/mockData';

const Automation = () => {
  const [rules, setRules] = useState(automationRules);
  const [selectedAutomation, setSelectedAutomation] = useState(null);
  const [executingAutomation, setExecutingAutomation] = useState(null);

  const toggleRule = (ruleId) => {
    setRules(rules.map(rule => 
      rule.id === ruleId ? { ...rule, enabled: !rule.enabled } : rule
    ));
  };

  const executeAutomation = (automation) => {
    setExecutingAutomation(automation.id);
    setTimeout(() => {
      setExecutingAutomation(null);
      // Show success notification
    }, 3000);
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold mb-2">Automation & Remediation</h1>
        <p className="text-gray-400">AI-powered automated fixes and remediation rules</p>
      </div>

      {/* Pending Automations */}
      <Card className="bg-gradient-to-br from-teal-950/30 to-cyan-950/30 border-teal-800/50">
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle className="text-lg flex items-center gap-2">
              <Zap className="h-5 w-5 text-teal-400" />
              Ready for Automation
            </CardTitle>
            <Badge className="bg-teal-600 text-white">
              {pendingAutomations.length} issues
            </Badge>
          </div>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {pendingAutomations.map((automation) => (
              <div
                key={automation.id}
                className="p-4 bg-gray-900/50 rounded-lg border border-gray-800 hover:border-gray-700 transition-colors"
              >
                <div className="flex items-start justify-between gap-4">
                  <div className="flex-1">
                    <div className="flex items-center gap-2 mb-2">
                      <h3 className="text-sm font-semibold text-gray-200">{automation.deviceName}</h3>
                      <Badge
                        variant="outline"
                        className={`text-xs ${
                          automation.confidence >= 90 ? 'border-teal-600 text-teal-400' :
                          automation.confidence >= 80 ? 'border-yellow-600 text-yellow-400' :
                          'border-gray-600 text-gray-400'
                        }`}
                      >
                        {automation.confidence}% confidence
                      </Badge>
                    </div>
                    
                    <p className="text-sm text-gray-400 mb-2">
                      <span className="font-medium text-gray-300">Issue:</span> {automation.issue}
                    </p>
                    
                    <p className="text-sm text-gray-400 mb-3">
                      <span className="font-medium text-gray-300">Recommended Action:</span> {automation.recommendedAction}
                    </p>
                    
                    <div className="flex items-center gap-4 text-xs text-gray-500">
                      <span className="flex items-center gap-1">
                        <Clock className="h-3 w-3" />
                        Est. time: {automation.estimatedTime}
                      </span>
                      <span>•</span>
                      <span>Impact: {automation.estimatedImpact}</span>
                    </div>
                  </div>
                  
                  <div className="flex flex-col gap-2">
                    <Button
                      size="sm"
                      onClick={() => executeAutomation(automation)}
                      disabled={executingAutomation === automation.id}
                      className="bg-teal-600 hover:bg-teal-500"
                    >
                      {executingAutomation === automation.id ? (
                        <>
                          <Clock className="h-4 w-4 mr-1 animate-spin" />
                          Executing
                        </>
                      ) : (
                        <>
                          <Play className="h-4 w-4 mr-1" />
                          Execute
                        </>
                      )}
                    </Button>
                    <Button
                      size="sm"
                      variant="outline"
                      className="border-gray-700 hover:bg-gray-800"
                    >
                      Reject
                    </Button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Automation Rules */}
      <Card className="bg-[#111419] border-gray-800">
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle className="text-lg flex items-center gap-2">
              <Settings className="h-5 w-5 text-teal-400" />
              Automation Rules
            </CardTitle>
            <Button className="bg-teal-600 hover:bg-teal-500">
              Create New Rule
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {rules.map((rule) => (
              <div
                key={rule.id}
                className="p-4 bg-gray-900/50 rounded-lg border border-gray-800"
              >
                <div className="flex items-start justify-between gap-4">
                  <div className="flex-1">
                    <div className="flex items-center gap-3 mb-2">
                      <h3 className="text-sm font-semibold text-gray-200">{rule.name}</h3>
                      <Badge
                        className={`${
                          rule.enabled ? 'bg-teal-600' : 'bg-gray-600'
                        } text-white text-xs`}
                      >
                        {rule.enabled ? 'Active' : 'Inactive'}
                      </Badge>
                    </div>
                    
                    <p className="text-sm text-gray-400 mb-3">{rule.description}</p>
                    
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mb-3">
                      <div className="p-2 bg-gray-900 rounded border border-gray-800">
                        <div className="text-xs text-gray-500 mb-1">Trigger Condition</div>
                        <div className="text-xs text-gray-300 font-mono">{rule.trigger}</div>
                      </div>
                      <div className="p-2 bg-gray-900 rounded border border-gray-800">
                        <div className="text-xs text-gray-500 mb-1">Automated Action</div>
                        <div className="text-xs text-gray-300 font-mono">{rule.action}</div>
                      </div>
                    </div>
                    
                    <div className="flex items-center gap-6 text-xs text-gray-500">
                      <div className="flex items-center gap-2">
                        <TrendingUp className="h-3 w-3 text-teal-400" />
                        <span>Applied {rule.appliedCount} times</span>
                      </div>
                      <div>
                        <span className="text-teal-400 font-semibold">{rule.successRate}%</span> success rate
                      </div>
                    </div>
                  </div>
                  
                  <div className="flex items-center gap-2">
                    <Switch
                      checked={rule.enabled}
                      onCheckedChange={() => toggleRule(rule.id)}
                    />
                  </div>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Execution Logs */}
      <Card className="bg-[#111419] border-gray-800">
        <CardHeader>
          <CardTitle className="text-lg">Recent Automation Executions</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {[
              {
                id: 1,
                timestamp: new Date(Date.now() - 1000 * 60 * 15),
                device: 'Switch-2',
                action: 'Applied broadcast storm control',
                status: 'success',
                duration: '28s'
              },
              {
                id: 2,
                timestamp: new Date(Date.now() - 1000 * 60 * 45),
                device: 'Server-DB-01',
                action: 'Cleared cache and restarted services',
                status: 'success',
                duration: '1m 45s'
              },
              {
                id: 3,
                timestamp: new Date(Date.now() - 1000 * 60 * 120),
                device: 'Virtual-Gateway',
                action: 'Optimized routing table',
                status: 'success',
                duration: '52s'
              },
              {
                id: 4,
                timestamp: new Date(Date.now() - 1000 * 60 * 180),
                device: 'AP-Floor2',
                action: 'Restarted interface eth0',
                status: 'failed',
                duration: '12s'
              },
            ].map((log) => (
              <div
                key={log.id}
                className="p-3 bg-gray-900/50 rounded-lg border border-gray-800 flex items-center gap-3"
              >
                {log.status === 'success' ? (
                  <CheckCircle className="h-5 w-5 text-teal-400 flex-shrink-0" />
                ) : (
                  <XCircle className="h-5 w-5 text-red-400 flex-shrink-0" />
                )}
                
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-1">
                    <span className="text-sm font-medium text-gray-200">{log.device}</span>
                    <span className="text-xs text-gray-500">•</span>
                    <span className="text-xs text-gray-500">
                      {log.timestamp.toLocaleTimeString()}
                    </span>
                  </div>
                  <p className="text-sm text-gray-400">{log.action}</p>
                </div>
                
                <div className="flex items-center gap-3">
                  <span className="text-xs text-gray-500">{log.duration}</span>
                  <Badge
                    className={`${
                      log.status === 'success' ? 'bg-teal-600' : 'bg-red-600'
                    } text-white text-xs`}
                  >
                    {log.status}
                  </Badge>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

export default Automation;
