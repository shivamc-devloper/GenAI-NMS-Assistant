import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Search, AlertCircle, ArrowRight, Target, CheckCircle2, TrendingUp } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import { rcaExamples, devices } from '../data/mockData';

const RCA = () => {
  const navigate = useNavigate();
  const [selectedRCA, setSelectedRCA] = useState(rcaExamples[0]);

  const getImpactColor = (level) => {
    switch (level) {
      case 'high':
        return 'text-red-400 bg-red-950/30 border-red-800/30';
      case 'medium':
        return 'text-orange-400 bg-orange-950/30 border-orange-800/30';
      case 'low':
        return 'text-yellow-400 bg-yellow-950/30 border-yellow-800/30';
      default:
        return 'text-gray-400 bg-gray-950/30 border-gray-800/30';
    }
  };

  const getSeverityColor = (severity) => {
    switch (severity) {
      case 'critical':
        return 'bg-red-600';
      case 'major':
        return 'bg-orange-600';
      case 'root':
        return 'bg-purple-600';
      default:
        return 'bg-gray-600';
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold mb-2">Root Cause Analysis</h1>
        <p className="text-gray-400">AI-powered investigation of network issues and their root causes</p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* RCA List */}
        <Card className="bg-[#111419] border-gray-800">
          <CardHeader className="border-b border-gray-800">
            <CardTitle className="text-lg">Recent Analyses</CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            <div className="divide-y divide-gray-800">
              {rcaExamples.map((rca) => (
                <button
                  key={rca.id}
                  onClick={() => setSelectedRCA(rca)}
                  className={`w-full p-4 text-left hover:bg-gray-900/50 transition-colors ${
                    selectedRCA?.id === rca.id ? 'bg-gray-900/50 border-l-4 border-teal-600' : ''
                  }`}
                >
                  <div className="flex items-start gap-3">
                    <Search className="h-5 w-5 text-teal-400 flex-shrink-0 mt-0.5" />
                    <div className="flex-1 min-w-0">
                      <div className="text-sm font-semibold text-gray-200 mb-1">{rca.title}</div>
                      <div className="flex items-center gap-2 text-xs text-gray-500">
                        <span>{rca.deviceName}</span>
                        <span>•</span>
                        <span>{new Date(rca.timestamp).toLocaleDateString()}</span>
                      </div>
                      <div className="mt-2">
                        <Badge className="bg-teal-600 text-white text-xs">
                          {rca.confidence}% confidence
                        </Badge>
                      </div>
                    </div>
                  </div>
                </button>
              ))}
            </div>
          </CardContent>
        </Card>

        {/* RCA Details */}
        <div className="lg:col-span-2 space-y-6">
          {selectedRCA && (
            <>
              {/* Summary */}
              <Card className="bg-gradient-to-br from-purple-950/30 to-pink-950/30 border-purple-800/50">
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <CardTitle className="text-xl mb-2">{selectedRCA.title}</CardTitle>
                      <p className="text-gray-400 text-sm">
                        Analysis completed at {new Date(selectedRCA.timestamp).toLocaleString()}
                      </p>
                    </div>
                    <Badge className="bg-purple-600 text-white">
                      {selectedRCA.confidence}% Confidence
                    </Badge>
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="p-4 bg-gray-900/50 rounded-lg border border-purple-800/30">
                    <div className="flex items-center gap-2 mb-2">
                      <Target className="h-5 w-5 text-purple-400" />
                      <div className="text-sm font-semibold text-gray-200">Root Cause Identified</div>
                    </div>
                    <p className="text-gray-300">{selectedRCA.rootCause}</p>
                  </div>
                </CardContent>
              </Card>

              {/* Cause Chain Diagram */}
              <Card className="bg-[#111419] border-gray-800">
                <CardHeader>
                  <CardTitle className="text-lg">Cause Chain Analysis</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    {selectedRCA.causeChain.map((step, idx) => (
                      <div key={step.step} className="flex items-start gap-4">
                        <div className="flex flex-col items-center">
                          <div className={`w-10 h-10 rounded-full ${getSeverityColor(step.severity)} flex items-center justify-center font-bold text-white`}>
                            {step.step}
                          </div>
                          {idx < selectedRCA.causeChain.length - 1 && (
                            <div className="w-0.5 h-12 bg-gray-700 my-1"></div>
                          )}
                        </div>
                        <div className="flex-1 pt-2">
                          <div className="flex items-center gap-2 mb-1">
                            <p className="text-sm font-medium text-gray-200">{step.description}</p>
                            {step.severity === 'root' && (
                              <Badge className="bg-purple-600 text-white text-xs">Root Cause</Badge>
                            )}
                          </div>
                          <Badge
                            variant="outline"
                            className={`text-xs ${
                              step.severity === 'critical' ? 'border-red-600 text-red-400' :
                              step.severity === 'major' ? 'border-orange-600 text-orange-400' :
                              'border-purple-600 text-purple-400'
                            }`}
                          >
                            {step.severity}
                          </Badge>
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>

              {/* Key Evidence */}
              <Card className="bg-[#111419] border-gray-800">
                <CardHeader>
                  <CardTitle className="text-lg flex items-center gap-2">
                    <AlertCircle className="h-5 w-5 text-teal-400" />
                    Key Evidence
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                    {selectedRCA.evidence.map((evidence, idx) => (
                      <div key={idx} className="p-3 bg-gray-900/50 rounded-lg border border-gray-800">
                        <div className="flex items-start gap-2">
                          <CheckCircle2 className="h-4 w-4 text-teal-400 flex-shrink-0 mt-0.5" />
                          <span className="text-sm text-gray-300">{evidence}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>

              {/* Impact Analysis */}
              <Card className="bg-[#111419] border-gray-800">
                <CardHeader>
                  <CardTitle className="text-lg">Impact Analysis</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div className={`p-4 rounded-lg border ${getImpactColor(selectedRCA.impactLevel)}`}>
                      <div className="flex items-center justify-between mb-2">
                        <div className="text-sm font-semibold text-gray-200">Business Impact</div>
                        <Badge className={getSeverityColor(selectedRCA.impactLevel === 'high' ? 'critical' : 'major')}>
                          {selectedRCA.impactLevel.toUpperCase()}
                        </Badge>
                      </div>
                      <p className="text-sm text-gray-400">
                        {selectedRCA.affectedDevices.length} devices affected by this issue
                      </p>
                    </div>

                    <div>
                      <div className="text-sm font-semibold text-gray-200 mb-3">Affected Devices</div>
                      <div className="space-y-2">
                        {selectedRCA.affectedDevices.map((deviceId) => {
                          const device = devices.find(d => d.id === deviceId);
                          return device ? (
                            <Button
                              key={deviceId}
                              variant="outline"
                              className="w-full justify-between border-gray-700 hover:bg-gray-800"
                              onClick={() => navigate(`/device/${deviceId}`)}
                            >
                              <span className="text-sm">{device.name}</span>
                              <ArrowRight className="h-4 w-4" />
                            </Button>
                          ) : null;
                        })}
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>

              {/* Suggested Actions */}
              <Card className="bg-gradient-to-br from-teal-950/30 to-cyan-950/30 border-teal-800/50">
                <CardHeader>
                  <CardTitle className="text-lg flex items-center gap-2">
                    <TrendingUp className="h-5 w-5 text-teal-400" />
                    Suggested Remediation Actions
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-3 mb-4">
                    {selectedRCA.suggestedFixes.map((fix, idx) => (
                      <div key={idx} className="p-4 bg-gray-900/50 rounded-lg border border-gray-800">
                        <div className="flex items-start gap-3">
                          <div className="flex-shrink-0 w-6 h-6 rounded-full bg-teal-600 flex items-center justify-center">
                            <span className="text-xs font-bold text-white">{idx + 1}</span>
                          </div>
                          <div className="flex-1">
                            <p className="text-sm text-gray-300">{fix}</p>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>

                  <div className="flex gap-3">
                    <Button className="flex-1 bg-teal-600 hover:bg-teal-500">
                      Apply Automated Fixes
                    </Button>
                    <Button
                      variant="outline"
                      onClick={() => navigate('/automation')}
                      className="flex-1 border-teal-600 text-teal-400 hover:bg-teal-950/30"
                    >
                      View in Automation
                    </Button>
                  </div>
                </CardContent>
              </Card>
            </>
          )}
        </div>
      </div>
    </div>
  );
};

export default RCA;
