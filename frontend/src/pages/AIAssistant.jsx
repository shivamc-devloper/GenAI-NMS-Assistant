import React, { useState, useRef, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Badge } from '../components/ui/badge';
import { Send, Bot, User, TrendingUp, AlertTriangle, CheckCircle } from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { aiChatHistory, devices, generateMetricsHistory } from '../data/mockData';

const AIAssistant = ({ initialQuestion = null }) => {
  const [messages, setMessages] = useState(aiChatHistory);
  const [input, setInput] = useState('');
  const [isTyping, setIsTyping] = useState(false);
  const messagesEndRef = useRef(null);
  const [contextDevice, setContextDevice] = useState(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  useEffect(() => {
    if (initialQuestion) {
      setInput(initialQuestion);
    }
  }, [initialQuestion]);

  const handleSend = async () => {
    if (!input.trim()) return;

    const userMessage = {
      id: `chat-${Date.now()}`,
      role: 'user',
      message: input,
      timestamp: new Date().toISOString(),
    };

    setMessages((prev) => [...prev, userMessage]);
    setInput('');
    setIsTyping(true);

    // Simulate AI response
    setTimeout(() => {
      let aiResponse = {
        id: `chat-${Date.now() + 1}`,
        role: 'assistant',
        timestamp: new Date().toISOString(),
      };

      // Simple pattern matching for demo responses
      const lowerInput = input.toLowerCase();
      
      if (lowerInput.includes('switch-2') || lowerInput.includes('high cpu')) {
        aiResponse.message = 'Based on my analysis, Switch-2 is experiencing high CPU usage (89%) due to a broadcast storm on VLAN 30. A misconfigured device is sending continuous broadcast packets at approximately 15,000 packets per second, overwhelming the switch processor. This is also causing elevated latency (45ms) and packet loss (2.5%) on the network.';
        aiResponse.context = {
          deviceId: 'device-2',
          metrics: { cpu: 89, traffic: 920, latency: 45 },
          suggestion: 'Apply broadcast storm control and identify the misconfigured device',
        };
        setContextDevice(devices.find(d => d.id === 'device-2'));
      } else if (lowerInput.includes('latency') || lowerInput.includes('slow')) {
        aiResponse.message = 'Network latency issues detected on multiple devices. Primary cause: Switch-2 broadcast storm affecting downstream devices. Average network latency is currently at 85ms, which is above the normal threshold. I recommend addressing the Switch-2 issue first, as it will likely resolve latency problems on connected devices.';
        aiResponse.context = {
          suggestion: 'Address Switch-2 broadcast storm to improve overall network latency',
        };
      } else if (lowerInput.includes('server') || lowerInput.includes('memory')) {
        aiResponse.message = 'Server-DB-01 is showing elevated memory usage at 85%, approaching the critical threshold of 90%. The high memory consumption is likely due to database query caching and background processes. CPU usage is also at 72%. I recommend clearing non-essential caches and potentially restarting background services during a maintenance window.';
        aiResponse.context = {
          deviceId: 'device-3',
          metrics: { cpu: 72, memory: 85 },
          suggestion: 'Clear cache and restart non-critical services',
        };
        setContextDevice(devices.find(d => d.id === 'device-3'));
      } else if (lowerInput.includes('offline') || lowerInput.includes('router-edge')) {
        aiResponse.message = 'Router-Edge has been offline for approximately 3 hours. The device is not responding to ICMP ping requests or SNMP queries. Based on historical patterns and last known state, this appears to be a hardware interface failure. The device will require physical inspection and likely a hardware module replacement.';
        aiResponse.context = {
          deviceId: 'device-8',
          suggestion: 'Schedule maintenance for hardware inspection and module replacement',
        };
      } else if (lowerInput.includes('fix') || lowerInput.includes('solve')) {
        aiResponse.message = 'I can help you apply automated fixes for the current issues. For Switch-2, I can apply broadcast storm control and implement rate limiting on VLAN 30. For Server-DB-01, I can clear caches and restart non-critical services. Would you like me to proceed with these automated remediation actions?';
        aiResponse.context = {
          suggestion: 'Apply automated fixes',
        };
      } else {
        aiResponse.message = 'I\'m analyzing your network for issues. Currently monitoring 8 devices with 5 critical alerts detected. The most pressing issue is Switch-2 experiencing high CPU usage due to a broadcast storm. Would you like me to explain any specific device or issue in detail?';
      }

      setMessages((prev) => [...prev, aiResponse]);
      setIsTyping(false);
    }, 1500);
  };

  const handleApplyFix = (suggestion) => {
    const fixMessage = {
      id: `chat-${Date.now()}`,
      role: 'user',
      message: `Apply fix: ${suggestion}`,
      timestamp: new Date().toISOString(),
    };

    setMessages((prev) => [...prev, fixMessage]);
    setIsTyping(true);

    setTimeout(() => {
      const confirmMessage = {
        id: `chat-${Date.now() + 1}`,
        role: 'assistant',
        message: 'Fix has been applied successfully. Monitoring the device for improvement. The configuration change will take effect within 30 seconds. I\'ll continue to monitor and notify you of any changes.',
        timestamp: new Date().toISOString(),
      };
      setMessages((prev) => [...prev, confirmMessage]);
      setIsTyping(false);
    }, 2000);
  };

  return (
    <div className="h-[calc(100vh-200px)] grid grid-cols-1 lg:grid-cols-3 gap-4">
      {/* Chat Window */}
      <Card className="lg:col-span-2 bg-[#111419] border-gray-800 flex flex-col">
        <CardHeader className="border-b border-gray-800">
          <CardTitle className="flex items-center gap-2">
            <Bot className="h-5 w-5 text-teal-400" />
            AI Assistant Console
          </CardTitle>
        </CardHeader>
        <CardContent className="flex-1 flex flex-col p-4">
          {/* Messages */}
          <div className="flex-1 overflow-y-auto space-y-4 mb-4 pr-2">
            {messages.map((msg) => (
              <div
                key={msg.id}
                className={`flex gap-3 ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}
              >
                {msg.role === 'assistant' && (
                  <div className="flex-shrink-0 w-8 h-8 rounded-full bg-gradient-to-r from-teal-600 to-cyan-600 flex items-center justify-center">
                    <Bot className="h-5 w-5 text-white" />
                  </div>
                )}
                <div
                  className={`max-w-[80%] rounded-lg p-3 ${
                    msg.role === 'user'
                      ? 'bg-teal-600 text-white'
                      : 'bg-gray-900 text-gray-200 border border-gray-800'
                  }`}
                >
                  <p className="text-sm">{msg.message}</p>
                  {msg.context?.suggestion && (
                    <div className="mt-3 pt-3 border-t border-gray-700">
                      <div className="text-xs text-gray-400 mb-2">Suggested Action:</div>
                      <Button
                        size="sm"
                        onClick={() => handleApplyFix(msg.context.suggestion)}
                        className="bg-teal-600 hover:bg-teal-500 text-xs"
                      >
                        <CheckCircle className="h-3 w-3 mr-1" />
                        Apply Fix
                      </Button>
                    </div>
                  )}
                </div>
                {msg.role === 'user' && (
                  <div className="flex-shrink-0 w-8 h-8 rounded-full bg-gray-700 flex items-center justify-center">
                    <User className="h-5 w-5 text-gray-300" />
                  </div>
                )}
              </div>
            ))}
            {isTyping && (
              <div className="flex gap-3 justify-start">
                <div className="flex-shrink-0 w-8 h-8 rounded-full bg-gradient-to-r from-teal-600 to-cyan-600 flex items-center justify-center">
                  <Bot className="h-5 w-5 text-white" />
                </div>
                <div className="bg-gray-900 text-gray-200 border border-gray-800 rounded-lg p-3">
                  <div className="flex gap-1">
                    <div className="w-2 h-2 bg-teal-400 rounded-full animate-bounce"></div>
                    <div className="w-2 h-2 bg-teal-400 rounded-full animate-bounce" style={{ animationDelay: '0.1s' }}></div>
                    <div className="w-2 h-2 bg-teal-400 rounded-full animate-bounce" style={{ animationDelay: '0.2s' }}></div>
                  </div>
                </div>
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>

          {/* Input */}
          <div className="flex gap-2">
            <Input
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyPress={(e) => e.key === 'Enter' && handleSend()}
              placeholder="Ask about network issues..."
              className="flex-1 bg-gray-900 border-gray-700"
            />
            <Button
              onClick={handleSend}
              disabled={!input.trim() || isTyping}
              className="bg-teal-600 hover:bg-teal-500"
            >
              <Send className="h-4 w-4" />
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Context Panel */}
      <Card className="bg-[#111419] border-gray-800 overflow-auto">
        <CardHeader className="border-b border-gray-800">
          <CardTitle className="text-lg">Context & Insights</CardTitle>
        </CardHeader>
        <CardContent className="p-4 space-y-4">
          {contextDevice ? (
            <>
              <div>
                <div className="text-sm font-semibold text-gray-200 mb-2">{contextDevice.name}</div>
                <Badge
                  className={`${
                    contextDevice.status === 'critical'
                      ? 'bg-red-600'
                      : contextDevice.status === 'warning'
                      ? 'bg-yellow-600'
                      : 'bg-teal-600'
                  } text-white`}
                >
                  {contextDevice.status}
                </Badge>
              </div>

              <div className="space-y-3">
                <div className="p-3 bg-gray-900/50 rounded-lg">
                  <div className="text-xs text-gray-500 mb-1">CPU Usage</div>
                  <div className="text-2xl font-bold text-teal-400">{contextDevice.cpu}%</div>
                </div>
                <div className="p-3 bg-gray-900/50 rounded-lg">
                  <div className="text-xs text-gray-500 mb-1">Memory Usage</div>
                  <div className="text-2xl font-bold text-cyan-400">{contextDevice.memory}%</div>
                </div>
                <div className="p-3 bg-gray-900/50 rounded-lg">
                  <div className="text-xs text-gray-500 mb-1">Latency</div>
                  <div className="text-2xl font-bold text-purple-400">{contextDevice.latency}ms</div>
                </div>
              </div>

              <div>
                <div className="text-xs text-gray-500 mb-2">CPU Trend (Last Hour)</div>
                <ResponsiveContainer width="100%" height={120}>
                  <LineChart data={generateMetricsHistory(1).slice(-20)}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                    <XAxis dataKey="time" stroke="#9ca3af" style={{ fontSize: '10px' }} hide />
                    <YAxis stroke="#9ca3af" style={{ fontSize: '10px' }} />
                    <Tooltip
                      contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
                      labelStyle={{ color: '#e5e7eb' }}
                    />
                    <Line type="monotone" dataKey="cpu" stroke="#14b8a6" strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </>
          ) : (
            <div className="space-y-4">
              <div className="p-4 bg-gradient-to-br from-red-950/30 to-orange-950/30 border border-red-800/30 rounded-lg">
                <div className="flex items-center gap-2 mb-2">
                  <AlertTriangle className="h-5 w-5 text-red-400" />
                  <div className="text-sm font-semibold text-gray-200">Active Critical Alerts</div>
                </div>
                <div className="text-2xl font-bold text-red-400">5</div>
              </div>

              <div className="p-4 bg-gray-900/50 rounded-lg border border-gray-800">
                <div className="text-xs text-gray-500 mb-2">Quick Actions</div>
                <div className="space-y-2">
                  <Button
                    variant="outline"
                    size="sm"
                    className="w-full justify-start border-gray-700 hover:bg-gray-800"
                    onClick={() => setInput('Why is Switch-2 having high CPU usage?')}
                  >
                    <TrendingUp className="h-4 w-4 mr-2 text-teal-400" />
                    Analyze Switch-2 Issue
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    className="w-full justify-start border-gray-700 hover:bg-gray-800"
                    onClick={() => setInput('What is causing network latency?')}
                  >
                    <AlertTriangle className="h-4 w-4 mr-2 text-yellow-400" />
                    Check Network Latency
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    className="w-full justify-start border-gray-700 hover:bg-gray-800"
                    onClick={() => setInput('How can I fix these issues?')}
                  >
                    <CheckCircle className="h-4 w-4 mr-2 text-teal-400" />
                    Get Remediation Steps
                  </Button>
                </div>
              </div>

              <div className="p-4 bg-gray-900/50 rounded-lg border border-gray-800">
                <div className="text-xs text-gray-500 mb-2">AI Capabilities</div>
                <ul className="space-y-2 text-xs text-gray-400">
                  <li className="flex items-start gap-2">
                    <CheckCircle className="h-3 w-3 text-teal-400 mt-0.5 flex-shrink-0" />
                    <span>Root cause analysis</span>
                  </li>
                  <li className="flex items-start gap-2">
                    <CheckCircle className="h-3 w-3 text-teal-400 mt-0.5 flex-shrink-0" />
                    <span>Performance diagnostics</span>
                  </li>
                  <li className="flex items-start gap-2">
                    <CheckCircle className="h-3 w-3 text-teal-400 mt-0.5 flex-shrink-0" />
                    <span>Automated remediation</span>
                  </li>
                  <li className="flex items-start gap-2">
                    <CheckCircle className="h-3 w-3 text-teal-400 mt-0.5 flex-shrink-0" />
                    <span>Log analysis</span>
                  </li>
                </ul>
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
};

export default AIAssistant;
