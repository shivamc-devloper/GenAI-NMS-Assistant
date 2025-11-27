import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import { 
  LayoutDashboard, 
  Network, 
  MessageSquare, 
  AlertTriangle, 
  Search, 
  Zap, 
  BarChart3,
  Menu,
  Bell
} from 'lucide-react';
import { Button } from './ui/button';

const Layout = ({ children, onAIAssistantClick }) => {
  const location = useLocation();
  const [sidebarOpen, setSidebarOpen] = React.useState(true);

  const navigation = [
    { name: 'Dashboard', path: '/', icon: LayoutDashboard },
    { name: 'Topology', path: '/topology', icon: Network },
    { name: 'Alerts', path: '/alerts', icon: AlertTriangle },
    { name: 'RCA', path: '/rca', icon: Search },
    { name: 'Automation', path: '/automation', icon: Zap },
    { name: 'Reports', path: '/reports', icon: BarChart3 },
  ];

  return (
    <div className="min-h-screen bg-[#0a0d12] text-gray-100">
      {/* Top Navigation Bar */}
      <div className="fixed top-0 left-0 right-0 h-16 bg-[#111419] border-b border-gray-800 z-50 flex items-center px-4">
        <Button
          variant="ghost"
          size="icon"
          onClick={() => setSidebarOpen(!sidebarOpen)}
          className="mr-4 hover:bg-gray-800"
        >
          <Menu className="h-5 w-5" />
        </Button>
        
        <h1 className="text-xl font-bold bg-gradient-to-r from-teal-400 to-cyan-400 bg-clip-text text-transparent">
          Network Monitoring AI
        </h1>

        <div className="ml-auto flex items-center gap-3">
          <Button variant="ghost" size="icon" className="relative hover:bg-gray-800">
            <Bell className="h-5 w-5" />
            <span className="absolute top-1 right-1 h-2 w-2 bg-red-500 rounded-full"></span>
          </Button>
          
          <Button 
            onClick={onAIAssistantClick}
            className="bg-gradient-to-r from-teal-600 to-cyan-600 hover:from-teal-500 hover:to-cyan-500"
          >
            <MessageSquare className="h-4 w-4 mr-2" />
            AI Assistant
          </Button>
        </div>
      </div>

      {/* Sidebar */}
      <div
        className={`fixed left-0 top-16 bottom-0 bg-[#111419] border-r border-gray-800 transition-all duration-300 z-40 ${
          sidebarOpen ? 'w-64' : 'w-16'
        }`}
      >
        <nav className="p-3 space-y-1">
          {navigation.map((item) => {
            const isActive = location.pathname === item.path;
            const Icon = item.icon;
            
            return (
              <Link
                key={item.path}
                to={item.path}
                className={`flex items-center gap-3 px-3 py-2.5 rounded-lg transition-all ${
                  isActive
                    ? 'bg-gradient-to-r from-teal-600/20 to-cyan-600/20 text-teal-400 border border-teal-600/30'
                    : 'text-gray-400 hover:bg-gray-800 hover:text-gray-200'
                }`}
              >
                <Icon className="h-5 w-5 flex-shrink-0" />
                {sidebarOpen && <span className="font-medium">{item.name}</span>}
              </Link>
            );
          })}
        </nav>
      </div>

      {/* Main Content */}
      <div
        className={`pt-16 transition-all duration-300 ${
          sidebarOpen ? 'ml-64' : 'ml-16'
        }`}
      >
        <div className="p-6">
          {children}
        </div>
      </div>

      {/* Floating AI Assistant Button */}
      <Button
        onClick={onAIAssistantClick}
        className="fixed bottom-6 right-6 h-14 w-14 rounded-full shadow-lg bg-gradient-to-r from-teal-600 to-cyan-600 hover:from-teal-500 hover:to-cyan-500 hover:scale-110 transition-transform z-50"
        size="icon"
      >
        <MessageSquare className="h-6 w-6" />
      </Button>
    </div>
  );
};

export default Layout;
