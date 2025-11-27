import React, { useState } from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import Layout from './components/Layout';
import Dashboard from './pages/Dashboard';
import Topology from './pages/Topology';
import AIAssistant from './pages/AIAssistant';
import DeviceDetail from './pages/DeviceDetail';
import Alerts from './pages/Alerts';
import RCA from './pages/RCA';
import Automation from './pages/Automation';
import Reports from './pages/Reports';
import { Toaster } from './components/ui/sonner';
import './App.css';

function App() {
  const [showAIAssistant, setShowAIAssistant] = useState(false);

  const handleAIAssistantClick = () => {
    setShowAIAssistant(!showAIAssistant);
  };

  return (
    <div className="App">
      <BrowserRouter>
        {showAIAssistant ? (
          <Layout onAIAssistantClick={handleAIAssistantClick}>
            <AIAssistant />
          </Layout>
        ) : (
          <Layout onAIAssistantClick={handleAIAssistantClick}>
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/topology" element={<Topology />} />
              <Route path="/device/:deviceId" element={<DeviceDetail />} />
              <Route path="/alerts" element={<Alerts />} />
              <Route path="/rca" element={<RCA />} />
              <Route path="/automation" element={<Automation />} />
              <Route path="/reports" element={<Reports />} />
            </Routes>
          </Layout>
        )}
        <Toaster />
      </BrowserRouter>
    </div>
  );
}

export default App;
