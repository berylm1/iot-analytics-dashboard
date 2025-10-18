import React, { useState, useEffect } from 'react';
import './App.css';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer
} from 'recharts';

const API_BASE_URL = 'http://localhost:8080/api';

function App() {
  const [stats, setStats] = useState(null);
  const [aggregations, setAggregations] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Fetch data from API
  const fetchData = async () => {
    try {
      // Fetch statistics
      const statsResponse = await fetch(`${API_BASE_URL}/stats`);
      const statsData = await statsResponse.json();
      setStats(statsData);

      // Fetch recent aggregations
      const aggResponse = await fetch(`${API_BASE_URL}/aggregations?limit=20`);
      const aggData = await aggResponse.json();
      
      // Filter out empty data and format for charts
      const validData = aggData
        .filter(item => item.deviceCount > 0)
        .reverse() // Oldest first for chart
        .map(item => ({
          time: new Date(item.windowStart).toLocaleTimeString(),
          temperature: parseFloat(item.avgTemperature.toFixed(2)),
          humidity: parseFloat(item.avgHumidity.toFixed(2)),
          devices: item.deviceCount
        }));
      
      setAggregations(validData);
      setLoading(false);
    } catch (err) {
      setError(err.message);
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    
    // Refresh data every 30 seconds
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, []);

  if (loading) {
    return <div className="loading">Loading dashboard...</div>;
  }

  if (error) {
    return <div className="error">Error: {error}</div>;
  }

  return (
    <div className="App">
      <header className="App-header">
        <h1>🌡️ IoT Analytics Dashboard</h1>
        <p>Real-time monitoring of IoT devices</p>
      </header>

      {/* Statistics Cards */}
      <div className="stats-container">
        <div className="stat-card">
          <h3>Average Temperature</h3>
          <p className="stat-value">{stats?.avgTemperature?.toFixed(2)}°C</p>
        </div>
        <div className="stat-card">
          <h3>Average Humidity</h3>
          <p className="stat-value">{stats?.avgHumidity?.toFixed(2)}%</p>
        </div>
        <div className="stat-card">
          <h3>Active Devices</h3>
          <p className="stat-value">{stats?.averageDevices?.toFixed(0)}</p>
        </div>
        <div className="stat-card">
          <h3>Data Windows</h3>
          <p className="stat-value">{stats?.windowCount}</p>
        </div>
      </div>

      {/* Temperature Chart */}
      <div className="chart-container">
        <h2>Temperature Over Time</h2>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={aggregations}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="time" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Line 
              type="monotone" 
              dataKey="temperature" 
              stroke="#ff6b6b" 
              strokeWidth={2}
              name="Temperature (°C)"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Humidity Chart */}
      <div className="chart-container">
        <h2>Humidity Over Time</h2>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={aggregations}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="time" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Line 
              type="monotone" 
              dataKey="humidity" 
              stroke="#4ecdc4" 
              strokeWidth={2}
              name="Humidity (%)"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <footer className="App-footer">
        <p>Last updated: {new Date().toLocaleTimeString()}</p>
      </footer>
    </div>
  );
}

export default App;
