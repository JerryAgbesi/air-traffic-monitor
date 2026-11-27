/**
 * Dashboard JavaScript for French Airspace Traffic Monitor
 * Handles real-time data fetching and Plotly chart rendering
 */

// Configuration
const REFRESH_INTERVAL = 30000; // 30 seconds for KPIs
const MAP_REFRESH_INTERVAL = 60000; // 1 minute for map

let trafficChart = null;
let altitudeChart = null;
let forecastChart = null;
let liveMap = null;
let refreshTimers = {};

// ---- Utility Functions ----

function formatDateTime(isoString) {
    if (!isoString || isoString === "Unknown") return "--";
    try {
        const date = new Date(isoString);
        return date.toLocaleString(undefined, {
            hour: '2-digit', minute: '2-digit', second: '2-digit',
            day: '2-digit', month: 'short', timeZoneName: 'short'
        });
    } catch {
        return isoString;
    }
}

function showError(containerId, message) {
    const container = document.getElementById(containerId);
    if (container) {
        container.innerHTML = `<div class="alert alert-warning">${message}</div>`;
    }
}

function setLoading(containerId, loading) {
    const container = document.getElementById(containerId);
    if (container) {
        if (loading) {
            container.classList.add('loading');
        } else {
            container.classList.remove('loading');
        }
    }
}

// ---- KPI Updates ----

async function loadKPIs() {
    try {
        const response = await fetch('/api/kpis');
        const data = await response.json();

        document.getElementById('kpi-airborne').textContent = data.airborne.toLocaleString();
        document.getElementById('kpi-total').textContent = data.total.toLocaleString();
        document.getElementById('kpi-climbing').textContent = data.climbing.toLocaleString();
        document.getElementById('kpi-descending').textContent = data.descending.toLocaleString();
        document.getElementById('kpi-altitude').textContent = data.mean_altitude.toLocaleString() + ' m';
        document.getElementById('kpi-timestamp').textContent = formatDateTime(data.timestamp);
        document.getElementById('last-update').textContent = `Updated: ${new Date().toLocaleTimeString()}`;

    } catch (error) {
        console.error('Failed to load KPIs:', error);
    }
}

// ---- Traffic Chart ----

async function loadTrafficChart() {
    const hours = document.getElementById('traffic-hours').value;
    setLoading('traffic-chart', true);

    try {
        const response = await fetch(`/api/traffic-chart?hours=${hours}`);
        const chartData = await response.text();

        if (chartData && chartData !== '{}') {
            const figure = JSON.parse(chartData);
            if (trafficChart) {
                Plotly.react('traffic-chart', figure.data, figure.layout, {responsive: true});
            } else {
                Plotly.newPlot('traffic-chart', figure.data, figure.layout, {responsive: true});
                trafficChart = true;
            }
        } else {
            showError('traffic-chart', 'No traffic data available for selected period');
        }
    } catch (error) {
        console.error('Failed to load traffic chart:', error);
        showError('traffic-chart', 'Failed to load chart');
    } finally {
        setLoading('traffic-chart', false);
    }
}

// ---- Altitude Chart ----

async function loadAltitudeChart() {
    const hours = document.getElementById('traffic-hours').value;
    setLoading('altitude-chart', true);

    try {
        const response = await fetch(`/api/altitude-chart?hours=${hours}`);
        const chartData = await response.text();

        if (chartData && chartData !== '{}') {
            const figure = JSON.parse(chartData);
            if (altitudeChart) {
                Plotly.react('altitude-chart', figure.data, figure.layout, {responsive: true});
            } else {
                Plotly.newPlot('altitude-chart', figure.data, figure.layout, {responsive: true});
                altitudeChart = true;
            }
        } else {
            showError('altitude-chart', 'No altitude data available');
        }
    } catch (error) {
        console.error('Failed to load altitude chart:', error);
        showError('altitude-chart', 'Failed to load chart');
    } finally {
        setLoading('altitude-chart', false);
    }
}

// ---- Forecast Chart ----

async function loadForecastChart() {
    const hours = document.getElementById('forecast-hours').value;
    setLoading('forecast-chart', true);

    try {
        const response = await fetch(`/api/forecast-chart?hours=${hours}`);
        const chartData = await response.text();

        if (chartData && chartData !== '{}') {
            const figure = JSON.parse(chartData);
            if (forecastChart) {
                Plotly.react('forecast-chart', figure.data, figure.layout, {responsive: true});
            } else {
                Plotly.newPlot('forecast-chart', figure.data, figure.layout, {responsive: true});
                forecastChart = true;
            }
        } else {
            showError('forecast-chart', 'No forecast data available yet');
        }
    } catch (error) {
        console.error('Failed to load forecast chart:', error);
        showError('forecast-chart', 'Failed to load chart');
    } finally {
        setLoading('forecast-chart', false);
    }
}

// ---- Live Map ----

async function loadLiveMap() {
    const limit = document.getElementById('map-limit').value;
    setLoading('live-map', true);

    try {
        const response = await fetch(`/api/live-map?limit=${limit}`);
        const chartData = await response.text();

        if (chartData && chartData !== '{}') {
            const figure = JSON.parse(chartData);
            if (liveMap) {
                Plotly.react('live-map', figure.data, figure.layout, {responsive: true});
            } else {
                Plotly.newPlot('live-map', figure.data, figure.layout, {responsive: true});
                liveMap = true;
            }
        } else {
            showError('live-map', 'No aircraft position data available');
        }
    } catch (error) {
        console.error('Failed to load live map:', error);
        showError('live-map', 'Failed to load map');
    } finally {
        setLoading('live-map', false);
    }
}

// ---- Auto-refresh Management ----

function startAutoRefresh() {
    // KPIs and charts
    refreshTimers.kpis = setInterval(loadKPIs, REFRESH_INTERVAL);
    refreshTimers.traffic = setInterval(loadTrafficChart, REFRESH_INTERVAL);
    refreshTimers.altitude = setInterval(loadAltitudeChart, REFRESH_INTERVAL);
    refreshTimers.forecast = setInterval(loadForecastChart, REFRESH_INTERVAL);

    // Map (less frequent)
    refreshTimers.map = setInterval(loadLiveMap, MAP_REFRESH_INTERVAL);
}

function stopAutoRefresh() {
    Object.values(refreshTimers).forEach(timer => clearInterval(timer));
    refreshTimers = {};
}

// Handle page visibility change to pause/resume refresh
document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
        stopAutoRefresh();
    } else {
        startAutoRefresh();
        // Immediate refresh when tab becomes visible
        loadKPIs();
        loadTrafficChart();
        loadAltitudeChart();
        loadForecastChart();
        loadLiveMap();
    }
});

// ---- Initialization ----

document.addEventListener('DOMContentLoaded', () => {
    console.log('Dashboard initialized');

    // Initial load
    loadKPIs();
    loadTrafficChart();
    loadAltitudeChart();
    loadForecastChart();
    loadLiveMap();

    // Start auto-refresh
    startAutoRefresh();

    // Handle window resize for Plotly
    window.addEventListener('resize', () => {
        if (trafficChart) Plotly.Plots.resize(document.getElementById('traffic-chart'));
        if (altitudeChart) Plotly.Plots.resize(document.getElementById('altitude-chart'));
        if (forecastChart) Plotly.Plots.resize(document.getElementById('forecast-chart'));
        if (liveMap) Plotly.Plots.resize(document.getElementById('live-map'));
    });
});

// Cleanup on page unload
window.addEventListener('beforeunload', stopAutoRefresh);