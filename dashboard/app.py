"""
Real-time Air Traffic Dashboard for French Airspace.

Flask + Plotly dashboard showing:
- Live KPIs (airborne, total, climbing, descending)
- 5-minute traffic trends (last 4 hours)
- 30-minute forecast vs actual
- Live aircraft map
"""
import os
import json
import logging
from datetime import datetime, timedelta, timezone
from functools import lru_cache

from flask import Flask, render_template, jsonify, request
import plotly.graph_objs as go
import plotly.utils
import requests
import pandas as pd

# Configuration
INFLUX_URL = os.getenv("INFLUX_URL", "http://worker5:8086")
INFLUX_ORG = os.getenv("INFLUX_ORG", "esilv")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "air_traffic")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "worker2:9092,worker3:9092,worker4:9092")
FORECAST_TOPIC = os.getenv("FORECAST_TOPIC", "opensky.forecast.30min")

# Flask app
app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---- InfluxDB Query Helpers ----

def influx_query(flux_query):
    """Execute Flux query against InfluxDB."""
    if not INFLUX_TOKEN:
        logger.warning("INFLUX_TOKEN not set")
        return []

    url = f"{INFLUX_URL}/api/v2/query"
    headers = {
        "Authorization": f"Token {INFLUX_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/csv"
    }
    payload = {"query": flux_query, "org": INFLUX_ORG}

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        return parse_csv_response(r.text)
    except Exception as e:
        logger.error(f"InfluxDB query failed: {e}")
        return []


def parse_csv_response(csv_text):
    """Parse InfluxDB CSV response into list of dicts."""
    lines = csv_text.strip().split("\n")
    if len(lines) < 2:
        return []

    # Skip annotation lines starting with #
    data_lines = [l for l in lines if not l.startswith("#")]
    if len(data_lines) < 2:
        return []

    headers = data_lines[0].split(",")
    rows = []
    for line in data_lines[1:]:
        values = line.split(",")
        if len(values) == len(headers):
            rows.append(dict(zip(headers, values)))
    return rows


def get_traffic_kpis(hours=4):
    """Get traffic KPIs for last N hours."""
    flux = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{hours}h)
      |> filter(fn: (r) => r._measurement == "traffic")
      |> pivot(rowKey: "_time", columnKey: "_field", valueColumn: "_value")
    '''
    return influx_query(flux)


def get_aircraft_positions(limit=500):
    """Get latest aircraft positions for map."""
    flux = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -5m)
      |> filter(fn: (r) => r._measurement == "aircraft")
      |> filter(fn: (r) => r._field == "lat" or r._field == "lon" or r._field == "altitude_m" or r._field == "velocity_ms" or r._field == "heading_deg" or r._field == "callsign" or r._field == "on_ground" or r._field == "flight_phase")
      |> pivot(rowKey: ["_time", "icao24"], columnKey: "_field", valueColumn: "_value")
      |> sort(columns: ["_time"], desc: true)
      |> limit(n: {limit})
    '''
    return influx_query(flux)


def get_forecast_data(hours=4):
    """Get forecast data from Kafka topic (via InfluxDB if stored there)."""
    # Try to read forecast from InfluxDB if we store it there
    flux = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{hours}h)
      |> filter(fn: (r) => r._measurement == "forecast")
      |> pivot(rowKey: "_time", columnKey: "_field", valueColumn: "_value")
    '''
    return influx_query(flux)


# ---- Plotly Chart Builders ----

def build_kpi_cards(traffic_data):
    """Build current KPI values from latest traffic data."""
    if not traffic_data:
        return {
            "airborne": 0, "total": 0, "climbing": 0, "descending": 0,
            "mean_altitude": 0, "timestamp": "No data"
        }

    # Get latest row
    latest = sorted(traffic_data, key=lambda x: x.get("_time", ""), reverse=True)[0]

    return {
        "airborne": int(float(latest.get("airborne", 0))),
        "total": int(float(latest.get("aircraft", 0))),
        "climbing": int(float(latest.get("climbing", 0))),
        "descending": int(float(latest.get("descending", 0))),
        "mean_altitude": round(float(latest.get("mean_altitude_m", 0)), 0),
        "timestamp": latest.get("_time", "Unknown")
    }


def build_traffic_chart(traffic_data):
    """Build traffic trend chart (5-min windows)."""
    if not traffic_data:
        return go.Figure().to_json()

    df = pd.DataFrame(traffic_data)
    df["_time"] = pd.to_datetime(df["_time"])
    df = df.sort_values("_time")

    # Convert numeric columns
    for col in ["aircraft", "airborne", "climbing", "descending", "mean_altitude_m"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    fig = go.Figure()

    # Total aircraft
    fig.add_trace(go.Scatter(
        x=df["_time"], y=df["aircraft"],
        name="Total Aircraft", line=dict(color="#1f77b4", width=2),
        hovertemplate="%{y} aircraft<br>%{x}<extra></extra>"
    ))

    # Airborne
    fig.add_trace(go.Scatter(
        x=df["_time"], y=df["airborne"],
        name="Airborne", line=dict(color="#2ca02c", width=2),
        hovertemplate="%{y} airborne<br>%{x}<extra></extra>"
    ))

    # Climbing
    fig.add_trace(go.Scatter(
        x=df["_time"], y=df["climbing"],
        name="Climbing", line=dict(color="#ff7f0e", width=1, dash="dot"),
        hovertemplate="%{y} climbing<br>%{x}<extra></extra>"
    ))

    # Descending
    fig.add_trace(go.Scatter(
        x=df["_time"], y=df["descending"],
        name="Descending", line=dict(color="#d62728", width=1, dash="dot"),
        hovertemplate="%{y} descending<br>%{x}<extra></extra>"
    ))

    fig.update_layout(
        title="Air Traffic Over French Airspace (5-min windows)",
        xaxis_title="Time (UTC)",
        yaxis_title="Aircraft Count",
        hovermode="x unified",
        template="plotly_white",
        height=400,
        margin=dict(l=50, r=20, t=50, b=50),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


def build_altitude_chart(traffic_data):
    """Build mean altitude chart."""
    if not traffic_data:
        return go.Figure().to_json()

    df = pd.DataFrame(traffic_data)
    df["_time"] = pd.to_datetime(df["_time"])
    df = df.sort_values("_time")
    df["mean_altitude_m"] = pd.to_numeric(df["mean_altitude_m"], errors="coerce")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["_time"], y=df["mean_altitude_m"],
        name="Mean Altitude", line=dict(color="#9467bd", width=2),
        fill="tozeroy", fillcolor="rgba(148, 103, 189, 0.1)",
        hovertemplate="%{y:.0f} m<br>%{x}<extra></extra>"
    ))

    fig.update_layout(
        title="Mean Aircraft Altitude",
        xaxis_title="Time (UTC)",
        yaxis_title="Altitude (m)",
        template="plotly_white",
        height=300,
        margin=dict(l=50, r=20, t=50, b=50)
    )

    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


def build_forecast_chart(forecast_data, traffic_data):
    """Build forecast vs actual comparison chart."""
    fig = go.Figure()

    # Actual traffic (airborne)
    if traffic_data:
        df = pd.DataFrame(traffic_data)
        df["_time"] = pd.to_datetime(df["_time"])
        df = df.sort_values("_time")
        df["airborne"] = pd.to_numeric(df["airborne"], errors="coerce")

        fig.add_trace(go.Scatter(
            x=df["_time"], y=df["airborne"],
            name="Actual Airborne", line=dict(color="#2ca02c", width=2),
            hovertemplate="%{y} aircraft<br>%{x}<extra></extra>"
        ))

    # Forecast
    if forecast_data:
        fdf = pd.DataFrame(forecast_data)
        if "valid_from" in fdf.columns:
            fdf["valid_from"] = pd.to_datetime(fdf["valid_from"])
            fdf = fdf.sort_values("valid_from")
            fdf["predicted_airborne"] = pd.to_numeric(fdf["predicted_airborne"], errors="coerce")

            fig.add_trace(go.Scatter(
                x=fdf["valid_from"], y=fdf["predicted_airborne"],
                name="Forecast (30-min ahead)", line=dict(color="#ff7f0e", width=2, dash="dash"),
                mode="lines+markers",
                hovertemplate="%{y:.1f} predicted<br>%{x}<extra></extra>"
            ))

    fig.update_layout(
        title="30-Minute Forecast vs Actual",
        xaxis_title="Time (UTC)",
        yaxis_title="Airborne Aircraft",
        hovermode="x unified",
        template="plotly_white",
        height=400,
        margin=dict(l=50, r=20, t=50, b=50),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


def build_live_map(aircraft_data):
    """Build live aircraft map (Plotly scattergeo)."""
    if not aircraft_data:
        fig = go.Figure()
        fig.update_layout(
            title="Live Aircraft Positions (last 5 min)",
            template="plotly_white",
            height=500,
            margin=dict(l=0, r=0, t=50, b=0)
        )
        return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

    df = pd.DataFrame(aircraft_data)

    # Parse numeric columns
    for col in ["lat", "lon", "altitude_m", "velocity_ms", "heading_deg"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Filter valid positions
    df = df.dropna(subset=["lat", "lon"])
    df = df[(df["lat"] >= 41) & (df["lat"] <= 51) & (df["lon"] >= -5) & (df["lon"] <= 10)]

    if df.empty:
        return go.Figure().to_json()

    # Color by flight phase
    phase_colors = {
        "climb": "#ff7f0e", "descent": "#d62728",
        "level": "#1f77b4", "ground": "#7f7f7f"
    }
    df["color"] = df["flight_phase"].map(phase_colors).fillna("#1f77b4")

    # Size by altitude (log scale for visibility)
    df["size"] = (df["altitude_m"].fillna(0) / 1000).clip(1, 12).apply(lambda x: max(5, min(20, x)))

    fig = go.Figure()

    # Add scattergeo trace
    fig.add_trace(go.Scattergeo(
        lon=df["lon"], lat=df["lat"],
        mode="markers",
        marker=dict(
            size=df["size"],
            color=df["color"],
            opacity=0.8,
            line=dict(width=0.5, color="white")
        ),
        text=df.apply(lambda r: f"{r.get('callsign', 'N/A').strip()}<br>"
                               f"ICAO: {r.get('icao24', 'N/A')}<br>"
                               f"Alt: {r.get('altitude_m', 0):.0f}m<br>"
                               f"Spd: {r.get('velocity_ms', 0):.0f} m/s<br>"
                               f"Phase: {r.get('flight_phase', 'N/A')}", axis=1),
        hoverinfo="text",
        name="Aircraft"
    ))

    # France bounds
    fig.update_geos(
        projection_type="mercator",
        lataxis_range=[41, 51],
        lonaxis_range=[-5, 10],
        showland=True, landcolor="rgb(240, 240, 240)",
        showocean=True, oceancolor="rgb(230, 240, 250)",
        showcountries=True, countrycolor="rgb(200, 200, 200)",
        showsubunits=True, subunitcolor="rgb(220, 220, 220)"
    )

    fig.update_layout(
        title="Live Aircraft Over France (last 5 min)",
        template="plotly_white",
        height=550,
        margin=dict(l=0, r=0, t=50, b=0),
        showlegend=False
    )

    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


# ---- Routes ----

@app.route("/")
def index():
    """Main dashboard page."""
    return render_template("index.html")


@app.route("/api/kpis")
def api_kpis():
    """Current KPI values."""
    traffic = get_traffic_kpis(hours=1)
    kpis = build_kpi_cards(traffic)
    return jsonify(kpis)


@app.route("/api/traffic-chart")
def api_traffic_chart():
    """Traffic trend chart."""
    hours = request.args.get("hours", 4, type=int)
    traffic = get_traffic_kpis(hours=hours)
    chart = build_traffic_chart(traffic)
    return chart


@app.route("/api/altitude-chart")
def api_altitude_chart():
    """Altitude chart."""
    hours = request.args.get("hours", 4, type=int)
    traffic = get_traffic_kpis(hours=hours)
    chart = build_altitude_chart(traffic)
    return chart


@app.route("/api/forecast-chart")
def api_forecast_chart():
    """Forecast vs actual chart."""
    hours = request.args.get("hours", 4, type=int)
    traffic = get_traffic_kpis(hours=hours)
    forecast = get_forecast_data(hours=hours)
    chart = build_forecast_chart(forecast, traffic)
    return chart


@app.route("/api/live-map")
def api_live_map():
    """Live aircraft map."""
    limit = request.args.get("limit", 500, type=int)
    aircraft = get_aircraft_positions(limit=limit)
    chart = build_live_map(aircraft)
    return chart


@app.route("/health")
def health():
    """Health check endpoint."""
    return jsonify({"status": "ok", "time": datetime.now(timezone.utc).isoformat()})


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=port, debug=debug)