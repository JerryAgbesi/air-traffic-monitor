-- TimescaleDB schema for the OpenSky pipeline.
-- Run on node6:  sudo -u postgres psql -v writer_pw="'...'" -v grafana_pw="'...'" -f sql/timescaledb_schema.sql
-- Safe to run again (IF NOT EXISTS everywhere).

SELECT 'CREATE DATABASE opensky'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'opensky') \gexec
\c opensky

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Users: spark_writer (Spark jobs + producer), grafana_ro (read only)
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'spark_writer') THEN
    CREATE ROLE spark_writer LOGIN; END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_ro') THEN
    CREATE ROLE grafana_ro LOGIN; END IF;
END $$;
ALTER ROLE spark_writer PASSWORD :writer_pw;
ALTER ROLE grafana_ro  PASSWORD :grafana_pw;

-- 5-minute traffic per region (written by traffic_agg)
CREATE TABLE IF NOT EXISTS traffic_5min (
  window_start      timestamptz NOT NULL,
  window_end        timestamptz NOT NULL,
  region            text        NOT NULL,   -- France, Paris, North-East, West, South-West, South-East
  airborne_avg      double precision,       -- mean airborne count per snapshot
  climbing          double precision,       -- mean per snapshot, vertical rate > 1 m/s
  descending        double precision,       -- mean per snapshot, vertical rate < -1 m/s
  aircraft_distinct bigint,                 -- distinct airborne aircraft seen in the window
  snapshots         bigint,
  events            bigint,
  updated_at        timestamptz,
  PRIMARY KEY (window_start, region)
);
SELECT create_hypertable('traffic_5min', 'window_start', if_not_exists => TRUE,
                         chunk_time_interval => INTERVAL '7 days');

-- Per-snapshot KPIs (written by live_kpis)
CREATE TABLE IF NOT EXISTS snapshot_kpis (
  snapshot_time timestamptz NOT NULL,
  kpi           text        NOT NULL,     -- airborne, on_ground, flight_level, paris_airports, origin_country
  label         text        NOT NULL,
  value         double precision,
  PRIMARY KEY (snapshot_time, kpi, label)
);
SELECT create_hypertable('snapshot_kpis', 'snapshot_time', if_not_exists => TRUE,
                         chunk_time_interval => INTERVAL '1 day');

-- Aircraft positions for the live map (written by live_kpis, kept 2 days)
CREATE TABLE IF NOT EXISTS live_positions (
  snapshot_time    timestamptz NOT NULL,
  icao24           text        NOT NULL,
  callsign         text,
  origin_country   text,
  lat              double precision,
  lon              double precision,
  baro_altitude_m  double precision,
  velocity_ms      double precision,
  true_track_deg   double precision,
  vertical_rate_ms double precision,
  region           text,
  PRIMARY KEY (snapshot_time, icao24)
);
SELECT create_hypertable('live_positions', 'snapshot_time', if_not_exists => TRUE,
                         chunk_time_interval => INTERVAL '6 hours');
SELECT add_retention_policy('live_positions', INTERVAL '2 days', if_not_exists => TRUE);

-- Forecasts (written later by the scoring stream)
CREATE TABLE IF NOT EXISTS predictions (
  target_time   timestamptz NOT NULL,
  region        text        NOT NULL,
  model_version text        NOT NULL,
  predicted     double precision,
  made_at       timestamptz,
  PRIMARY KEY (target_time, region, model_version)
);
SELECT create_hypertable('predictions', 'target_time', if_not_exists => TRUE,
                         chunk_time_interval => INTERVAL '7 days');

-- Pipeline metrics (producer + Spark query reporter)
CREATE TABLE IF NOT EXISTS pipeline_health (
  time   timestamptz NOT NULL,
  source text        NOT NULL,   -- producer, raw_sink, traffic_agg, live_kpis, live_positions
  metric text        NOT NULL,
  value  double precision,
  PRIMARY KEY (time, source, metric)
);
SELECT create_hypertable('pipeline_health', 'time', if_not_exists => TRUE,
                         chunk_time_interval => INTERVAL '1 day');
SELECT add_retention_policy('pipeline_health', INTERVAL '30 days', if_not_exists => TRUE);

-- Staging tables for idempotent upserts from Spark (one per table)
CREATE UNLOGGED TABLE IF NOT EXISTS stg_traffic_5min   (LIKE traffic_5min);
CREATE UNLOGGED TABLE IF NOT EXISTS stg_snapshot_kpis  (LIKE snapshot_kpis);
CREATE UNLOGGED TABLE IF NOT EXISTS stg_live_positions (LIKE live_positions);
CREATE UNLOGGED TABLE IF NOT EXISTS stg_predictions    (LIKE predictions);

-- Privileges
GRANT CONNECT ON DATABASE opensky TO spark_writer, grafana_ro;
GRANT USAGE ON SCHEMA public TO spark_writer, grafana_ro;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA public TO spark_writer;
GRANT SELECT ON traffic_5min, snapshot_kpis, live_positions, predictions, pipeline_health TO grafana_ro;
