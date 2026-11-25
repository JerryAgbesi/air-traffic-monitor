# OpenSky pipeline: API → Kafka → Spark on YARN → HDFS + TimescaleDB

This folder holds the ingestion, cleaning and storage part of the project.
Names (topics, HDFS paths, checkpoints, tables) are the ones in the Checkpoint 2 report.

```
node6  producer.py ──► Kafka opensky.states.raw (6 partitions, RF 3,
                       one message = one whole API snapshot, key = snapshot time)
                              │
     ┌────────────────────────┼──────────────────────────┐
     ▼                        ▼                          ▼
 raw_sink (5 min)       traffic_agg (1 min)        live_kpis (30 s)
 HDFS /lake/raw/        TimescaleDB traffic_5min   TimescaleDB snapshot_kpis
 opensky_states/        + Kafka opensky.traffic.5min          + live_positions
 date=YYYY-MM-DD/
     │
     ▼  nightly (cron on node1)
 clean_daily ──► HDFS /lake/cleaned/states/date=YYYY-MM-DD/
```

| File | What it does |
|---|---|
| `producer/producer.py` | Calls `/api/states/all` for the France box every 75 s and sends **the whole API response as one message**, exactly as received (key and timestamp = snapshot time, `acks=all`, idempotent), as in PR #2. Skips a snapshot OpenSky serves twice. Writes credits left etc. to `pipeline_health`. |
| `jobs/common.py` | Shared code: splits each snapshot message into one row per aircraft with named columns (also accepts per-aircraft JSON), cleaning rules, regions, TimescaleDB upsert, health reporter. |
| `jobs/raw_sink.py` | Kafka → one row per aircraft → Parquet, partitioned by UTC date, values as received + Kafka partition/offset. Exactly-once (file sink + checkpoint). |
| `jobs/traffic_agg.py` | Clean → dedupe on (icao24, snapshot) within a 10-min watermark → 5-min windows per region (France, Paris, North-East, West, South-West, South-East). Upsert into `traffic_5min`, publish to `opensky.traffic.5min`. |
| `jobs/live_kpis.py` | Per-snapshot KPIs (airborne by region, on ground, flight-level mix, CDG/ORY climbing/descending, origin countries) → `snapshot_kpis`; aircraft positions for the map → `live_positions` (kept 2 days). |
| `jobs/clean_daily.py` | Nightly batch: raw day → cleaned day. Cleaning rules + dedupe on (icao24, event_time) + region/flight-level columns, 4 compact files per day. Safe to re-run. |
| `jobs/backfill_jsonl.py` | One-off: imports the first collector's `data/raw/date=*/states.jsonl` files into the raw zone. |
| `sql/timescaledb_schema.sql` | Database `opensky`, users `spark_writer` / `grafana_ro`, hypertables, staging tables. |
| `deploy/` | `create_topics.sh`, `submit.sh` (spark-submit to YARN), `opensky-producer.service` (systemd). |

**Cleaning rules** (`common.clean`): drop rows without a valid 6-hex `icao24`, event time or position;
keep positions inside 41.3–51.1 N, 5.2 W–9.6 E; set impossible values to NULL (altitude outside −500 m…20 km,
speed > 400 m/s, |vertical rate| > 100 m/s, track outside 0–360°); lower-case `icao24`, trim/upper-case `callsign`.

**No duplicates after a crash:** the producer is idempotent, `raw_sink` uses Spark's exactly-once file sink,
and every TimescaleDB write is `INSERT … ON CONFLICT … DO UPDATE` through a staging table, so a replayed
micro-batch overwrites rows. Tested locally by killing `raw_sink` with `kill -9` mid-run and restarting it:
no Kafka record was written twice.

---

## Setup (once)

Assumes `/etc/hosts` has node1…node6 on every VM, Hadoop/YARN, Spark 3.5.x and Kafka are running,
and TimescaleDB is installed on node6. Paths below use `/opt/opensky/pipeline`; change them if yours differ.

### 1. Copy this folder to the VMs

```bash
# Run on: node1 (master) AND node6
sudo mkdir -p /opt/opensky && sudo chown $USER /opt/opensky
# copy the pipeline/ folder here (scp, or git clone the team repo), then:
cd /opt/opensky/pipeline
cp .env.example .env && chmod 600 .env
nano .env        # OpenSky client id/secret, TimescaleDB password
```

### 2. Kafka topics

```bash
# Run on: node2 (any one Kafka node)
cd /opt/opensky/pipeline && bash deploy/create_topics.sh
```
Expected: `opensky.states.raw` with 6 partitions and `opensky.traffic.5min` with 3, each `ReplicationFactor: 3`.

### 3. TimescaleDB schema and users

```bash
# Run on: node6
cd /opt/opensky/pipeline
sudo -u postgres psql -v writer_pw="'CHOOSE-A-PASSWORD'" -v grafana_pw="'ANOTHER-PASSWORD'" \
     -f sql/timescaledb_schema.sql
```
Put the `spark_writer` password in `.env` on node1 and node6 (`TSDB_PASSWORD` and `TSDB_DSN`).
Postgres must accept connections from the VNet:

```bash
# Run on: node6
# postgresql.conf:  listen_addresses = '*'
# pg_hba.conf, add: host  opensky  spark_writer,grafana_ro  10.0.0.0/24  scram-sha-256
sudo systemctl restart postgresql
```

### 4. HDFS folders

```bash
# Run on: node1 (master)
hdfs dfs -mkdir -p /lake/raw/opensky_states /lake/cleaned/states /lake/curated /models /checkpoints
hdfs dfs -ls /
```

### 5. Producer as a service

```bash
# Run on: node6
python3 -m venv /opt/opensky/venv
/opt/opensky/venv/bin/pip install -r /opt/opensky/pipeline/requirements.txt
# if the old docker / laptop producer is still running, STOP it first (only one producer may run)
sudo sed -i "s/^User=.*/User=$USER/" /opt/opensky/pipeline/deploy/opensky-producer.service
sudo cp /opt/opensky/pipeline/deploy/opensky-producer.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now opensky-producer
journalctl -u opensky-producer -f      # expect "Snapshot ...: sent N aircraft, credits left ..." every 75 s
```

Check messages arrive:
```bash
# Run on: node2 (any Kafka node)
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server node2:9092 \
  --topic opensky.states.raw --property print.key=true --max-messages 1
```
Expected: one long JSON line per snapshot, key = the snapshot's Unix time.

### 6. Start the three streaming jobs on YARN

```bash
# Run on: node1 (master)
cd /opt/opensky/pipeline
nohup deploy/submit.sh raw_sink    > ~/raw_sink.submit.log 2>&1 &
nohup deploy/submit.sh traffic_agg > ~/traffic_agg.submit.log 2>&1 &
nohup deploy/submit.sh live_kpis   > ~/live_kpis.submit.log 2>&1 &
yarn application -list          # three apps RUNNING
```
Each job uses 2 executors × 1 core × 1 GB plus a 1 GB driver. Adjust in `deploy/submit.sh`
if YARN cannot fit all three. The first submit downloads the Kafka and PostgreSQL jars from Maven
Central, so node1 needs outbound internet.

Check the output:
```bash
# Run on: node1 (master), after ~10 minutes
hdfs dfs -ls /lake/raw/opensky_states/
hdfs dfs -du -h /lake/raw/opensky_states/
```
```bash
# Run on: node6
sudo -u postgres psql -d opensky -c "SELECT * FROM traffic_5min ORDER BY window_start DESC LIMIT 6;"
sudo -u postgres psql -d opensky -c "SELECT kpi, label, value FROM snapshot_kpis WHERE snapshot_time = (SELECT max(snapshot_time) FROM snapshot_kpis) ORDER BY kpi, value DESC;"
sudo -u postgres psql -d opensky -c "SELECT source, metric, value FROM pipeline_health WHERE time > now() - interval '2 minutes' ORDER BY source, metric;"
```

### 7. Nightly cleaning

```bash
# Run on: node1 (master)
crontab -e
# add:  15 1 * * *  cd /opt/opensky/pipeline && deploy/submit.sh clean_daily >> $HOME/clean_daily.log 2>&1
```
Manual run for a range of days: `deploy/submit.sh clean_daily 2026-11-20 2026-11-24`.

### 8. Old collector history (once, only if you have the JSON-lines files)

```bash
# Run on: node1 (master), after copying the old data/raw folder to node1
hdfs dfs -mkdir -p /staging/old_archive
hdfs dfs -put data/raw/date=* /staging/old_archive/
deploy/submit.sh backfill_jsonl hdfs:///staging/old_archive
deploy/submit.sh clean_daily 2026-11-03 2026-11-25     # re-clean the backfilled days
```

---

## Operating notes

* **Stop a stream:** `yarn application -kill <appId>` (on node1). Restarting with `deploy/submit.sh <job>` resumes from its checkpoint.
* **Changing a job's logic:** give it a new checkpoint (e.g. `CHECKPOINT_ROOT` stays, rename the folder in the job to `traffic_agg_v2`), as in the report.
* **Fault-tolerance demo:** kill one executor's NodeManager or a Kafka broker (`sudo systemctl stop kafka` on node3); jobs keep running (RF 3, min ISR 2). Kill `raw_sink` and restart it; count rows per `(kafka_offset, kafka_partition, icao24)` to show nothing is duplicated.
* **Grafana** (PostgreSQL data source `node6:5432/opensky`, user `grafana_ro`), example panels:
  * Airborne now: `SELECT snapshot_time AS time, label AS metric, value FROM snapshot_kpis WHERE kpi='airborne' AND $__timeFilter(snapshot_time) ORDER BY 1`
  * 5-min curve: `SELECT window_start AS time, airborne_avg FROM traffic_5min WHERE region='France' AND $__timeFilter(window_start) ORDER BY 1`
  * Top 10 countries: `SELECT label, value FROM snapshot_kpis WHERE kpi='origin_country' AND snapshot_time=(SELECT max(snapshot_time) FROM snapshot_kpis) ORDER BY value DESC LIMIT 10`
  * Live map (Geomap panel): `SELECT lat, lon, baro_altitude_m, callsign FROM live_positions WHERE snapshot_time=(SELECT max(snapshot_time) FROM live_positions)`

## Differences from the Checkpoint 2 draft

* Kafka carries **one message per API snapshot**, kept exactly as received, keyed by snapshot time (team decision in PR #2). The split into one row per aircraft happens in Spark (`common.parse_values`), before the raw zone is written.
* Streams dedupe on **(icao24, snapshot_time)**, not (icao24, event_time): when an aircraft has not sent a new position, OpenSky repeats the old one in the next snapshot, and dropping it would undercount traffic. `clean_daily` still dedupes on (icao24, event_time) for the trajectory data.
* 5-minute windows use the **snapshot time** so each 75 s snapshot falls in exactly one window.
* New table `live_positions` for the live map; `traffic_5min` has extra columns `aircraft_distinct`, `snapshots`, `updated_at`.
