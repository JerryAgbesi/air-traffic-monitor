# Project status

Last updated 2026-11-27. This page covers what has been built so far, what was deployed on the Azure VMs, what state the cluster is in today, and what still needs a decision.

## Summary

- Data collection works end to end on the cluster. It ran from 2026-11-25 until 2026-11-26 06:29 UTC, then stopped when worker2–5 went offline.
- The repo has two Spark pipelines that do overlapping work: `spark/` (this branch, the one deployed) and `jobs/` (PR #3, not deployed yet). We need to keep one.
- Still to do: the ML forecast, the dashboard, and the 1/2/4-executor scaling experiment.

## What has been done

| Date | Work | Where |
|---|---|---|
| 2026-11-03 | Checkpoint 1 brief: data source, KPIs, Kafka message design, ML task | report |
| PR #1 | OpenSky producer (OAuth2, 75 s polling of the France box), JSONL archive consumer, single-node Kafka in Docker | `producer/`, `consumer/`, `docker-compose.yml` |
| PR #2 | Producer publishes each API response unchanged, one message per snapshot, keyed by snapshot time. The per-aircraft split moved to Spark. | `producer/` |
| PR #3 | Spark jobs for a TimescaleDB setup: raw sink, 5-min aggregates, live KPIs, nightly cleaning, JSONL backfill, plus a schema and deploy scripts. Merged but **never run on the VMs**: the checkout on the master is still at PR #2. | `jobs/`, `sql/`, `deploy/`, `docs/PIPELINE.md` |
| 2026-11-25 | Native deployment on the 6 VMs: Kafka cluster, producer service, raw stream on YARN, InfluxDB, curated batch job. Details below. | this branch: `spark/`, `docs/KAFKA_CLUSTER_PLAN.md` |

## What runs on the cluster (deployed 2026-11-25)

```
master   opensky-producer.service ──► Kafka opensky.states.raw on worker2, worker3, worker4
                                        (6 partitions, RF 3, min ISR 2, 14 days)
                                                 │
YARN     opensky-ingest-raw (stream, every 5 min) ◄┘
             └──► HDFS /data/raw/opensky_states/date=YYYY-MM-DD/      (Parquet, all fields, typed)
                      │
YARN     opensky-curate (batch, run by hand)
             ├──► HDFS /data/curated/opensky_states/date=YYYY-MM-DD/  (cleaned, one row per position)
             └──► InfluxDB on worker5 (10.0.0.47:8086), bucket opensky: measurements aircraft, traffic
```

| Component | Details |
|---|---|
| VMs | 1 master + 5 workers, each 2 vCPU, 3.8 GiB RAM, no swap. Spark 4.0.3 (Scala 2.13), Hadoop in `/usr/local/hadoop`. |
| Kafka | 4.3.2 in KRaft mode on worker2–4, each broker also a controller. `kafka.service`, heap 768 MB, data in `/var/lib/kafka/data`. Config in `/opt/kafka/config/cluster.properties`. |
| Producer | `opensky-producer.service` on the master, user `opensky`, code in `/opt/air-traffic-monitor`. Credentials in `/etc/opensky/producer.env` (root only). Each poll costs 3 credits. |
| Raw stream | `spark/ingest_raw.py`, started with `spark/submit_ingest_raw.sh` in YARN cluster mode (1 GB driver, one 1 GB executor). Keeps every OpenSky field plus the Kafka partition and offset. Checkpoint in `/checkpoints/opensky/ingest_raw`. |
| Curated job + serving | `spark/curate.py`, run with `spark/submit_curate.sh [--date YYYY-MM-DD]`. It drops rows with no position, positions older than 60 s, and impossible speeds or altitudes, keeps one row per `(icao24, time_position)`, and adds a flight phase. It rewrites whole days in HDFS and writes InfluxDB points, which overwrite on re-run. The InfluxDB token is passed with `--files`, so it never appears on the command line. |
| YARN tuning | To leave room for Kafka: NodeManager memory and max allocation down from 6144 to 2048 MB, min allocation 512 MB, executors 1g, `maximum-am-resource-percent` up from 0.1 to 0.5. At 0.1 the always-on stream used the whole application-master budget and every other job waited in ACCEPTED. Old files are kept as `*.bak-<timestamp>`. |

Tested on 2026-11-25:
- Killing and restarting `opensky-ingest-raw` left no duplicate rows. The file sink and the checkpoint resume from the last committed offsets.
- Running `curate.py` twice on the same day left every count unchanged in both HDFS and InfluxDB.

## Cluster state on 2026-11-27

Checked at about 14:30 UTC:

| Part | State |
|---|---|
| master, worker1 | Running, rebooted about 12 minutes before the check. HDFS and YARN daemons **not started** (`hdfs dfs` gets connection refused on `master:9000`). |
| worker2–5 | **Off** (no route to host), so Kafka and InfluxDB are down. |
| Producer | Restarting in a loop (18 restarts): it can't reach any broker. Last snapshot sent: 2026-11-26 06:29:26 UTC, 504 aircraft. |

There is no data from 2026-11-26 06:30 UTC onward, and Kafka wasn't running to buffer any. The forecast needs about 3 weeks of continuous history, so every day the cluster stays down pushes the ML work back.

To bring it back, in this order:
1. Start worker1–5 in the Azure portal. `kafka.service` is enabled at boot on worker2–4.
2. On the master: `/usr/local/hadoop/sbin/start-dfs.sh`, then `/usr/local/hadoop/sbin/start-yarn.sh`. Wait for `hdfs dfsadmin -safemode get` to say OFF.
3. Check Kafka: `/opt/kafka/bin/kafka-topics.sh --bootstrap-server worker2:9092 --describe --topic opensky.states.raw`. Every partition should list 3 in-sync replicas.
4. The producer reconnects on its own at its next restart. Check with `journalctl -u opensky-producer -f`.
5. `spark/submit_ingest_raw.sh`. It resumes from its checkpoint.

## Two pipelines in the repo

PR #3 and this branch were written in parallel against different assumptions, and they don't fit together as they stand:

| | `spark/` (deployed) | `jobs/` (PR #3) |
|---|---|---|
| Hostnames | `master`, `worker1–5` | `node1–6` |
| Kafka brokers | worker2–4 | "node2" and others |
| Raw zone path | `/data/raw/opensky_states` | `/lake/raw/opensky_states` |
| Serving database | InfluxDB on worker5 | TimescaleDB on node6 |
| Spark / Scala | 4.0.3 / 2.13 | `deploy/submit.sh` looks for `spark-core_2.12-*.jar`, which doesn't exist on this cluster, so it fails before submitting |
| Streams | 1 (raw) plus 1 batch job | 3 (raw, 5-min aggregates, live KPIs) plus nightly cleaning |
| Dedup key | `(icao24, time_position)` in the curated job | `(icao24, snapshot_time)` in streams, `(icao24, event_time)` nightly |

PR #3 has things this branch lacks: per-region 5-minute aggregates, live KPIs for the dashboard, the `opensky.traffic.5min` topic, and a pipeline health table. This branch has the parts that are proven on the real VMs: memory sizing that fits 3.8 GiB nodes, the YARN application-master fix, and an exactly-once raw stream that is already filling HDFS.

Suggested merge: keep the deployed raw stream and its paths. Port `traffic_agg` and `live_kpis` onto them, writing to whichever database we keep. Fix the Scala version in `deploy/submit.sh`. We can only fit one serving database in RAM, so InfluxDB vs TimescaleDB is a team decision. The project brief accepts either, as long as writes are idempotent.

Running every PR #3 stream next to the raw stream probably won't fit in YARN. Each one asks for a 1 GB driver plus 2 × 1 GB executors, and the cluster has 5 × 2 GB of NodeManager memory.

## Next steps

- [x] **ML feature engineering** (`ml/features.py`): 5-min window aggregation, lag features (t, t-15, t-30, t-60), rolling stats, time features, target (t+30min)
- [x] **ML training** (`ml/train.py`): Linear Regression + Random Forest with Spark MLlib, chronological split, persistence baseline, cross-validation support, model saving
- [x] **ML evaluation** (`ml/evaluate.py`): MAE/RMSE/R2 comparison, error analysis by hour/weekday/traffic level, detailed reports
- [x] **ML forecasting** (`ml/forecast.py`): Real-time 30-min ahead inference from latest curated data, Kafka output for dashboard
- [x] **ML deployment scripts** (`ml/submit_train.sh`, `ml/submit_forecast.sh`): YARN cluster submission
- [x] **Dashboard** (`dashboard/`): Flask + Plotly with KPIs, traffic trends, altitude, forecast vs actual, live aircraft map
- [ ] Bring the cluster back up (steps above) and decide who checks it each day.
- [ ] Choose between InfluxDB and TimescaleDB, then merge the two pipelines into one.
- [ ] Schedule `curate.py`. A systemd timer on the master was planned but isn't set up yet.
- [ ] Run ML training on cluster once enough data accumulated (need ~3 weeks continuous)
- [ ] Scaling experiment with 1, 2 and 4 executors.
- [ ] Compare with Ray, XGBoost-on-Spark or TorchDistributor (required for a group of 3).
- [ ] Fix `consumer/archive_consumer.py`: it crashes with `_NO_OFFSET` when a batch contains only error messages, because it then commits with nothing to commit.

## Where to find things

| Path | Contents |
|---|---|
| `docs/KAFKA_CLUSTER_PLAN.md` | Kafka install plan. Its "Status" section describes what was really deployed and overrides the plan sections below it, which still mention worker1–3 and Kafka 3.9. |
| `docs/PIPELINE.md` | PR #3 setup guide (TimescaleDB version) |
| `spark/` | Deployed YARN jobs and their submit scripts |
| `jobs/`, `deploy/`, `sql/` | PR #3 pipeline |
| `producer/`, `consumer/` | Producer (runs on the master) and local JSONL archiver |
