# Kafka on the 6-VM Cluster: Setup Plan

Goal: run a native (no Docker) Apache Kafka cluster on the existing Azure VMs
(1 master + 5 workers, already running HDFS and Spark/YARN), so that

```
OpenSky API -> producer.py -> Kafka (opensky.states.raw) -> Spark Structured Streaming on YARN -> HDFS raw zone (Parquet, partitioned by date)
```

works end to end and survives the loss of one VM.

## Status (2026-11-25): installed

What was actually deployed. Where it differs from the sections below, this wins.

| Item            | Deployed                                                              |
|-----------------|-----------------------------------------------------------------------|
| Brokers         | **worker2, worker3, worker4** (node ids 2, 3, 4), combined broker+controller |
| Version         | Kafka **4.3.2** (Scala 2.13) on the system Java 17                    |
| Cluster ID      | `INWwLAZtQeCRDlxksj6n7Q` (also in `~/kafka-cluster-id.txt` on master) |
| Bootstrap       | `worker2:9092,worker3:9092,worker4:9092`                              |
| Config          | `/opt/kafka/config/cluster.properties`                                |
| Data            | `/var/lib/kafka/data` on the root disk (`/mnt` is Azure's temporary disk, wiped on deallocate) |
| Service         | `kafka.service`, user `kafka`, heap `-Xms768m -Xmx768m`, enabled at boot |
| Client tools    | `/opt/kafka/bin` on master                                            |
| Topic           | `opensky.states.raw`: 6 partitions, RF 3, min ISR 2, 14 days, 5 MB max message |

VM size: 2 vCPU, 3.8 GiB RAM, no swap. To leave room for Kafka:
- `yarn-site.xml` (same on all 6 nodes): `nodemanager.resource.memory-mb` 6144 → **2048**,
  `scheduler.maximum-allocation-mb` 6144 → **2048**, new `scheduler.minimum-allocation-mb` = **512**.
- `/opt/spark/conf/spark-defaults.conf` on master: `spark.executor.memory` 2g → **1g**
  (2g + overhead would be rounded up to 3 GB and never fit in a 2 GB NodeManager).
- `capacity-scheduler.xml` (all nodes): `maximum-am-resource-percent` 0.1 → **0.5**. At 0.1 the
  always-on stream's driver used the whole AM budget and every other YARN app stayed ACCEPTED.
- Backups: `*.bak-<timestamp>` next to each file.

Pipeline running since 2026-11-25:
- **Producer**: `opensky-producer.service` on master, user `opensky`, code in `/opt/air-traffic-monitor`,
  credentials in `/etc/opensky/producer.env` (root, 600) via `EnvironmentFile=`.
- **Raw zone**: YARN app `opensky-ingest-raw` (`spark/ingest_raw.py`, started with
  `spark/submit_ingest_raw.sh`, cluster mode, 1g driver + one 1g executor). Writes
  `/data/raw/opensky_states/date=YYYY-MM-DD/` every 5 min, one file per batch; checkpoint
  `/checkpoints/opensky/ingest_raw`. Verified: no duplicates after a kill and restart.
- **InfluxDB 2.10.1** on worker5: `10.0.0.47:8086` (private IP only), org `esilv`, bucket
  `opensky` (30 days). Admin credentials in `/etc/influxdb/admin.env` on worker5 (root, 600).
  Memory capped (systemd `MemoryMax=1G`, 256 MB cache). Spark uses a token limited to the
  `opensky` bucket, in `~/.config/opensky/influx.env` on master (600).
- **Curated zone + serving**: `spark/curate.py` (run with `spark/submit_curate.sh`, cluster mode).
  Writes `/data/curated/opensky_states/date=…/` (the processed days are overwritten as a whole) and InfluxDB
  measurements `aircraft` (tag `icao24`, time = position time) and `traffic` (KPIs per snapshot).
  Verified idempotent: a second run over the same day left all counts unchanged.
  Not scheduled yet (see the `opensky-curate.timer` setup).

---

## 1. Target layout

Kafka runs in **KRaft mode** (no ZooKeeper). Three nodes run in combined
`broker,controller` mode. Three is the smallest quorum that tolerates one
failure, and it leaves the other workers and the master free for HDFS/Spark.

| Host     | Existing roles                      | Kafka role                 | Other new roles          |
|----------|-------------------------------------|----------------------------|--------------------------|
| master   | NameNode, ResourceManager, Spark client | none (client tools only) | `producer.py` (systemd) |
| worker1  | DataNode, NodeManager               | broker + controller, id 1  |                          |
| worker2  | DataNode, NodeManager               | broker + controller, id 2  |                          |
| worker3  | DataNode, NodeManager               | broker + controller, id 3  |                          |
| worker4  | DataNode, NodeManager               | none                       |                          |
| worker5  | DataNode, NodeManager               | none                       |                          |

Why not put a broker on the master: the NameNode and ResourceManager are already
single points of failure. Adding Kafka there means one VM going down takes out
everything at once, and Kafka's page cache would compete with the NameNode heap.

Why not 5 brokers: our load is one message every 75 s. Three brokers already give
replication factor 3; more brokers add RAM pressure on the workers without any
throughput benefit. (Optional extension in section 9.)

Topic settings:

| Setting               | Value                          | Reason                                   |
|-----------------------|--------------------------------|------------------------------------------|
| partitions            | 6                              | already in `producer/config.py`; 2 per broker, lets Spark read with up to 6 tasks |
| replication.factor    | 3                              | one copy per broker                      |
| min.insync.replicas   | 2                              | with `acks=all`, one broker can be down and writes still succeed |
| retention.ms          | 14 days                        | already in config; buffer if the Spark job is down |
| max.message.bytes     | 5 MB                           | each message is a full OpenSky snapshot (see 7.1) |

---

## 2. Prerequisites (all three Kafka nodes, plus master for client tools)

1. **Check RAM and disk** on each worker: `free -h`, `df -h`.
   Budget about 1 GB heap for Kafka plus page cache. If a worker has 8 GB or less,
   lower `yarn.nodemanager.resource.memory-mb` on worker1-3 by ~1.5 GB so YARN
   does not hand that memory to executors.
2. **Java.** Check what Hadoop/Spark use: `java -version`, `echo $JAVA_HOME`.
   - Kafka 3.9.x brokers run on Java 11 or 17, so the existing JDK probably works.
   - Kafka 4.x brokers need Java 17+. If the cluster is on Java 8/11, either pick
     3.9.x or install a separate JDK 17 and point only Kafka at it (via
     `Environment=JAVA_HOME=...` in the systemd unit). Do not change the system
     default, which Hadoop relies on.
   - **Recommendation:** Kafka 3.9.x (latest patch), unless the course requires 4.x.
3. **Hostnames.** `/etc/hosts` on all 6 VMs must resolve `master`, `worker1..5` to
   their **private** IPs (it probably already does for HDFS). Kafka will advertise
   these hostnames, so the producer and Spark must be able to resolve them.
4. **Dedicated user and dirs** (on worker1-3):
   ```bash
   sudo useradd -r -m -d /opt/kafka -s /bin/bash kafka
   sudo mkdir -p /data/kafka-logs && sudo chown kafka:kafka /data/kafka-logs
   ```
   Put `log.dirs` on a different disk/path from the HDFS DataNode directory if the
   VM has a data disk. Never inside `/tmp` (Azure can wipe it on reboot).
5. **Time sync.** `timedatectl` should show NTP synchronised. Snapshot timestamps
   and date partitions depend on it.

---

## 3. Network / Azure

Open these ports **between the VMs only**, on the private subnet (NSG rule with
source = VNet, not Internet):

| Port | Used by                          |
|------|----------------------------------|
| 9092 | clients: producer, Spark, CLI    |
| 9093 | KRaft controller quorum (worker1-3 to each other) |

Also check the OS firewall: `sudo ufw status` or `firewall-cmd --list-all`.

Do **not** expose 9092 publicly. If we need to look at Kafka from a laptop, use an
SSH tunnel through the master: `ssh -L 9092:worker1:9092 user@master` (note: this
only works for metadata calls unless listeners are adjusted; easier to just run the
CLI on the master).

---

## 4. Install (worker1-3, and the tarball only on master)

```bash
KVER=3.9.1        # check https://kafka.apache.org/downloads for the latest 3.9.x
SVER=2.13
cd /tmp
wget https://downloads.apache.org/kafka/$KVER/kafka_$SVER-$KVER.tgz
wget https://downloads.apache.org/kafka/$KVER/kafka_$SVER-$KVER.tgz.sha512
sha512sum -c kafka_$SVER-$KVER.tgz.sha512   # or compare manually
sudo tar -xzf kafka_$SVER-$KVER.tgz -C /opt/kafka --strip-components=1
sudo chown -R kafka:kafka /opt/kafka
```

Script this once (e.g. `infra/kafka/install.sh`) and run it over SSH on each node
so all three are identical.

---

## 5. Configuration

`/opt/kafka/config/kraft/server.properties` on each broker. Only `node.id` and the
hostname in `advertised.listeners` change between nodes.

```properties
# --- identity / roles ---
process.roles=broker,controller
node.id=1                                   # 2 on worker2, 3 on worker3
controller.quorum.voters=1@worker1:9093,2@worker2:9093,3@worker3:9093

# --- listeners ---
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://worker1:9092   # this node's hostname
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT

# --- storage ---
log.dirs=/data/kafka-logs

# --- durability defaults ---
num.partitions=6
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
auto.create.topics.enable=false
unclean.leader.election.enable=false

# --- retention ---
log.retention.hours=336
log.segment.bytes=268435456

# --- message size (full snapshots) ---
message.max.bytes=5242880
replica.fetch.max.bytes=5242880
```

Heap: set `KAFKA_HEAP_OPTS="-Xms1g -Xmx1g"` in the systemd unit.

### Format storage (once)

The three nodes must share one cluster ID:

```bash
# on worker1
/opt/kafka/bin/kafka-storage.sh random-uuid          # copy the output
# on worker1, worker2, worker3 (as user kafka)
/opt/kafka/bin/kafka-storage.sh format -t <CLUSTER_ID> \
    -c /opt/kafka/config/kraft/server.properties
```

Record the cluster ID in the infra report. Formatting again wipes the data.

### systemd unit

`/etc/systemd/system/kafka.service`:

```ini
[Unit]
Description=Apache Kafka (KRaft)
After=network-online.target
Wants=network-online.target

[Service]
User=kafka
Environment=KAFKA_HEAP_OPTS=-Xms1g -Xmx1g
# Environment=JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64   # only if needed (see 2.2)
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now kafka
journalctl -u kafka -f
```

Start all three within a minute or so; the quorum needs 2 of 3 to elect a leader.

---

## 6. Verify the cluster

Run from the master:

```bash
BS=worker1:9092,worker2:9092,worker3:9092

# quorum healthy, one leader, 3 voters
/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server $BS describe --status

# all 3 brokers visible
/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server $BS | grep id

# create the topic explicitly (auto-create is off)
/opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --create \
  --topic opensky.states.raw --partitions 6 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=1209600000 \
  --config max.message.bytes=5242880

/opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic opensky.states.raw
# expect: each partition has 3 replicas, Isr lists 3 brokers, leaders spread over 1/2/3

# smoke test
echo hello | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $BS --topic smoke-test
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BS --topic smoke-test --from-beginning --max-messages 1
```

**Failure test** (good material for the infra report): `sudo systemctl stop kafka`
on worker2, confirm the producer keeps writing and `--describe` shows leaders moved
and ISR shrank to 2. Start it again and watch the ISR return to 3.

---

## 7. Code changes in this repo

### 7.1 Producer

- `.env` on the master: `KAFKA_BOOTSTRAP=worker1:9092,worker2:9092,worker3:9092`.
- `producer/kafka_utils.py` → `ensure_topic`: change `replication_factor=1` to `3`
  and add `"min.insync.replicas": "2"` and `"max.message.bytes": "5242880"` to the
  topic config. (Or create the topic by hand as in section 6 and leave
  `ensure_topic` as a no-op when it exists.)
- `make_producer`: add `"message.max.bytes": 5242880`. One message is a whole
  France snapshot (~1-2k aircraft × 17 fields as JSON). Check the real size first
  with `kafka-log-dirs.sh` or by logging `len(value)`. If it is well under 1 MB the
  size settings are just a safety margin.
- `acks=all` + `enable.idempotence=True` are already set, which is what we want
  with `min.insync.replicas=2`.

### 7.2 Run the producer as a service on the master

`/etc/systemd/system/opensky-producer.service`, running
`/opt/air-traffic-monitor/.venv/bin/python producer/producer.py` with
`WorkingDirectory` set to the repo checkout and `Restart=always`. Only one
producer may run at a time (shared API credits), so stop any laptop instance.

### 7.3 Replace the JSONL archiver with Spark Structured Streaming

The raw zone must be Parquet in HDFS, partitioned by date, so
`consumer/archive_consumer.py` becomes a local-dev tool only. New job, e.g.
`spark/ingest_raw.py`:

```python
from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.appName("opensky-ingest-raw").getOrCreate()

snapshot_schema = T.StructType([
    T.StructField("time", T.LongType()),
    T.StructField("states", T.ArrayType(T.ArrayType(T.StringType()))),
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "worker1:9092,worker2:9092,worker3:9092")
       .option("subscribe", "opensky.states.raw")
       .option("startingOffsets", "earliest")
       .option("failOnDataLoss", "false")
       .load())

parsed = (raw
    .select(F.from_json(F.col("value").cast("string"), snapshot_schema).alias("s"),
            "partition", "offset")
    .select("s.time", F.explode("s.states").alias("st"), "partition", "offset")
    .select(
        F.col("time").alias("snapshot_time"),
        F.col("st")[0].alias("icao24"),
        F.trim(F.col("st")[1]).alias("callsign"),
        F.col("st")[2].alias("origin_country"),
        F.col("st")[5].cast("double").alias("longitude"),
        F.col("st")[6].cast("double").alias("latitude"),
        F.col("st")[7].cast("double").alias("baro_altitude"),
        F.col("st")[8].cast("boolean").alias("on_ground"),
        F.col("st")[9].cast("double").alias("velocity"),
        F.col("st")[10].cast("double").alias("true_track"),
        F.col("st")[11].cast("double").alias("vertical_rate"),
        F.col("st")[13].cast("double").alias("geo_altitude"),
        "partition", "offset")
    .withColumn("date", F.to_date(F.from_unixtime("snapshot_time"))))

(parsed.writeStream.format("parquet")
    .option("path", "hdfs:///data/raw/opensky_states")
    .option("checkpointLocation", "hdfs:///checkpoints/opensky_ingest_raw")
    .partitionBy("date")
    .trigger(processingTime="5 minutes")
    .start()
    .awaitTermination())
```

Submit on YARN, with the Kafka connector matching the cluster's Spark/Scala
versions (check with `spark-submit --version`):

```bash
spark-submit --master yarn --deploy-mode cluster \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:<SPARK_VERSION> \
  --num-executors 2 --executor-memory 1g \
  spark/ingest_raw.py
```

If the VMs have no outbound Maven access, download the connector jar and its
dependencies (`kafka-clients`, `commons-pool2`, `spark-token-provider-kafka`)
once, put them in HDFS and pass `--jars`.

Notes:
- The HDFS checkpoint stores the Kafka offsets, so restarts resume exactly where
  they stopped. The file sink plus checkpoint gives exactly-once output in HDFS.
- The 5-minute trigger keeps the small-files count down (one snapshot every 75 s).
  A daily compaction job can rewrite each `date=` folder into a few files later.
- `snapshot_time` is UTC, so partitions are UTC days. Set
  `spark.sql.session.timeZone=UTC` to make that explicit.

---

## 8. Operations checklist

- [ ] `kafka.service` enabled on worker1-3 and survives a VM reboot
- [ ] `opensky-producer.service` enabled on master
- [ ] Spark ingest job visible in the YARN UI (`http://master:8088`) as RUNNING
- [ ] `hdfs dfs -ls /data/raw/opensky_states/` shows a `date=YYYY-MM-DD` folder filling up
- [ ] Consumer lag check:
      `kafka-consumer-groups.sh --bootstrap-server $BS --describe --all-groups`
      (Structured Streaming does not commit offsets to Kafka by default; lag is
      read from the Spark streaming UI / checkpoint instead.)
- [ ] Disk check on worker1-3: 14 days × ~1150 snapshots/day × message size × RF 3
      should be tiny, but confirm with `du -sh /data/kafka-logs`
- [ ] Optional UI: Kafka UI / AKHQ can be run as a jar on the master behind an SSH
      tunnel. Docker is allowed only as a last optional step, so do this last.

---

## 9. Optional extensions

- **5 brokers**: add worker4/5 as `process.roles=broker` only (the controller quorum
  stays on 1-3), then `kafka-reassign-partitions.sh` to spread partitions. Useful
  only if we want to show scaling in the report.
- **Second topic** for cleaned/per-aircraft events written back by Spark, feeding
  the dashboard and the TimescaleDB/InfluxDB sink.
- **Monitoring**: JMX exporter + Prometheus/Grafana for under-replicated
  partitions and request latency.
- **Security**: SASL/SCRAM on 9092. Not needed while traffic stays on the private
  subnet.

---

## 10. Order of work and owners

| # | Task                                                     | Est.   | Owner |
|---|----------------------------------------------------------|--------|-------|
| 1 | Check Java / RAM / disk / hostnames on all VMs (sect. 2) | 30 min |       |
| 2 | NSG + OS firewall rules for 9092/9093 (sect. 3)          | 20 min |       |
| 3 | Install script + config on worker1-3 (sect. 4-5)          | 1 h    |       |
| 4 | Format storage, systemd, start quorum, verify (sect. 5-6) | 45 min |       |
| 5 | Create topic, run failure test, take screenshots          | 30 min |       |
| 6 | Producer changes + systemd on master (sect. 7.1-7.2)      | 45 min |       |
| 7 | Spark ingest job on YARN to HDFS Parquet (sect. 7.3)      | 2-3 h  |       |
| 8 | Operations checklist + write-up for the infra report      | 1 h    |       |

Commit the scripts and configs (`infra/kafka/`, `spark/`, systemd units) from
different members' accounts so the git history shows everyone's work.
