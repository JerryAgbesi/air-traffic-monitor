# OpenSky Airspace Monitor

Real-time air traffic over France: OpenSky Network → Kafka → dashboard + a small ML forecast.

## Quick start

1. **Install** Docker Desktop and Python 3.10+.
2. **Credentials:** create a free account on https://opensky-network.org, then go to Account → create an API client.
   ```bash
   cp .env.example .env    # then paste your client_id and client_secret
   ```
3. **Start Kafka:**
   ```bash
   docker compose up -d
   ```
   Kafka UI: http://localhost:8080
4. **Python environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate      # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```
5. **Run the producer** (terminal 1):
   ```bash
   python producer/producer.py
   ```
   Expected log every 75 s: `Snapshot ...: sent 1234 aircraft`
6. **Run the archive consumer** (terminal 2):
   ```bash
   python consumer/archive_consumer.py
   ```
   Raw events accumulate in `data/raw/date=YYYY-MM-DD/states.jsonl`.

## Important: keep it running 24/7

The ML part needs weeks of continuous history. Run the producer and the archiver on a machine
that stays on (a teammate's desktop, a Raspberry Pi, or a small cloud VM), not a laptop that sleeps.
Only **one** producer should run at a time, otherwise you burn the shared API credits twice.

## Structure

```
producer/   OpenSky polling -> Kafka (topic opensky.states.raw, key = icao24)
consumer/   Kafka -> daily JSON-lines archive
ml/         features, baseline, model (to do)
dashboard/  KPIs + live map (to do)
docs/       checkpoint reports
```

## Data source

OpenSky Network, free for research and non-commercial use. Please cite:
Schäfer, M. et al. "Bringing up OpenSky: A large-scale ADS-B sensor network for research", IPSN 2014.
