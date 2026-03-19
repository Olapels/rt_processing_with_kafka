# Real-Time Stream Processing Pipeline

## Overview

This repository implements a local real-time data pipeline using:

- **Redpanda (Kafka API)** as the streaming broker
- **Python producer/consumer** for ingestion and validation
- **PyFlink** for event-time windowed aggregations
- **PostgreSQL** as the analytical sink

Pipeline flow:

`Producer -> green-trips topic -> Flink jobs -> PostgreSQL`

## Implemented Work

- Producer pipeline publishes trip events to Kafka topic `green-trips`
- Consumer pipeline reads from `green-trips` (`earliest`) for stream validation
- Flink job for **5-minute tumbling window** aggregation by `PULocationID`
- Flink job for **5-minute session window** aggregation by `PULocationID`
- Flink job for **1-hour tumbling window** aggregation of total `tip_amount`
- PostgreSQL sink tables and JDBC upsert paths configured for all aggregations

## Project Jobs

- `src/job/aggregation_job.py`  
	Tumbling window (`5 MINUTE`) by `PULocationID` -> `processed_events_aggregated`

- `src/job/session_window_job.py`  
	Session window (`5 MINUTE` gap) partitioned by `PULocationID` -> `processed_events_session`

- `src/job/tip_hourly_job.py`  
	Tumbling window (`1 HOUR`) for total tips -> `processed_events_tip_hourly`

## Prerequisites

- Docker + Docker Compose
- `uv`
- Python 3.12

## Setup

From `workshop/`:

```bash
uv sync
docker compose up -d redpanda postgres jobmanager taskmanager
```

Optional topic reset:

```bash
docker exec workshop-redpanda-1 rpk topic delete green-trips || true
docker exec workshop-redpanda-1 rpk topic create green-trips -p 1 -r 1
```

## PostgreSQL Sink Tables

```bash
docker exec workshop-postgres-1 psql -U postgres -d postgres -c "CREATE TABLE IF NOT EXISTS processed_events_aggregated (window_start TIMESTAMP(3) NOT NULL, PULocationID INT NOT NULL, num_trips BIGINT, PRIMARY KEY (window_start, PULocationID));"
docker exec workshop-postgres-1 psql -U postgres -d postgres -c "CREATE TABLE IF NOT EXISTS processed_events_session (window_start TIMESTAMP(3) NOT NULL, window_end TIMESTAMP(3) NOT NULL, PULocationID INT NOT NULL, num_trips BIGINT, PRIMARY KEY (window_start, window_end, PULocationID));"
docker exec workshop-postgres-1 psql -U postgres -d postgres -c "CREATE TABLE IF NOT EXISTS processed_events_tip_hourly (window_start TIMESTAMP(3) NOT NULL, total_tip DOUBLE PRECISION, PRIMARY KEY (window_start));"
```

Optional table reset:

```bash
docker exec workshop-postgres-1 psql -U postgres -d postgres -c "TRUNCATE TABLE processed_events_aggregated;"
docker exec workshop-postgres-1 psql -U postgres -d postgres -c "TRUNCATE TABLE processed_events_session;"
docker exec workshop-postgres-1 psql -U postgres -d postgres -c "TRUNCATE TABLE processed_events_tip_hourly;"
```

## Running the Pipeline

1. Produce data into `green-trips` (notebook/script producer).
2. Submit Flink jobs:

```bash
docker compose exec jobmanager ./bin/flink run -py /opt/src/job/aggregation_job.py --pyFiles /opt/src -d
docker compose exec jobmanager ./bin/flink run -py /opt/src/job/session_window_job.py --pyFiles /opt/src -d
docker compose exec jobmanager ./bin/flink run -py /opt/src/job/tip_hourly_job.py --pyFiles /opt/src -d
```

3. Check Flink job status:

```bash
docker compose exec jobmanager ./bin/flink list
```

## Querying Results

Open PostgreSQL CLI:

```bash
docker exec -it workshop-postgres-1 psql -U postgres -d postgres
```

Example queries:

```sql
SELECT * FROM processed_events_aggregated ORDER BY num_trips DESC LIMIT 10;
SELECT * FROM processed_events_session ORDER BY num_trips DESC LIMIT 10;
SELECT * FROM processed_events_tip_hourly ORDER BY total_tip DESC LIMIT 10;
```
