# College Football Data Pipeline

An end-to-end data engineering pipeline that ingests college football data from the
[CollegeFootballData API](https://collegefootballdata.com/), lands it in an S3 data
lake, transforms it with PySpark, loads it into Snowflake, and models it into an
analytics-ready star schema with dbt — orchestrated by Airflow.

> **Status: work in progress.** Ingestion, the S3 data lake, and PySpark processing
> are complete; Snowflake loading is underway; dbt modeling and Airflow orchestration
> are next. See [Project status](#project-status).

## Architecture

```
CollegeFootballData API
      │  Python ingestion (requests)
      ▼
  data/raw/ (JSON)  ──────►  S3  raw/            (boto3)
      │
      │  PySpark (local mode) — flatten, pivot, parse, type
      ▼
  data/processed/ (Parquet)  ──────►  S3  processed/
      │
      │  Snowflake external stage + COPY INTO  (S3 storage integration)
      ▼
  Snowflake RAW  ──►  dbt: STAGING ──► ANALYTICS (star schema)
      │
      ▼
  Airflow (Docker Compose) orchestrates the full DAG
```

## Tech stack

| Layer          | Tools                                   |
| -------------- | --------------------------------------- |
| Ingestion      | Python, `requests`                      |
| Data lake      | AWS S3, `boto3`, IAM (least privilege)  |
| Processing     | PySpark (local), Parquet                |
| Warehouse      | Snowflake (S3 storage integration)      |
| Modeling       | dbt (staging → marts, tests, docs)      |
| Orchestration  | Airflow, Docker Compose                 |

## Data model (star schema)

| Table             | Grain                 | Source         |
| ----------------- | --------------------- | -------------- |
| `dim_teams`       | one row per team      | `/teams/fbs`   |
| `dim_games`       | one row per game      | `/games`       |
| `fact_game_stats` | one row per game-team | `/games/teams` |

`fact_game_stats` joins to `dim_games` on `game_id` and to `dim_teams` on `team_id`.

## Datasets & scope

- FBS teams, games, and team game stats for **2022–2025** (completed-season backfill).
- The current season is planned as a **live, incrementally-updating source** so the
  scheduled pipeline reflects new results each week.
- Raw data is partitioned by `season` (and by `week` for stats).

## Project status

- [x] **Ingestion** — API client + per-dataset scripts, response caching, 4-season backfill
- [x] **S3 raw layer** — idempotent uploads, least-privilege IAM
- [x] **PySpark processing** — flatten nested JSON, pivot long→wide stats, parse/type → Parquet
- [x] **Processed layer in S3**
- [ ] **Snowflake load** — storage integration + external stage + `COPY INTO` *(in progress)*
- [ ] **dbt** — staging + marts, `unique`/`not_null`/`relationships` tests, docs
- [ ] **Airflow** — orchestrate the DAG in Docker with retries + weekly schedule
- [ ] **Live current season**

## Repository structure

```
ingestion/   api_client.py, ingest_teams.py, ingest_games.py, ingest_stats.py, upload_to_s3.py
spark/       process_teams.py, process_games.py, process_stats.py
docs/        data-notes.md, samples/   (data exploration notes + sample API responses)
data/        local raw + processed files (gitignored)
```

## Getting started

**Prerequisites:** Python 3.12, Java 11 (for PySpark), an AWS account + S3 bucket, a
Snowflake account, and a CollegeFootballData API key.

```bash
# 1. Install dependencies
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure credentials
cp .env.example .env   # then fill in the values

# 3. Ingest raw data (per season)
python ingestion/ingest_teams.py --season 2024
python ingestion/ingest_games.py --season 2024
python ingestion/ingest_stats.py --season 2024

# 4. Upload raw to S3
python ingestion/upload_to_s3.py

# 5. Transform to Parquet with Spark
python spark/process_teams.py
python spark/process_games.py
python spark/process_stats.py

# 6. Upload processed Parquet to S3
python ingestion/upload_to_s3.py
```

Ingestion caches responses locally and skips the API call if a file already exists,
to stay within the API's monthly request limit.

## Data quality notes

Modeling decisions and data-quality findings (e.g. neutral-site venues vs. home-team
stadiums, filtering unplayed/cancelled games, and reconciling the games and stats
endpoints) are documented in [`docs/data-notes.md`](docs/data-notes.md).

## License

MIT
