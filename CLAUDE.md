# College Football Data Pipeline

Portfolio project to show data engineering and analytics engineering skills for job applications. Built by Jake, a Python/SQL developer who is newer to Spark, Airflow and Docker.

## How to work with me

- Be direct: conclusions first, short answers, no filler.
- Keep things simple. I want to understand and follow every step, so explain new concepts briefly the first time they come up.
- I need to be able to explain every part of this project in an interview. Before writing or changing code, say what you're about to do and why. After writing it, walk me through the key parts. Prefer small steps I can run and check over large files written all at once.
- Leave design decisions open to me. Suggest a default, but don't make big choices silently.
- Don't add tools or abstractions until the step that needs them. A working v1 matters more than a long tool list.
- Never put secrets in code or commits. Credentials live in `.env` (gitignored); `.env.example` holds the variable names only.

## Architecture (v1)

```
CollegeFootballData API
  -> Python ingestion (requests)
  -> local JSON (data/raw/) -> S3 raw/ (boto3)
  -> PySpark, local mode -> Parquet -> S3 processed/
  -> Snowflake RAW (internal stage + COPY INTO)
  -> dbt: STAGING -> ANALYTICS
  -> Airflow (Docker Compose) orchestrates all of the above
```

## v1 scope

- Datasets: teams, games, team game stats. 2–3 seasons.
- dbt models: `stg_teams`, `stg_games`, `stg_game_stats` -> `dim_teams`, `dim_games`, `fact_game_stats`.
- dbt tests: unique + not_null on keys, relationships from fact to dims.
- Airflow DAG: ingest -> upload_raw -> spark_process -> load_snowflake -> dbt_run -> dbt_test. Retries on API and load tasks; weekly schedule.

**v2, only after v1 works end to end:** custom metrics (success rate, yards per play by down, havoc rate), Streamlit dashboard, play-by-play data, Terraform for S3/IAM, GitHub Actions.

**Not doing:** Kafka, managed Spark, managed Airflow.

## Key decisions and constraints

- CFBD free tier has a monthly API call cap. Cache raw responses and skip calls when the file already exists locally.
- Ingestion is parameterized by season (e.g. `--season 2024`).
- S3 layout: `raw/<dataset>/season=<year>/` and `processed/<dataset>/`.
- Spark runs locally on files pulled down from S3. Don't set up Spark-to-S3 (hadoop-aws) for v1.
- Snowflake: database `CFB`, schemas `RAW`, `STAGING`, `ANALYTICS`; XS warehouse, auto-suspend 60s. Load via internal stage, no S3 storage integration in v1.
- Start the Snowflake trial only when reaching that phase (the trial clock runs).
- S3 bucket is created by hand in the console for v1 (Terraform is v2).

## Environment

- macOS (Apple Silicon), VS Code, zsh.
- Repo: https://github.com/jacobbenson21/college-football-data (MIT license)
- Local path: `~/Projects/college-football-data`
- Virtual env: `.venv` (activate with `source .venv/bin/activate`)
- `.gitignore`: GitHub Python template + `data/`
- Env vars: `CFBD_API_KEY`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_BUCKET`

## Planned layout

```
ingestion/   api_client.py, ingest_teams.py, ingest_games.py, ingest_stats.py
spark/       process_*.py
snowflake/   load scripts / SQL
dbt/         models/staging, models/marts, tests
airflow/     dags/football_pipeline.py
data/        local raw + processed files (gitignored)
```

Create folders only when they're needed.

## Phases

0. Setup: repo, venv, .gitignore, .env files, first commit
1. API ingestion: teams, games, game stats to local JSON
2. S3 raw layer (set AWS billing alert first)
3. PySpark -> Parquet -> S3 processed
4. Snowflake load + validation
5. dbt models, tests, docs
6. Airflow in Docker
7. README with architecture diagram, setup steps, screenshots, limitations

## Current status

- Phase 0 nearly done: repo cloned, `.venv` created, `requests`, `python-dotenv`, `boto3` installed, `requirements.txt` generated.
- Remaining in Phase 0: create `.env.example` and `.env`, first commit and push.
- Next: get the CFBD API key, then make the first authenticated request for teams and save the JSON to `data/raw/teams/`.
