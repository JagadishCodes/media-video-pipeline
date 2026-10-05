# Media / Video Processing & Content Pipeline — Apache Airflow 3

A certification-oriented, real-time-style Airflow 3 project covering the Level 1
Fundamentals and Level 2 DAG Authoring concepts.

## Business flow

Video upload -> ingestion -> validation -> metadata extraction -> dynamic
transcoding -> quality decision -> publish -> catalog asset -> reporting.

The project deliberately uses several DAGs so each Airflow concept has a clear
job rather than forcing everything into one giant DAG.

## Concepts covered

- Airflow UI and CLI
- DAGs, DAG runs, dependencies
- Operators
- TaskFlow API
- XCom
- Variables
- Connections
- Sensors + Triggerer / deferrable tasks
- DAG scheduling and templating
- TaskGroups
- Branching
- Assets / asset-aware scheduling
- Dynamic Task Mapping
- Dynamic DAG generation
- DAG versioning with Git
- DAG bundles (deployment concept)
- retries, logging and testing

## Project DAGs

1. `media_ingestion` — scheduled ingestion + asset producer
2. `media_processing` — asset-triggered processing DAG
3. `media_reporting` — scheduled reporting DAG
4. `media_channel_*` — dynamically generated DAGs from `config/channels.yaml`

## Airflow 3 note

Airflow 3 introduced the stable `airflow.sdk` authoring interface. The DAG
code in this project uses `airflow.sdk` for core DAG-authoring objects.

## Quick start

1. Put the project under your Airflow DAG bundle/local DAG directory.
2. Install Python dependencies from `requirements.txt`.
3. Create the Variables and Connections described in `docs/setup.md`.
4. Start Airflow 3 with a deployment that includes the Scheduler, API Server,
   Dag Processor and Triggerer.
5. Run the commands in `docs/cli-practice.md`.
6. Trigger `media_ingestion` and inspect the asset-driven downstream DAG.

## Important

This is a learning project. The "video processing" operations are simulated
with local files and Python functions so the Airflow concepts can be learned
without requiring FFmpeg, S3 or a cloud account. Later, individual tasks can
be replaced by real storage/transcoding operators.
