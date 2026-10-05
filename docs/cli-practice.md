# Airflow 3 CLI practice

Run these from the Airflow environment:

```bash
airflow dags list
airflow tasks list media_ingestion
airflow tasks list media_processing
airflow dags trigger media_ingestion
airflow dags trigger media_reporting
airflow tasks render media_reporting render_context 2026-09-07
```

For a mapped task, inspect its mapped index in the UI and use the Airflow 3
task-render command with `--map-index` when appropriate.

The current Airflow CLI also supports DAG bundle selection for commands where a
bundle is relevant.
