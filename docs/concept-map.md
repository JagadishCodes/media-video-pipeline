# Certification concept map

## Level 1

- UI -> inspect DAGs, runs, task logs
- CLI -> `docs/cli-practice.md`
- DAG -> all files under `dags/`
- Dependencies -> TaskFlow calls and `>>`
- DAG runs -> scheduled/manual runs
- Architecture -> Scheduler, Dag Processor, API Server, Worker/Executor, Triggerer
- Operators -> BashOperator, TimeDeltaSensor
- XCom -> TaskFlow return values
- Connections -> `media_storage`
- Variables -> `media_environment`
- Sensors -> `TimeDeltaSensor`

## Level 2

- Assets -> `VIDEO_CATALOG_ASSET`
- Asset-aware scheduling -> `media_processing`
- Dynamic task mapping -> `transcode.expand(...)`
- Dynamic DAGs -> `media_dynamic_dags.py`
- DAG scheduling -> cron schedules + asset schedule
- TaskFlow API -> `@task`
- TaskGroups -> validation/publish/reprocess/channel groups
- Templating -> `media_reporting.py`
- Branching -> `@task.branch`
- DAG versioning -> Git commits/tags
- DAG bundles -> deployment/version-selection concept documented in
  `docs/versioning-and-bundles.md`
