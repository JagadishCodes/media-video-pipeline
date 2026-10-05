# Setup

## 1. Airflow Variables

Create this in the Airflow UI:

Admin -> Variables

| Key | Value |
|---|---|
| `media_environment` | `dev` |

Change it to `prod` later to practice environment-specific configuration.

## 2. Airflow Connections

Create a connection to practice connection management:

- Connection ID: `media_storage`
- Connection Type: HTTP
- Host: `https://example.invalid`

This learning project does not call the endpoint; the connection exists so you
can practice the Airflow Connection UI. In a real implementation, replace the
simulated storage/transcoding functions with provider hooks/operators.

## 3. Triggerer

Run the Triggerer in your Airflow deployment. The `media_processing` DAG uses
a deferrable `TimeDeltaSensor`, which demonstrates worker-slot release while
waiting.

## 4. Paths

The example uses:

- `/opt/airflow/include/raw`
- `/opt/airflow/include/processed`
- `/opt/airflow/include/reports`

If your Airflow deployment mounts this project elsewhere, update the paths in
`config/media_config.yaml` and the DAG constants.

## 5. First run

Trigger:

`media_ingestion`

That creates the simulated catalog asset. The asset-triggered
`media_processing` DAG can then be scheduled from that asset event.
