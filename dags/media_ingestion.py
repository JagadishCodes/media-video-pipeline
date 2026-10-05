from __future__ import annotations

from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, BaseHook, DAG, Variable, task

from common import get_project_config, write_json, ensure_dirs


VIDEO_CATALOG_ASSET = Asset(
    "file:///opt/airflow/include/processed/video_catalog.json"
)


with DAG(
    dag_id="media_ingestion",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["media", "level1", "asset-producer"],
) as dag:

    @task
    def check_storage_connection() -> str:
        conn = BaseHook.get_connection("media_storage")
        return conn.host or "configured"

    @task
    def read_environment() -> str:
        return Variable.get("media_environment", default_var="dev")

    @task(retries=2)
    def discover_uploads() -> list[dict]:
        """
        Simulates an upload API by creating/reading a small set of video
        metadata. The returned list becomes an XComArg for the next task.
        """
        config = get_project_config()
        ensure_dirs(config)

        incoming = Path(config["incoming_dir"])
        sample = incoming / "launch_demo.mp4"

        if not sample.exists():
            sample.write_text("SIMULATED VIDEO CONTENT", encoding="utf-8")

        return [
            {
                "video_id": "vid-1001",
                "filename": sample.name,
                "source": "upload",
                "duration_seconds": 95,
            }
        ]

    @task(outlets=[VIDEO_CATALOG_ASSET])
    def register_catalog(videos: list[dict]) -> str:
        """
        Writes a small catalog and declares it as an Airflow Asset.
        The asset event is the trigger for the processing DAG.
        """
        config = get_project_config()
        output = Path(config["processed_dir"]) / "video_catalog.json"

        payload = {
            "videos": videos,
            "environment": "dev",
        }
        write_json(str(output), payload)
        return str(output)

    storage = check_storage_connection()
    environment = read_environment()
    uploads = discover_uploads()
    uploads.set_upstream([storage, environment])
    publish_catalog = register_catalog(uploads)

    # Use a classic operator too, so the project contains both TaskFlow and
    # classic operator authoring styles.
    show_catalog = BashOperator(
        task_id="show_catalog_path",
        bash_command="echo 'Catalog generated for {{ ds }}'",
    )

    publish_catalog >> show_catalog
