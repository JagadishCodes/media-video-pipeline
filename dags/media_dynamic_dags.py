from __future__ import annotations

import pendulum
import yaml
from pathlib import Path

from airflow.sdk import DAG, task, task_group

CONFIG = Path(__file__).resolve().parents[1] / "config" / "channels.yaml"
CHANNELS = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))["channels"]


def build_channel_dag(channel: str) -> DAG:
    """
    Dynamic DAG generation: the DAG objects are generated at DAG-parse time
    from configuration. Runtime data mapping is demonstrated elsewhere.
    """
    with DAG(
        dag_id=f"media_channel_{channel}",
        schedule="0 * * * *",
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        tags=["media", "dynamic-dag", channel],
    ) as dag:

        @task
        def discover(channel_name: str) -> list[str]:
            return [
                f"{channel_name}-video-001",
                f"{channel_name}-video-002",
            ]

        @task_group(group_id="channel_processing")
        def channel_processing(video_ids: list[str]):
            @task
            def validate(video_id: str) -> str:
                return f"validated:{video_id}"

            @task
            def publish(values: list[str]) -> None:
                print(f"Publishing {values}")

            # This mapping is runtime dynamic mapping inside a dynamically
            # generated DAG.
            validated = validate.expand(video_id=video_ids)
            publish(validated)

        videos = discover(channel)
        channel_processing(videos)

    return dag


for channel in CHANNELS:
    globals()[f"media_channel_{channel}"] = build_channel_dag(channel)
