from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.sdk import Asset, DAG, task, task_group


VIDEO_CATALOG_ASSET = Asset(
    "file:///opt/airflow/include/processed/video_catalog.json"
)


with DAG(
    dag_id="media_processing",
    schedule=[VIDEO_CATALOG_ASSET],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["media", "level2", "assets", "mapping"],
) as dag:

    # ---------------------------------------------------------
    # 1. Read video catalog
    # ---------------------------------------------------------
    @task
    def read_catalog() -> list[dict]:
        path = Path(
            "/opt/airflow/include/processed/video_catalog.json"
        )

        if not path.exists():
            raise FileNotFoundError(path)

        with path.open(encoding="utf-8") as handle:
            return json.load(handle)["videos"]

    # ---------------------------------------------------------
    # 2. Get available resolutions
    # ---------------------------------------------------------
    @task
    def extract_resolutions() -> list[str]:
        return ["1080p", "720p", "480p"]

    # ---------------------------------------------------------
    # 3. Wait for processing window
    #
    # IMPORTANT:
    # TimeDeltaSensor uses `delta`, not `time_to_wait`
    # ---------------------------------------------------------
    wait_for_processing_window = TimeDeltaSensor(
        task_id="wait_for_processing_window",
        delta=timedelta(seconds=5),
        deferrable=True,
    )

    # ---------------------------------------------------------
    # 4. Validate videos + extract metadata
    # ---------------------------------------------------------
    @task_group(group_id="validate_and_metadata")
    def validate_and_metadata(videos: list[dict]):

        @task
        def validate(items: list[dict]) -> list[dict]:
            for video in items:
                if not video.get("video_id") or not video.get("filename"):
                    raise ValueError(f"Invalid video: {video}")

            return items

        @task
        def extract_metadata(items: list[dict]) -> list[dict]:
            enriched = []

            for video in items:
                enriched.append(
                    {
                        **video,
                        "codec": "h264",
                        "fps": 30,
                    }
                )

            return enriched

        validated = validate(videos)

        enriched = extract_metadata(validated)

        return enriched

    # ---------------------------------------------------------
    # 5. Build transcode jobs
    # ---------------------------------------------------------
    @task
    def build_transcode_jobs(
        videos: list[dict],
        resolutions: list[str],
    ) -> list[dict]:

        jobs = []

        for video in videos:
            for resolution in resolutions:
                jobs.append(
                    {
                        "video_id": video["video_id"],
                        "filename": video["filename"],
                        "resolution": resolution,
                    }
                )

        return jobs

    # ---------------------------------------------------------
    # 6. Read catalog + resolutions
    # ---------------------------------------------------------
    videos = read_catalog()

    resolutions = extract_resolutions()

    # ---------------------------------------------------------
    # 7. Validate + enrich metadata
    # ---------------------------------------------------------
    enriched_videos = validate_and_metadata(videos)

    # ---------------------------------------------------------
    # 8. Build transcode jobs
    # ---------------------------------------------------------
    jobs = build_transcode_jobs(
        enriched_videos,
        resolutions,
    )

    # ---------------------------------------------------------
    # 9. Transcode
    #
    # Dynamic Task Mapping
    # ---------------------------------------------------------
    @task
    def transcode(job: dict) -> dict:

        output = {
            **job,
            "output": (
                f"{job['video_id']}_"
                f"{job['resolution']}.mp4"
            ),
            "status": "completed",
        }

        print(f"Simulated transcode: {output}")

        return output

    mapped_transcodes = transcode.expand(
        job=jobs
    )

    # ---------------------------------------------------------
    # 10. Collect transcode results
    #
    # Reduce operation
    # ---------------------------------------------------------
    @task
    def collect_transcodes(
        results: list[dict],
    ) -> dict:

        completed = [
            result
            for result in results
            if result["status"] == "completed"
        ]

        return {
            "count": len(completed),
            "results": completed,
        }

    collected = collect_transcodes(
        mapped_transcodes
    )

    # ---------------------------------------------------------
    # 11. Publish TaskGroup
    # ---------------------------------------------------------
    @task_group(group_id="publish_group")
    def publish_group(summary: dict):

        @task
        def publish(summary: dict) -> str:
            print(
                f"Publishing "
                f"{summary['count']} transcoded outputs"
            )

            return "published"

        return publish(summary)

    # ---------------------------------------------------------
    # 12. Reprocess TaskGroup
    # ---------------------------------------------------------
    @task_group(group_id="reprocess_group")
    def reprocess_group(summary: dict):

        @task
        def reprocess(summary: dict) -> str:
            print(
                f"Reprocessing required: "
                f"{summary['count']} outputs"
            )

            return "reprocess_requested"

        return reprocess(summary)

    # ---------------------------------------------------------
    # 13. Quality Gate / Branching
    # ---------------------------------------------------------
    @task.branch
    def quality_gate(summary: dict) -> str:

        if summary["count"] >= 3:
            return "publish_group.publish"

        return "reprocess_group.reprocess"

    branch = quality_gate(collected)

    # ---------------------------------------------------------
    # 14. Create TaskGroups
    # ---------------------------------------------------------
    publish = publish_group(collected)

    reprocess = reprocess_group(collected)

    # ---------------------------------------------------------
    # 15. Dependencies
    # ---------------------------------------------------------

    # Sensor → catalog processing
    wait_for_processing_window >> enriched_videos

    # Quality gate → either publish OR reprocess
    branch >> [publish, reprocess]