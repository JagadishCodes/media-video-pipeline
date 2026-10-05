from __future__ import annotations

from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Variable, task

with DAG(
    dag_id="media_reporting",
    schedule="0 18 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["media", "reporting", "templating"],
) as dag:

    @task
    def get_environment() -> str:
        try:
            return Variable.get("media_environment")
        except Exception:
            return "dev"

    @task
    def build_report(environment: str) -> str:
        report_dir = Path("/opt/airflow/include/reports")
        report_dir.mkdir(parents=True, exist_ok=True)
        path = report_dir / "media_report.txt"
        path.write_text(
            f"Media pipeline report\nEnvironment: {environment}\n",
            encoding="utf-8",
        )
        return str(path)

    report_path = build_report(get_environment())

    render_context = BashOperator(
        task_id="render_context",
        bash_command=(
            "echo 'logical date={{ logical_date }}'; "
            "echo 'data interval start={{ data_interval_start }}'; "
            "echo 'report day={{ ds }}'"
        ),
    )

    report_path >> render_context
