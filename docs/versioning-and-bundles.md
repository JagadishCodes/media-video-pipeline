# DAG versioning and DAG bundles

## Versioning

Use Git for the whole project:

```bash
git init
git add .
git commit -m "feat: initial media pipeline"
git tag v1.0.0
```

Later:

```bash
git checkout -b feature/dynamic-mapping
git commit -am "feat: add dynamic transcoding"
git tag v1.1.0
```

Airflow 3 DAG bundles can make a run use a specific bundle version. A Git-backed
bundle is the most useful production-style exercise for this project.

## Git DAG Bundle example

See `config/airflow-bundle-example.cfg`.

Create an Airflow Git connection named `git_dags_conn` and keep credentials in
the Connection/secrets backend rather than putting tokens in the repository.

Example deployment configuration:

```ini
[dag_processor]
dag_bundle_config_list = [
  {
    "name": "media-git",
    "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
    "kwargs": {
      "git_conn_id": "git_dags_conn",
      "subdir": "dags",
      "tracking_ref": "main"
    }
  }
]
```

For a reproducible release exercise, change `tracking_ref` from `main` to a
specific Git tag or full commit SHA.

## Why this matters

The Local DAG bundle is useful for development but does not provide bundle
versioning. A Git DAG bundle supports versioning so a DAG run can keep using
the same DAG code even when the repository changes.

Do not put bundle/deployment configuration inside DAG Python files.
