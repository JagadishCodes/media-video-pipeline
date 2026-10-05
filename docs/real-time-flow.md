# Real-time-style flow

```text
Upload event / ingestion
        |
        v
media_ingestion
        |
        +--> VIDEO_CATALOG_ASSET update
                         |
                         v
                 media_processing
                         |
                         +--> deferrable wait
                         |
                         +--> validation TaskGroup
                         |
                         +--> metadata
                         |
                         +--> dynamic mapping
                         |       +--> 1080p
                         |       +--> 720p
                         |       +--> 480p
                         |
                         +--> collect results
                         |
                         +--> branch
                              /      \
                         publish    reprocess
```

The project simulates external video storage/transcoding locally. This keeps
the learning environment deterministic while preserving the Airflow design.
