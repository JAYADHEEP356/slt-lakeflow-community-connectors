# Cloudera WebHDFS Lakeflow connector

This connector incrementally ingests completed clickstream CSV files from a Cloudera HDFS directory exposed through Apache Knox WebHDFS.

## Connection parameters

| Parameter | Purpose |
|---|---|
| `base_url` | Trusted HTTPS Knox URL ending in `/gateway/<topology>/webhdfs/v1` |
| `username` / `password` | Knox basic-auth credentials; store the password as a Databricks secret |
| `root_path` | HDFS directory containing completed files |
| `request_timeout_seconds` | Per-request timeout, default `30` |
| `max_retries` | Retry count for throttling and temporary server errors, default `3` |
| `ca_bundle` | Optional private CA bundle path; public trusted certificates need no override |

The only source object is `clickstream`. It expects these CSV columns:
`event_time,tower_id,msisdn,url,bytes_used,session_id`.

## Incremental behaviour

- Only visible `.csv` files are discovered; `_` and `.` prefixed files are ignored.
- Files are ordered by HDFS `modificationTime` and filename.
- The checkpoint records the last modification time, filename, and length.
- A late file is picked up when it arrives because its HDFS modification time is new, even if its filename represents an older event time.
- Each file is checked before and after reading. A changing file fails the batch and does not advance the checkpoint.
- Invalid CSV fails the batch and does not advance the checkpoint.
- Files must be immutable after publication. Replacing an already processed file is treated as a new file version and can append its rows again.

For production, write to a staging filename and atomically rename it into this directory only after the file is complete.
