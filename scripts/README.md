# Scripts

| Script | Operation |
| --- | --- |
| `register_schemas.py` | Register raw and enriched Avro value schemas in Schema Registry. |
| `create_topics.py` | Create `raw_earthquakes` and `enriched_earthquakes` if missing. |
| `reset_raw_topic.py` | Delete and recreate the disposable local raw topic during the JSON-to-Avro cutover. |

From the repository root, after starting the Docker Compose stack:

```powershell
.\.venv\Scripts\python.exe scripts/register_schemas.py
.\.venv\Scripts\python.exe scripts/create_topics.py
```

Run `reset_raw_topic.py` only when discarding retained Phase 2 JSON records from the local raw topic. Direct Python commands read process environment variables or use local defaults; they do not load `.env` automatically.
