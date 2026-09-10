# MCP-style tools

## Why "MCP-style" and not the wire protocol

`backend/app/tools/base.py` and `backend/app/tools/registry.py` implement
every tool as a typed Python class: a Pydantic input model, a Pydantic
output model, a declared `risk_level`, and audited execution
(`registry.invoke_tool` writes an `AuditLog` row for every call, success or
failure, including the full validated input and output). This mirrors the
semantics MCP defines for tools (typed schema, structured result,
discoverable catalog -- see `GET /tools`) without wiring the actual
stdio/SSE JSON-RPC transport and client/server handshake.

That transport is real integration surface (a process boundary, a
serialization format, connection lifecycle management) that would not
change the actual safety story of this project: the properties that matter
-- an LLM-authored plan cannot invoke a tool with malformed input, a
CRITICAL tool cannot be invoked no matter what, and every invocation is
audited -- are enforced identically whether the call crosses a real MCP
transport or a Python function call. `app/tools/registry.py` is the single
place a real MCP server wrapper would be added later (each `BaseTool`
subclass's `input_model`/`output_model`/`_execute` maps directly onto an MCP
tool's `inputSchema`/`outputSchema`/handler).

## Tool catalog (`GET /tools`)

| Tool | Risk | Notes |
|---|---|---|
| get_kafka_consumer_lag | LOW | real kafka-python admin call |
| get_topic_metadata | LOW | |
| get_recent_pipeline_errors | LOW | reads pipeline.dlq |
| quarantine_message | LOW | never deletes data; records intent |
| get_flink_job_status | LOW | real Flink REST call |
| restart_flink_job | MEDIUM | real Flink REST call |
| change_flink_parallelism | MEDIUM | writes pending config; needs redeploy |
| get_airflow_dag_status | LOW | real Airflow REST call |
| get_airflow_task_logs | LOW | |
| rerun_airflow_task | MEDIUM | real Airflow REST call |
| get_database_schema | LOW | SQLAlchemy inspector |
| get_schema_versions | LOW | reads schema_versions table |
| compare_schema_versions | LOW | reuses the deterministic schema-diff engine |
| request_human_approval | LOW | catalog completeness; real approval flow is the API, not this tool |
| apply_safe_config_patch | MEDIUM | whitelisted keys only, captures previous state for rollback |
| rollback_change | MEDIUM | restores state captured by apply_safe_config_patch |
| validate_pipeline_health | LOW | used exclusively by the independent validation step |
| drop_database_table | **CRITICAL** | **hard-blocked in code, no implementation exists** |
| truncate_topic | **CRITICAL** | **hard-blocked in code** |
| delete_dag | **CRITICAL** | **hard-blocked in code** |
| run_arbitrary_sql | **CRITICAL** | **hard-blocked in code** |
| run_shell_command | **CRITICAL** | **hard-blocked in code** |

## Defense in depth on CRITICAL tools

`CRITICAL_HARD_BLOCK` in `backend/app/tools/registry.py` is a Python
`set` literal, not a config value. `get_tool()` raises `ToolPermissionError`
for any name in that set before even checking the registry, and none of
those five tools have an implementing class at all -- there is no code path
that could execute them even if the YAML policy file
(`backend/app/policies/autonomy_policy.yaml`) were edited to reclassify
them. `backend/app/policies/engine.py::assess_risk` separately forces
`autonomy_decision = BLOCKED` for any plan referencing one of these names,
so a plan can never reach the approval-required path for a critical action
either -- it is refused before a human is even asked.
