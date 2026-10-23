from dagster import Output, RetryPolicy, job, op

INDEX_MAINTENANCE_RETRY_POLICY = RetryPolicy(max_retries=1, delay=300)
INDEX_MAINTENANCE_QUERY = (
    "EXEC ctl.usp_IndexMaintenance_Run "
    "@SchemaName = ?, "
    "@MinPageCount = ?, "
    "@ReorganizeThreshold = ?, "
    "@RebuildThreshold = ?, "
    "@MaxDop = ?, "
    "@UseOnlineRebuild = ?"
)

INDEX_MAINTENANCE_DEFAULTS = {
    "schema_name": None,
    "min_page_count": 1000,
    "reorganize_threshold": 30.0,
    "rebuild_threshold": 80.0,
    "max_dop": 1,
    "use_online_rebuild": 0,
}


@op(
    required_resource_keys={"sqlserver"},
    retry_policy=INDEX_MAINTENANCE_RETRY_POLICY,
)
def run_index_maintenance_op(context):
    sql = context.resources.sqlserver
    result = sql.execute_query(
        INDEX_MAINTENANCE_QUERY,
        INDEX_MAINTENANCE_DEFAULTS["schema_name"],
        INDEX_MAINTENANCE_DEFAULTS["min_page_count"],
        INDEX_MAINTENANCE_DEFAULTS["reorganize_threshold"],
        INDEX_MAINTENANCE_DEFAULTS["rebuild_threshold"],
        INDEX_MAINTENANCE_DEFAULTS["max_dop"],
        INDEX_MAINTENANCE_DEFAULTS["use_online_rebuild"],
    )
    if isinstance(result, Exception):
        raise RuntimeError("Index maintenance procedure failed.") from result

    context.log.info("Completed index maintenance for tables and indexed views in all schemas.")
    return Output(value=result, metadata=INDEX_MAINTENANCE_DEFAULTS)


@job(name="081_maintenance_index", tags={"job_type": "mantenimiento", "run_lane": "operational", "user_code_run_lane": "projectD_operational", "user_code": "projectD"})
def index_maintenance_job():
    run_index_maintenance_op()
