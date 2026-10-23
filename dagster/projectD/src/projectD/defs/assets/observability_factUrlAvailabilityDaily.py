import json
import math
import os
from datetime import datetime, time, timedelta, timezone
from time import perf_counter
from urllib.parse import urlencode
from urllib.request import urlopen

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    RetryPolicy,
    asset,
)

from ..resources import SqlServerResource


FACT_URL_AVAILABILITY_DAILY_INGESTION_NAME = "factUrlAvailabilityDaily"
PROMETHEUS_API_URL = os.getenv(
    "PROMETHEUS_API_URL",
    "http://prometheus:9090",
).rstrip("/")
PROMETHEUS_API_TIMEOUT_SECONDS = int(
    os.getenv("PROMETHEUS_API_TIMEOUT_SECONDS", "30")
)
PROMETHEUS_QUERY_STEP_SECONDS = int(
    os.getenv("PROMETHEUS_QUERY_STEP_SECONDS", "60")
)
PROMETHEUS_TIMEOUT_THRESHOLD_SECONDS = float(
    os.getenv("PROMETHEUS_TIMEOUT_THRESHOLD_SECONDS", "9.5")
)
PROMETHEUS_JOB_NAME = "blackbox_probe"
PROMETHEUS_USER_CODE = "projectD"
URL_AVAILABILITY_RETRY_POLICY = RetryPolicy(max_retries=3, delay=60)
FACT_URL_AVAILABILITY_DAILY_MERGE_SQL = """
MERGE observability.factUrlAvailabilityDaily WITH (HOLDLOCK) AS target
USING (
    SELECT
        CAST(? AS VARCHAR(850)) AS Url,
        CAST(? AS DATE) AS [Date],
        CAST(? AS DECIMAL(7, 4)) AS UptimePercentage,
        CAST(? AS DECIMAL(18, 6)) AS AvgLatencySeconds,
        CAST(? AS DECIMAL(18, 6)) AS Latency95thPercentileSeconds,
        CAST(? AS BIT) AS IsUp,
        CAST(? AS BIT) AS TimeoutsPresent
) AS source
ON
    target.[Date] = source.[Date]
    AND target.Url = source.Url
WHEN MATCHED THEN
    UPDATE SET
        UptimePercentage = source.UptimePercentage,
        AvgLatencySeconds = source.AvgLatencySeconds,
        Latency95thPercentileSeconds = source.Latency95thPercentileSeconds,
        IsUp = source.IsUp,
        TimeoutsPresent = source.TimeoutsPresent,
        ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (
        Url,
        [Date],
        UptimePercentage,
        AvgLatencySeconds,
        Latency95thPercentileSeconds,
        IsUp,
        TimeoutsPresent
    )
    VALUES (
        source.Url,
        source.[Date],
        source.UptimePercentage,
        source.AvgLatencySeconds,
        source.Latency95thPercentileSeconds,
        source.IsUp,
        source.TimeoutsPresent
    );
"""


@asset(
    required_resource_keys={"sqlserver"},
    group_name="projectD",
    key_prefix=["projectD"],
    retry_policy=URL_AVAILABILITY_RETRY_POLICY,
)
def observability_facturlavailabilitydaily(
    context: AssetExecutionContext,
) -> MaterializeResult:
    start_time = perf_counter()
    sqlserver: SqlServerResource = context.resources.sqlserver
    last_loaded_at = sqlserver.get_watermark(
        FACT_URL_AVAILABILITY_DAILY_INGESTION_NAME
    )
    now_utc = datetime.now(timezone.utc)
    from_utc = datetime.combine(
        now_utc.date() - timedelta(days=1),
        time.min,
        tzinfo=timezone.utc,
    )
    sqlserver.mark_run_started(
        FACT_URL_AVAILABILITY_DAILY_INGESTION_NAME
    )

    try:
        targets_request_url = (
            PROMETHEUS_API_URL
            + "/api/v1/targets?"
            + urlencode({"state": "active"})
        )
        with urlopen(
            targets_request_url,
            timeout=PROMETHEUS_API_TIMEOUT_SECONDS,
        ) as response:
            targets_payload = json.load(response)

        if (
            not isinstance(targets_payload, dict)
            or targets_payload.get("status") != "success"
        ):
            raise RuntimeError("Prometheus targets query failed")
        targets_data = targets_payload.get("data")
        if (
            not isinstance(targets_data, dict)
            or not isinstance(targets_data.get("activeTargets"), list)
        ):
            raise RuntimeError("Prometheus returned invalid targets")

        configured_urls = set()
        for target in targets_data["activeTargets"]:
            if not isinstance(target, dict):
                raise RuntimeError("Prometheus returned an invalid target")
            labels = target.get("labels")
            if not isinstance(labels, dict):
                raise RuntimeError(
                    "Prometheus returned a target without labels"
                )
            if (
                labels.get("job") != PROMETHEUS_JOB_NAME
                or labels.get("user_code") != PROMETHEUS_USER_CODE
            ):
                continue
            url = labels.get("instance")
            if not isinstance(url, str) or not url:
                raise RuntimeError(
                    "Prometheus returned a target without instance"
                )
            configured_urls.add(url)

        metric_samples = {}
        for metric_name in (
            "probe_success",
            "probe_duration_seconds",
        ):
            query = (
                metric_name
                + '{job="'
                + PROMETHEUS_JOB_NAME
                + '",user_code="'
                + PROMETHEUS_USER_CODE
                + '"}'
            )
            request_url = (
                PROMETHEUS_API_URL
                + "/api/v1/query_range?"
                + urlencode(
                    {
                        "query": query,
                        "start": from_utc.timestamp(),
                        "end": now_utc.timestamp(),
                        "step": PROMETHEUS_QUERY_STEP_SECONDS,
                    }
                )
            )
            with urlopen(
                request_url,
                timeout=PROMETHEUS_API_TIMEOUT_SECONDS,
            ) as response:
                payload = json.load(response)

            if (
                not isinstance(payload, dict)
                or payload.get("status") != "success"
            ):
                raise RuntimeError(
                    "Prometheus query failed for " + metric_name
                )
            data = payload.get("data")
            if (
                not isinstance(data, dict)
                or data.get("resultType") != "matrix"
                or not isinstance(data.get("result"), list)
            ):
                raise RuntimeError(
                    "Prometheus returned an invalid matrix for "
                    + metric_name
                )

            metric_samples[metric_name] = {}
            for series in data["result"]:
                metric = series.get("metric")
                values = series.get("values")
                if (
                    not isinstance(metric, dict)
                    or not isinstance(values, list)
                ):
                    raise RuntimeError(
                        "Prometheus returned an invalid series for "
                        + metric_name
                    )
                url = metric.get("instance")
                if not isinstance(url, str) or not url:
                    raise RuntimeError(
                        "Prometheus returned a series without instance"
                    )
                if url not in configured_urls:
                    continue
                for sample in values:
                    if not isinstance(sample, list) or len(sample) != 2:
                        raise RuntimeError(
                            "Prometheus returned an invalid sample for "
                            + metric_name
                        )
                    sample_timestamp = float(sample[0])
                    sample_value = float(sample[1])
                    if not (
                        math.isfinite(sample_timestamp)
                        and math.isfinite(sample_value)
                    ):
                        raise RuntimeError(
                            "Prometheus returned a non-finite sample for "
                            + metric_name
                        )
                    sample_date = datetime.fromtimestamp(
                        sample_timestamp,
                        tz=timezone.utc,
                    ).date()
                    metric_samples[metric_name].setdefault(
                        url,
                        {},
                    ).setdefault(
                        sample_date,
                        [],
                    ).append(
                        (sample_timestamp, sample_value)
                    )

        success_samples = metric_samples["probe_success"]
        duration_samples = metric_samples["probe_duration_seconds"]
        missing_success_urls = configured_urls.difference(
            success_samples
        )
        if missing_success_urls:
            raise RuntimeError(
                "Prometheus returned no probe_success samples for "
                + ", ".join(sorted(missing_success_urls))
            )

        rows = []
        for url in sorted(success_samples):
            for sample_date in sorted(success_samples[url]):
                daily_success = success_samples[url][sample_date]
                daily_duration = duration_samples.get(
                    url,
                    {},
                ).get(sample_date)
                if not daily_duration:
                    raise RuntimeError(
                        "Prometheus returned no duration samples for "
                        + url
                    )

                success_count = sum(
                    1
                    for _, sample_value in daily_success
                    if sample_value >= 1
                )
                uptime_percentage = (
                    success_count / len(daily_success)
                ) * 100
                duration_values = [
                    sample_value
                    for _, sample_value in daily_duration
                ]
                avg_latency_seconds = (
                    sum(duration_values) / len(duration_values)
                )
                sorted_durations = sorted(duration_values)
                percentile_index = max(
                    0,
                    math.ceil(len(sorted_durations) * 0.95) - 1,
                )
                latency_95th_percentile_seconds = (
                    sorted_durations[percentile_index]
                )
                is_up = max(
                    daily_success,
                    key=lambda sample: sample[0],
                )[1] >= 1
                duration_by_timestamp = dict(daily_duration)
                timeouts_present = any(
                    sample_value < 1
                    and duration_by_timestamp.get(
                        sample_timestamp,
                        0,
                    ) >= PROMETHEUS_TIMEOUT_THRESHOLD_SECONDS
                    for sample_timestamp, sample_value in daily_success
                )
                rows.append(
                    (
                        url,
                        sample_date,
                        round(uptime_percentage, 4),
                        round(avg_latency_seconds, 6),
                        round(
                            latency_95th_percentile_seconds,
                            6,
                        ),
                        is_up,
                        timeouts_present,
                    )
                )

        for row in rows:
            upsert_result = sqlserver.execute_query(
                FACT_URL_AVAILABILITY_DAILY_MERGE_SQL,
                *row,
            )
            if isinstance(upsert_result, Exception):
                raise upsert_result
            if upsert_result is None:
                raise RuntimeError(
                    "SQL Server did not confirm the URL availability merge"
                )
    except Exception as ex:
        sqlserver.mark_run_completed(
            ingestion_name=FACT_URL_AVAILABILITY_DAILY_INGESTION_NAME,
            new_last_loaded_at=last_loaded_at,
            succeeded=False,
            error_message=str(ex),
        )
        raise

    sqlserver.mark_run_completed(
        ingestion_name=FACT_URL_AVAILABILITY_DAILY_INGESTION_NAME,
        new_last_loaded_at=now_utc.replace(tzinfo=None),
        succeeded=True,
        error_message=None,
    )

    return MaterializeResult(
        metadata={
            "row_count": len(rows),
            "url_count": len(success_samples),
            "elapsed_seconds": round(perf_counter() - start_time, 3),
            "from_utc": from_utc.isoformat(),
            "to_utc": now_utc.isoformat(),
        },
    )
