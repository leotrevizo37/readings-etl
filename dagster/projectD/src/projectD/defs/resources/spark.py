from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

import json
import logging
import os
import socket
import time
from urllib.parse import urlparse
from urllib.request import urlopen
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, max as spark_max, upper
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType
from pyspark import SparkConf, StorageLevel
from dagster import resource
from zoneinfo import ZoneInfo


OPERATIONS_TIMEZONE_NAME = "America/Ciudad_Juarez"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.5.0"
MSSQL_JDBC_PACKAGE = "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11"
DEFAULT_SPARK_MASTER_STATUS_URL = "http://spark-master:8080/json/"
DEFAULT_SPARK_RESOURCE_WAIT_SECONDS = 300
DEFAULT_SPARK_RESOURCE_POLL_SECONDS = 10
DEFAULT_SPARK_WORKER_NETWORK_TIMEOUT_SECONDS = 3
DEFAULT_SPARK_DRIVER_BIND_ADDRESS = "0.0.0.0"
DEFAULT_SPARK_RESOURCE_PROFILES = {
    "default": {"min_cores": 1, "min_memory_mb": 1024, "cores_max": 1},
    "maintenance": {"min_cores": 1, "min_memory_mb": 1024, "cores_max": 1},
    "dimensiones": {"min_cores": 1, "min_memory_mb": 1024, "cores_max": 1},
    "sensor_expected_values": {"min_cores": 2, "min_memory_mb": 2048, "cores_max": 2},
    "lecturas": {"min_cores": 2, "min_memory_mb": 2048, "cores_max": 2},
    "observability": {"min_cores": 1, "min_memory_mb": 2048, "cores_max": 1},
    "operaciones": {"min_cores": 2, "min_memory_mb": 2048, "cores_max": 2},
    "backfill": {"min_cores": 2, "min_memory_mb": 2048, "cores_max": 2},
}
SPARK_RETRY_POLICY_ENV = {
    "spark.task.maxFailures": ("SPARK_TASK_MAX_FAILURES", "8"),
    "spark.stage.maxConsecutiveAttempts": ("SPARK_STAGE_MAX_CONSECUTIVE_ATTEMPTS", "8"),
    "spark.shuffle.io.maxRetries": ("SPARK_SHUFFLE_IO_MAX_RETRIES", "10"),
    "spark.shuffle.io.retryWait": ("SPARK_SHUFFLE_IO_RETRY_WAIT", "10s"),
    "spark.network.timeout": ("SPARK_NETWORK_TIMEOUT", "800s"),
    "spark.executor.heartbeatInterval": ("SPARK_EXECUTOR_HEARTBEAT_INTERVAL", "60s"),
}
SPARK_S3A_TUNING_ENV = {
    "fs.s3a.connection.maximum": ("SPARK_S3A_CONNECTION_MAXIMUM", "64"),
    "fs.s3a.threads.max": ("SPARK_S3A_THREADS_MAX", "32"),
    "fs.s3a.max.total.tasks": ("SPARK_S3A_MAX_TOTAL_TASKS", "64"),
    "fs.s3a.multipart.size": ("SPARK_S3A_MULTIPART_SIZE", "128M"),
    "fs.s3a.multipart.threshold": ("SPARK_S3A_MULTIPART_THRESHOLD", "128M"),
    "fs.s3a.fast.upload": ("SPARK_S3A_FAST_UPLOAD", "true"),
    "fs.s3a.fast.upload.buffer": ("SPARK_S3A_FAST_UPLOAD_BUFFER", "disk"),
    "fs.s3a.fast.upload.active.blocks": ("SPARK_S3A_FAST_UPLOAD_ACTIVE_BLOCKS", "4"),
    "fs.s3a.attempts.maximum": ("SPARK_S3A_ATTEMPTS_MAXIMUM", "10"),
    "fs.s3a.connection.request.timeout": ("SPARK_S3A_CONNECTION_REQUEST_TIMEOUT", "30m"),
    "fs.s3a.connection.timeout": ("SPARK_S3A_CONNECTION_TIMEOUT", "30s"),
    "fs.s3a.socket.timeout": ("SPARK_S3A_SOCKET_TIMEOUT", "30m"),
}

def _get_operations_timezone():
    try:
        return ZoneInfo(OPERATIONS_TIMEZONE_NAME)
    except Exception:
        return timezone(timedelta(hours=-6), name=OPERATIONS_TIMEZONE_NAME)


def _coerce_utc_naive(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(microsecond=0)
    return dt.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0)


def _local_to_utc_naive(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_get_operations_timezone())
    return _coerce_utc_naive(dt)


def _daterange_days(start: datetime, end: datetime) -> List[datetime]:
    current = start.date()
    last = end.date()
    days = []
    while current <= last:
        days.append(datetime(current.year, current.month, current.day))
        current = current + timedelta(days=1)
    return days


def _env_int(name: str, default: int, env: Optional[Dict[str, str]] = None) -> int:
    raw = (os.environ if env is None else env).get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def _build_spark_retry_conf(env: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    current_env = os.environ if env is None else env
    return {
        spark_key: current_env.get(env_key) or default
        for spark_key, (env_key, default) in SPARK_RETRY_POLICY_ENV.items()
    }


def _build_s3a_tuning_conf(env: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    current_env = os.environ if env is None else env
    return {
        spark_key: current_env.get(env_key) or default
        for spark_key, (env_key, default) in SPARK_S3A_TUNING_ENV.items()
        if (current_env.get(env_key) or default)
    }


def _build_spark_master_status_url(
    master_url: str,
    env: Optional[Dict[str, str]] = None,
) -> str:
    current_env = os.environ if env is None else env
    explicit_url = (current_env.get("SPARK_MASTER_STATUS_URL") or "").strip()
    parsed_master_url = urlparse(master_url)
    master_host = parsed_master_url.hostname
    if explicit_url and (
        explicit_url != DEFAULT_SPARK_MASTER_STATUS_URL
        or master_host == "spark-master"
        or not master_host
    ):
        return explicit_url

    if not master_host:
        return DEFAULT_SPARK_MASTER_STATUS_URL

    status_port = (current_env.get("SPARK_MASTER_STATUS_PORT") or "").strip()
    if not status_port:
        if master_host == "spark-master":
            status_port = current_env.get("SPARK_MASTER_WEBUI_PORT") or "8080"
        else:
            status_port = (
                current_env.get("SPARK_MASTER_UI_PORT")
                or current_env.get("SPARK_MASTER_WEBUI_PORT")
                or "8080"
            )

    return f"http://{master_host}:{status_port}/json/"


def _coerce_memory_mb(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)

    text = str(value).strip().lower()
    if not text:
        return 0
    try:
        if text.endswith("g"):
            return int(float(text[:-1]) * 1024)
        if text.endswith("m"):
            return int(float(text[:-1]))
        return int(float(text))
    except ValueError:
        return 0


def _normalize_spark_profile(job_type: str) -> str:
    profile_name = (job_type or "default").strip().lower()
    if profile_name not in DEFAULT_SPARK_RESOURCE_PROFILES:
        return "default"
    return profile_name


def _build_spark_resource_profile(
    job_type: str,
    env: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    profile_name = _normalize_spark_profile(job_type)
    defaults = DEFAULT_SPARK_RESOURCE_PROFILES[profile_name]
    prefix = f"SPARK_{profile_name.upper()}_"

    return {
        "name": profile_name,
        "min_cores": _env_int(f"{prefix}MIN_CORES", defaults["min_cores"], env),
        "min_memory_mb": _env_int(
            f"{prefix}MIN_MEMORY_MB",
            defaults["min_memory_mb"],
            env,
        ),
        "cores_max": _env_int(f"{prefix}CORES_MAX", defaults["cores_max"], env),
    }


def _build_executor_cores(
    resource_profile: Dict[str, Any],
    env: Optional[Dict[str, str]] = None,
) -> int:
    current_env = os.environ if env is None else env
    if current_env.get("SPARK_EXECUTOR_CORES"):
        return max(1, min(_env_int("SPARK_EXECUTOR_CORES", 1, current_env), int(resource_profile["cores_max"])))

    legacy_worker_cores = _env_int("SPARK_WORKER_CORES", 1, current_env)
    return max(1, min(legacy_worker_cores, int(resource_profile["cores_max"])))


def _spark_master_available_resources(master_status: Dict[str, Any]) -> Dict[str, int]:
    cores_free = 0
    memory_free_mb = 0

    for worker in master_status.get("workers", []):
        w = {k.lower(): v for k, v in worker.items()}
        if str(w.get("state", "")).upper() != "ALIVE":
            continue
        cores_free += int(w.get("coresfree") or 0)
        memory_free_mb += _coerce_memory_mb(w.get("memoryfree"))

    return {"cores_free": cores_free, "memory_free_mb": memory_free_mb}


def _has_spark_capacity(
    available: Dict[str, int],
    resource_profile: Dict[str, Any],
) -> bool:
    return (
        available["cores_free"] >= resource_profile["min_cores"]
        and available["memory_free_mb"] >= resource_profile["min_memory_mb"]
    )


def _clean_app_name_part(value: str, fallback: str) -> str:
    cleaned = str(value or fallback).strip().replace("/", "-")
    return cleaned or fallback


def _short_run_id(run_id: str) -> str:
    return _clean_app_name_part(run_id, "manual")[:8]


def _build_spark_app_name(user_code: str, job_name: str, run_id: str) -> str:
    return "/".join(
        [
            _clean_app_name_part(user_code, "unknown_user_code"),
            _clean_app_name_part(job_name, "unknown_job"),
            _short_run_id(run_id),
        ]
    )


def _build_spark_app_context(
    init_context: Any,
    default_app_name: str,
    default_user_code: str,
) -> Dict[str, str]:
    dagster_run = getattr(init_context, "dagster_run", None)
    tags = dict(getattr(dagster_run, "tags", {}) or {})
    job_name = (
        getattr(dagster_run, "job_name", "")
        or os.getenv("SPARK_JOB_NAME", default_app_name)
    )
    run_id = getattr(dagster_run, "run_id", "") or os.getenv("SPARK_RUN_ID", "manual")
    user_code = tags.get("user_code") or os.getenv("SPARK_USER_CODE", default_user_code)
    job_type = tags.get("job_type") or os.getenv("SPARK_JOB_TYPE")
    if not job_type and "_maintenance_" in job_name.lower():
        job_type = "maintenance"
    if not job_type:
        job_type = "default"
    if tags.get("backfill"):
        job_type = "backfill"

    return {
        "user_code": user_code,
        "job_name": job_name,
        "job_type": job_type,
        "run_id": _short_run_id(run_id),
        "app_name": _build_spark_app_name(user_code, job_name, run_id),
    }


LECTURAS_SCHEMA = StructType(
    [
        StructField("ReadId", StringType(), True),
        StructField("TimeSpan", TimestampType(), True),
        StructField("LocalTimeSpan", TimestampType(), True),
        StructField("Value", DoubleType(), True),
        StructField("SensorId", StringType(), True),
    ]
)

class SparkResource:
    def __init__(
        self,
        master_url: str,
        worker_mem: str,
        executor_mem: str,
        executor_cores: int,
        app_name: str,
        adls_account_name: str,
        adls_container_name: str,
        adls_base_path: str,
        adls_account_key: Optional[str],
        ozone_s3_endpoint: str,
        ozone_s3_region: str,
        ozone_s3_access_key: Optional[str],
        ozone_s3_secret_key: Optional[str],
        ozone_s3_path_style_access: str,
        ozone_s3_bucket_probe: str,
        ozone_s3_change_detection_mode: str,
        jdbc_host: str,
        jdbc_port: int,
        jdbc_database: str,
        jdbc_user: str,
        jdbc_driver: str,
        jdbc_password: str,
        jdbc_write_num_chunks: int,
        jdbc_write_target_partitions: int,
        jdbc_write_batch_size: int,
        user_code: str = "",
        job_name: str = "",
        job_type: str = "default",
        run_id: str = "",
        master_status_url: str = DEFAULT_SPARK_MASTER_STATUS_URL,
        resource_wait_seconds: int = DEFAULT_SPARK_RESOURCE_WAIT_SECONDS,
        resource_poll_seconds: int = DEFAULT_SPARK_RESOURCE_POLL_SECONDS,
        driver_host: str = "",
        driver_bind_address: str = "",
        driver_port: str = "",
        driver_block_manager_port: str = "",
        block_manager_port: str = "",
        worker_network_timeout_seconds: int = (
            DEFAULT_SPARK_WORKER_NETWORK_TIMEOUT_SECONDS
        ),
        resource_profile: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.master_url = master_url
        self.app_name = app_name
        self.worker_mem = worker_mem
        self.executor_mem = executor_mem
        self.executor_cores = executor_cores
        self.user_code = user_code
        self.job_name = job_name
        self.job_type = job_type
        self.run_id = run_id
        self.master_status_url = master_status_url
        self.resource_wait_seconds = resource_wait_seconds
        self.resource_poll_seconds = resource_poll_seconds
        self.driver_host = driver_host
        self.driver_bind_address = driver_bind_address
        self.driver_port = driver_port
        self.driver_block_manager_port = driver_block_manager_port
        self.block_manager_port = block_manager_port
        self.worker_network_timeout_seconds = worker_network_timeout_seconds
        self.resource_profile = resource_profile or _build_spark_resource_profile(job_type)
        self.adls_account_name = adls_account_name
        self.adls_container_name = adls_container_name
        self.adls_base_path = adls_base_path
        self.adls_account_key = adls_account_key
        self.ozone_s3_endpoint = ozone_s3_endpoint
        self.ozone_s3_region = ozone_s3_region
        self.ozone_s3_access_key = ozone_s3_access_key
        self.ozone_s3_secret_key = ozone_s3_secret_key
        self.ozone_s3_path_style_access = ozone_s3_path_style_access
        self.ozone_s3_bucket_probe = ozone_s3_bucket_probe
        self.ozone_s3_change_detection_mode = ozone_s3_change_detection_mode
        self.jdbc_host = jdbc_host
        self.jdbc_port = jdbc_port
        self.jdbc_database = jdbc_database
        self.jdbc_user = jdbc_user
        self.jdbc_driver = jdbc_driver
        self.jdbc_password = jdbc_password
        self.jdbc_write_num_chunks = jdbc_write_num_chunks
        self.jdbc_write_target_partitions = jdbc_write_target_partitions
        self.jdbc_write_batch_size = jdbc_write_batch_size
        self._spark: Optional[SparkSession] = None

    def _ozone_s3_enabled(self) -> bool:
        return bool(self.ozone_s3_endpoint)

    def _spark_packages(self) -> List[str]:
        packages = []
        if self.adls_account_name and self.adls_account_key:
            packages.extend([
                "org.apache.hadoop:hadoop-azure:3.5.0",
                "com.microsoft.azure:azure-storage:7.0.1",
                MSSQL_JDBC_PACKAGE,
            ])
        return packages

    def _build_ozone_s3_options(self) -> Dict[str, str]:
        if not self._ozone_s3_enabled():
            return {}

        options = {
            "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "fs.s3a.endpoint": self.ozone_s3_endpoint,
            "fs.s3a.path.style.access": self.ozone_s3_path_style_access,
            "fs.s3a.bucket.probe": self.ozone_s3_bucket_probe,
            "fs.s3a.change.detection.mode": self.ozone_s3_change_detection_mode,
            "fs.s3a.connection.ssl.enabled": (
                "false"
                if self.ozone_s3_endpoint.lower().startswith("http://")
                else "true"
            ),
        }
        if self.ozone_s3_region:
            options["fs.s3a.endpoint.region"] = self.ozone_s3_region
        options.update(_build_s3a_tuning_conf())
        if self.ozone_s3_access_key and self.ozone_s3_secret_key:
            options["fs.s3a.access.key"] = self.ozone_s3_access_key
            options["fs.s3a.secret.key"] = self.ozone_s3_secret_key
            options["fs.s3a.aws.credentials.provider"] = (
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            )
        return options

    def _build_spark_conf(self) -> SparkConf:
        conf = (
            SparkConf()
            .setAppName(self.app_name)
            .set("spark.ui.enabled", "true")
            .set("spark.eventLog.enabled", "true")
            .set("spark.eventLog.dir", "file:///opt/spark-events")
            .setMaster(self.master_url)
            .set("spark.executor.memory", self.executor_mem)
            .set("spark.executor.cores", str(self.executor_cores))
            .set("spark.cores.max", str(self.resource_profile["cores_max"]))
            .set("spark.jars.ivy", os.path.join(os.path.expanduser("~"), ".ivy2.5.2", self.resource_profile["name"]))
        )
        if self.driver_host:
            conf.set("spark.driver.host", self.driver_host)
            conf.set(
                "spark.driver.bindAddress",
                self.driver_bind_address or DEFAULT_SPARK_DRIVER_BIND_ADDRESS,
            )
        if self.driver_port:
            conf.set("spark.driver.port", str(self.driver_port))
        if self.driver_block_manager_port:
            conf.set(
                "spark.driver.blockManager.port",
                str(self.driver_block_manager_port),
            )
        if self.block_manager_port:
            conf.set("spark.blockManager.port", str(self.block_manager_port))

        for key, value in _build_spark_retry_conf().items():
            conf.set(key, value)

        packages = self._spark_packages()
        if packages:
            conf.set("spark.jars.packages", ",".join(packages))
        for key, value in self._build_ozone_s3_options().items():
            conf.set("spark.hadoop." + key, value)

        if self.adls_account_name and self.adls_account_key:
            account = self.adls_account_name
            conf.set(
                f"fs.azure.account.key.{account}.dfs.core.windows.net",
                self.adls_account_key,
            )
            conf.set(
                "fs.abfss.impl",
                "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem",
            )
            conf.set(
                "spark.jars.packages",
                ",".join(packages)
            )
        return conf

    def _fetch_spark_master_status(self) -> Dict[str, Any]:
        with urlopen(self.master_status_url, timeout=5) as response:
            return json.loads(response.read().decode("utf-8"))

    def _worker_endpoint_is_routable(self, host: str, port: int) -> bool:
        try:
            with socket.create_connection(
                (host, port),
                timeout=max(self.worker_network_timeout_seconds, 1),
            ):
                return True
        except ConnectionRefusedError:
            return True
        except OSError:
            return False

    def _validate_spark_worker_network(self, master_status: Dict[str, Any]) -> None:
        if not self.block_manager_port:
            return

        block_manager_port = int(self.block_manager_port)
        unreachable = []
        for worker in master_status.get("workers", []):
            w = {k.lower(): v for k, v in worker.items()}
            if str(w.get("state", "")).upper() != "ALIVE":
                continue
            host = str(w.get("host") or "").strip()
            if not host:
                continue
            if not self._worker_endpoint_is_routable(host, block_manager_port):
                unreachable.append(f"{host}:{block_manager_port}")

        if unreachable:
            raise RuntimeError(
                "Spark worker block manager endpoints are not reachable from "
                f"the driver: {', '.join(unreachable)}. Configure each worker "
                "to advertise a routable host/IP and publish "
                "SPARK_BLOCK_MANAGER_PORT for executor shuffle traffic."
            )

    def _validate_port_assignments(self) -> None:
        ports = {}
        for label, value in [
            ("spark.driver.port", self.driver_port),
            ("spark.driver.blockManager.port", self.driver_block_manager_port),
            ("spark.blockManager.port", self.block_manager_port),
        ]:
            if not value:
                continue
            port_str = str(value).strip()
            if not port_str:
                continue
            if port_str in ports:
                raise RuntimeError(
                    f"Spark port conflict: {label} and {ports[port_str]} "
                    f"are both set to port {port_str}. Each Spark port "
                    f"(driver, driver block manager, executor block manager) "
                    f"must be unique."
                )
            ports[port_str] = label

    def _wait_for_spark_capacity(self) -> None:
        if self.master_url.lower().startswith("local"):
            return
        if not self.master_status_url:
            raise RuntimeError("SPARK_MASTER_STATUS_URL is required for Spark capacity checks.")

        deadline = time.monotonic() + max(self.resource_wait_seconds, 0)
        last_available = {"cores_free": 0, "memory_free_mb": 0}

        while True:
            try:
                master_status = self._fetch_spark_master_status()
            except Exception as exc:
                raise RuntimeError(
                    "Could not read Spark master status from "
                    f"{self.master_status_url}: {exc}"
                ) from exc

            last_available = _spark_master_available_resources(master_status)
            if _has_spark_capacity(last_available, self.resource_profile):
                self._validate_spark_worker_network(master_status)
                return

            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "Insufficient Spark resources for "
                    f"job_type={self.job_type!r}: required "
                    f"{self.resource_profile['min_cores']} cores and "
                    f"{self.resource_profile['min_memory_mb']} MB free memory; "
                    f"available {last_available['cores_free']} cores and "
                    f"{last_available['memory_free_mb']} MB free memory."
                )

            time.sleep(max(self.resource_poll_seconds, 1))

    def get_spark(self) -> SparkSession:
        if self._spark is not None:
            return self._spark

        self._validate_port_assignments()
        self._wait_for_spark_capacity()
        conf = self._build_spark_conf()

        self._spark = (
            SparkSession.builder
            .config(conf=conf)
            .getOrCreate()
        )
        if self.adls_account_name and self.adls_account_key:
            account = self.adls_account_name
            hadoop_conf = self._spark._jsc.hadoopConfiguration()
            hadoop_conf.set(
                f"fs.azure.account.key.{account}.dfs.core.windows.net",
                self.adls_account_key,
            )
            hadoop_conf.set(
                "fs.abfss.impl",
                "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem",
            )
        for key, value in self._build_ozone_s3_options().items():
            self._spark._jsc.hadoopConfiguration().set(key, value)
        return self._spark

    def stop(self) -> None:
        if self._spark is not None:
            try:
                self._spark.stop()
            except Exception:
                pass
            self._spark = None

    def _build_adls_path(self) -> str:
        return (
            "abfss://"
            + self.adls_container_name
            + "@"
            + self.adls_account_name
            + ".dfs.core.windows.net/"
            + self.adls_base_path
        )

    def _build_wh_path(self) -> Dict[str, Any]:
        url = (
                "jdbc:sqlserver://"
                + self.jdbc_host
                + ":"
                + str(self.jdbc_port)
                + ";databaseName="
                + self.jdbc_database
                + ";encrypt=true;trustServerCertificate=true"
        )
        properties = {
            "user": self.jdbc_user,
            "password": self.jdbc_password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "batchsize": "200",
        }
        return {"url": url, "properties": properties}

    def _write_jdbc_in_chunks(
        self,
        df: DataFrame,
        table: str,
        chunk_column: str,
        properties: Optional[Dict[str, str]] = None,
        storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK,
    ) -> int:
        wh = self._build_wh_path()
        url = wh["url"]
        write_properties = dict(properties or wh["properties"])
        write_properties.pop("batchsize", None)

        df_chunked = (
            df
            .selectExpr("*", f"pmod(hash(`{chunk_column}`), {self.jdbc_write_num_chunks}) as __chunk_id")
            .persist(storage_level)
        )

        try:
            row_count = df_chunked.count()
            if row_count == 0:
                return 0

            chunk_ids = [
                row["__chunk_id"]
                for row in df_chunked.select("__chunk_id").distinct().collect()
            ]
            for chunk_id in chunk_ids:
                df_chunk = (
                    df_chunked
                    .filter(f"__chunk_id = {chunk_id}")
                    .drop("__chunk_id")
                    .coalesce(self.jdbc_write_target_partitions)
                )

                (
                    df_chunk.write
                    .mode("append")
                    .format("jdbc")
                    .option("url", url)
                    .option("dbtable", table)
                    .option("batchsize", str(self.jdbc_write_batch_size))
                    .option("isolationLevel", "READ_COMMITTED")
                    .options(**write_properties)
                    .save()
                )
        finally:
            df_chunked.unpersist()

        return row_count

    def _has_readable_files(self, path: str) -> bool:
        spark = self.get_spark()
        j_path = spark._jvm.org.apache.hadoop.fs.Path(path)
        fs = j_path.getFileSystem(spark._jsc.hadoopConfiguration())
        if not fs.exists(j_path):
            return False

        pending = [j_path]
        while pending:
            current_path = pending.pop()
            for status in fs.listStatus(current_path):
                name = status.getPath().getName()
                if status.isDirectory():
                    pending.append(status.getPath())
                elif name.lower().endswith(".parquet"):
                    return True
        return False

    def _existing_readable_paths(self, paths: List[str]) -> List[str]:
        existing_paths = []
        for path in paths:
            if self._has_readable_files(path):
                existing_paths.append(path)
        return existing_paths

    def _empty_lecturas_dataframe(self) -> DataFrame:
        return self.get_spark().createDataFrame([], LECTURAS_SCHEMA)

    def read_lecturas_range(self, from_utc: datetime, to_utc: datetime) -> DataFrame:
        spark = self.get_spark()
        base = self._build_adls_path()

        days = _daterange_days(from_utc, to_utc)
        paths = []
        for d in days:
            year_str = str(d.year).zfill(4)
            month_str = str(d.month)
            day_str = str(d.day)
            path = (
                    base
                    + "/"
                    + year_str
                    + "/"
                    + month_str
                    + "/"
                    + day_str
                    + "//"
            )
            paths.append(path)

        if not paths:
            return self._empty_lecturas_dataframe()

        existing_paths = self._existing_readable_paths(paths)
        if not existing_paths:
            return self._empty_lecturas_dataframe()

        try:
            df = (
                spark.read
                .option("ignoreCorruptFiles", "true")
                .parquet(*existing_paths)
            )
        except Exception as exc:
            logging.getLogger(__name__).warning(
                "No valid lecturas parquet files found in path(s) %s: %s",
                existing_paths,
                exc,
            )
            return self._empty_lecturas_dataframe()

        return df.filter(
            (col("TimeSpan") >= from_utc) & (col("TimeSpan") <= to_utc)
        )

    def read_lecturas_backfill_range(
        self,
        from_utc: datetime,
        to_utc: datetime,
        sensor_ids: Optional[List[str]] = None,
    ) -> DataFrame:
        df = self.read_lecturas_range(from_utc, to_utc)
        df = df.filter(col("TimeSpan") < to_utc)
        if sensor_ids:
            normalized_sensor_ids = [str(sensor_id).upper() for sensor_id in sensor_ids]
            df = df.filter(upper(col("SensorId").cast("string")).isin(*normalized_sensor_ids))

        return df

    def read_lecturas_local_range(self, from_local: datetime, to_utc: datetime) -> DataFrame:
        from_utc = _local_to_utc_naive(from_local)
        readings_df = self.read_lecturas_range(from_utc, _coerce_utc_naive(to_utc))
        return readings_df.filter(col("LocalTimeSpan") >= from_local)

    def write_fact_lecturas_to_sql(self, df: DataFrame) -> int:
        df_dedup = df.dropDuplicates(["ReadId"])
        if len(df_dedup.head(1)) == 0:
            return 0

        return self._write_jdbc_in_chunks(
            df_dedup,
            table="dwh.factLecturas",
            chunk_column="ReadId",
        )

    def write_fact_operations_incremental(self, df: DataFrame) -> int:

        try:
            max_df = self._spark.read.jdbc(
                url=self._build_wh_path()["url"],
                table="(SELECT MAX(LocalTimeSpan) AS max_ts FROM dwh.factReadingsMeasurement) AS t",
                properties=self._build_wh_path()["properties"],
            )
            row = max_df.collect()[0]
            max_ts = row.max_ts
        except Exception:
            max_ts = None

        df_filtered = df
        if max_ts is not None:
            df_filtered = df_filtered.filter(col("LocalTimeSpan") >= max_ts)

        if len(df_filtered.head(1)) == 0:
            return 0

        return self._write_jdbc_in_chunks(
            df_filtered,
            table="dwh.factReadingsMeasurement",
            chunk_column="OperationId",
            properties={
                "user": self.jdbc_user,
                "password": self.jdbc_password,
                "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            },
        )

    def write_fact_operations(self, df: DataFrame) -> int:

        return self._write_jdbc_in_chunks(
            df,
            table="dwh.factReadingsMeasurement",
            chunk_column="OperationId",
        )

    def write_fact_operations_stage(self, df: DataFrame, run_id: str) -> int:
        operation_columns = df.columns
        df_stage = (
            df
            .select(*operation_columns)
            .dropDuplicates(["OperationId"])
            .withColumn("RunId", lit(run_id))
            .select("RunId", *operation_columns)
        )

        return self._write_jdbc_in_chunks(
            df_stage,
            table="stg.factReadingsMeasurement",
            chunk_column="OperationId",
        )


    def get_max_time_span(self, df: DataFrame, time_span_type: str) -> Optional[datetime]:
        row = df.agg(spark_max(time_span_type).alias("max_ts")).collect()[0]
        max_ts = row.max_ts
        if max_ts is None:
            return None
        return max_ts

    def write_dim_prod_dimensions(
        self,
        df: DataFrame,
        run_id: str,
    ) -> Optional[int]:
        dimension_columns = df.columns
        df_stage = (
            df
            .dropDuplicates(["SensorId"])
            .withColumn("RunId", lit(run_id))
            .select("RunId", *dimension_columns)
        )
        if len(df_stage.head(1)) == 0:
            return 0

        return self._write_jdbc_in_chunks(
            df_stage,
            table="stg.dimTelemetryProdDimensions",
            chunk_column="SensorId",
        )

    def write_dim_aspnet_users(
        self,
        df: DataFrame,
        run_id: str,
    ) -> Optional[int]:
        dimension_columns = [
            "Id",
            "UserName",
            "Name",
            "LastName",
            "Position",
        ]
        df_stage = (
            df
            .select(*dimension_columns)
            .dropDuplicates(["Id"])
            .withColumn("RunId", lit(run_id))
            .select("RunId", *dimension_columns)
        )
        if len(df_stage.head(1)) == 0:
            return 0

        return self._write_jdbc_in_chunks(
            df_stage,
            table="stg.dimAspNetUsers",
            chunk_column="Id",
        )

    def write_fact_telemetry_user_usage_stage(
        self,
        df: DataFrame,
        run_id: str,
    ) -> int:
        fact_columns = [
            "UserId",
            "Date",
            "HasConnected",
            "MadeCompleteInteraction",
            "TimeConnected",
            "AvgLatency",
            "Latency95thPercentile",
        ]
        df_stage = (
            df
            .select(*fact_columns)
            .dropDuplicates(["Date", "UserId"])
            .withColumn("RunId", lit(run_id))
            .select("RunId", *fact_columns)
        )

        return self._write_jdbc_in_chunks(
            df_stage,
            table="stg.factTelemetryUserUsage",
            chunk_column="UserId",
        )

    def write_dim_sensor_expected_values(self, df: DataFrame) -> int:
        df_write = df.dropDuplicates(
            [
                "SensorId",
                "DeviceId",
                "StartTime",
                "EndTime",
                "scheduledDays",
                "min_expected_value",
                "max_expected_value",
            ]
        )

        return self._write_jdbc_in_chunks(
            df_write,
            table="dwh.dimSensorExpectedVal",
            chunk_column="SensorId",
            storage_level=StorageLevel.DISK_ONLY,
        )

    def read_dim_prod_dimensions(self) -> DataFrame:
        spark = self.get_spark()

        return spark.read.jdbc(
            url=self._build_wh_path()["url"],
            table="dwh.dimTelemetryProdDimensions",
            properties=self._build_wh_path()["properties"]
        )

    def read_fact_redings_audits_state(
        self,
        from_utc: datetime,
        to_utc: datetime,
    ) -> DataFrame:
        spark = self.get_spark()
        from_text = from_utc.strftime("%Y-%m-%d %H:%M:%S")
        to_text = to_utc.strftime("%Y-%m-%d %H:%M:%S")
        table = (
            "(SELECT SensorId, TimeSpan, LastReadingAt "
            "FROM observability.factRedingsAudits "
            "WHERE TimeSpan >= '"
            + from_text
            + "' AND TimeSpan <= '"
            + to_text
            + "' "
            "UNION ALL "
            "SELECT SensorId, TimeSpan, LastReadingAt "
            "FROM ("
            "SELECT SensorId, TimeSpan, LastReadingAt, "
            "ROW_NUMBER() OVER ("
            "PARTITION BY SensorId ORDER BY TimeSpan DESC"
            ") AS PreviousRow "
            "FROM observability.factRedingsAudits "
            "WHERE TimeSpan < '"
            + from_text
            + "'"
            ") AS previous "
            "WHERE PreviousRow = 1) AS fact_redings_audits_state"
        )

        return spark.read.jdbc(
            url=self._build_wh_path()["url"],
            table=table,
            properties=self._build_wh_path()["properties"],
        )

    def read_dim_sensor_expected_values(self) -> DataFrame:
        spark = self.get_spark()

        return spark.read.jdbc(
            url=self._build_wh_path()["url"],
            table="dwh.dimSensorExpectedVal",
            properties=self._build_wh_path()["properties"]
        )

    def read_completed_sensor_events_range(
        self,
        sensor_type: str,
        from_utc: datetime,
        to_utc: datetime,
    ) -> DataFrame:
        spark = self.get_spark()
        from_text = from_utc.strftime("%Y-%m-%d %H:%M:%S")
        to_text = to_utc.strftime("%Y-%m-%d %H:%M:%S")
        table = (
            "(SELECT SensorId, TimeSpan AS PreviousTimeSpan, "
            "Value AS PreviousValue, "
            "DATEADD(SECOND, CONVERT(INT, ROUND(EventMinutes * 60, 0)), "
            "TimeSpan) AS PreviousEndTime "
            "FROM dwh.factSensorEvents "
            "WHERE LOWER(SensorType) LIKE LOWER('"
            + sensor_type
            + "') AND IsEventCompleted = 1 "
            "AND TimeSpan < '"
            + to_text
            + "' AND DATEADD(SECOND, CONVERT(INT, ROUND(EventMinutes * 60, 0)), "
            "TimeSpan) > '"
            + from_text
            + "') AS completed_sensor_events_range"
        )

        return spark.read.jdbc(
            url=self._build_wh_path()["url"],
            table=table,
            properties=self._build_wh_path()["properties"],
        )

    def write_fact_sensor_events(self, df: DataFrame, run_id: str) -> int:
        df_dedup = (
            df
            .select(
                "EventId",
                "ReadId",
                "SensorId",
                "DeviceId",
                "LocationId",
                "LocationName",
                "DeviceType",
                "SensorType",
                "SensorName",
                "TimeSpan",
                "LocalTimeSpan",
                "Value",
                "EventMinutes",
                "IsEventCompleted",
            )
            .dropDuplicates(["EventId"])
        )
        df_stage = (
            df_dedup
            .withColumn("RunId", lit(run_id))
            .select("RunId", *df_dedup.columns)
        )

        return self._write_jdbc_in_chunks(
            df_stage,
            table="stg.factSensorEvents",
            chunk_column="EventId",
        )

    def write_fact_redings_audits_stage(
        self,
        df: DataFrame,
        run_id: str,
    ) -> int:
        fact_columns = [
            "SensorId",
            "TimeSpan",
            "LocalTimeSpan",
            "ReadingsCount",
            "AverageGapMinutes",
            "MaximumGapMinutes",
            "MinimumGapMinutes",
            "ReadingsCoveragePercentage",
            "CoveredReadingBlocks",
            "HasLateReadings",
            "IsConnectionLost",
            "LastReadingAt",
            "ConnectionLostAt",
            "MinutesWithoutReadings",
        ]
        df_stage = (
            df
            .select(*fact_columns)
            .dropDuplicates(["SensorId", "TimeSpan"])
            .withColumn("RunId", lit(run_id))
            .select("RunId", *fact_columns)
        )

        return self._write_jdbc_in_chunks(
            df_stage,
            table="stg.factRedingsAudits",
            chunk_column="SensorId",
        )

    def write_chirpstack_device_metrics_stage(
        self,
        df: DataFrame,
        run_id: str,
    ) -> int:
        fact_columns = [
            "DevEui",
            "TimeSpan",
            "TenantId",
            "ApplicationId",
            "DeviceProfileId",
            "UplinkIntervalSeconds",
            "ExpectedUplinkCount",
            "ReceivedUplinkCount",
            "AverageRssi",
            "AverageSnr",
            "LinkErrorCount",
            "LinkErrorCountsJson",
            "BatteryLevel",
            "ExternalPowerSource",
            "BatteryLevelSource",
            "BatteryObservedAt",
            "MetricsAvailable",
            "ExtractedAt",
        ]
        df_stage = (
            df
            .select(*fact_columns)
            .dropDuplicates(["DevEui", "TimeSpan"])
            .withColumn("RunId", lit(run_id))
            .select("RunId", *fact_columns)
        )

        return self._write_jdbc_in_chunks(
            df_stage,
            table="stg.factChirpStackDeviceMetrics",
            chunk_column="DevEui",
        )

    def write_chirpstack_gateway_metrics_stage(
        self,
        df: DataFrame,
        run_id: str,
    ) -> int:
        fact_columns = [
            "GatewayId",
            "TimeSpan",
            "TenantId",
            "GatewayName",
            "ReceivedPacketCount",
            "TransmittedPacketCount",
            "TxOkCount",
            "TxErrorCount",
            "TxPacketStatusCountsJson",
            "GatewayState",
            "GatewayLastSeenAt",
            "StatsIntervalSeconds",
            "MetricsAvailable",
            "ExtractedAt",
        ]
        df_stage = (
            df
            .select(*fact_columns)
            .dropDuplicates(["GatewayId", "TimeSpan"])
            .withColumn("RunId", lit(run_id))
            .select("RunId", *fact_columns)
        )

        return self._write_jdbc_in_chunks(
            df_stage,
            table="stg.factChirpStackGatewayMetrics",
            chunk_column="GatewayId",
        )

    def write_chirpstack_device_event_metrics_stage(
        self,
        df: DataFrame,
        run_id: str,
    ) -> int:
        fact_columns = [
            "DevEui",
            "TimeSpan",
            "TenantId",
            "CapturedUplinkCount",
            "ObservedGatewayCount",
            "MultiGatewayUplinkCount",
            "EventLogCountsJson",
            "RadioProfileCountsJson",
            "UplinkContextCountsJson",
            "RadioFieldQualityJson",
            "FrameCounterSummaryJson",
            "ExtractedAt",
        ]
        df_stage = (
            df
            .select(*fact_columns)
            .dropDuplicates(["DevEui", "TimeSpan"])
            .withColumn("RunId", lit(run_id))
            .select("RunId", *fact_columns)
        )

        return self._write_jdbc_in_chunks(
            df_stage,
            table="stg.factChirpStackDeviceEventMetrics",
            chunk_column="DevEui",
        )

    def write_chirpstack_device_gateway_metrics_stage(
        self,
        df: DataFrame,
        run_id: str,
    ) -> int:
        fact_columns = [
            "DevEui",
            "GatewayId",
            "TimeSpan",
            "TenantId",
            "GatewayReceptionCount",
            "ReceptionShare",
            "AverageRssi",
            "MinRssi",
            "MaxRssi",
            "AverageSnr",
            "MinSnr",
            "MaxSnr",
            "RadioReceptionStatsJson",
            "ReceptionFieldQualityJson",
            "FirstReceivedAt",
            "LastReceivedAt",
            "ExtractedAt",
        ]
        df_stage = (
            df
            .select(*fact_columns)
            .dropDuplicates(["DevEui", "GatewayId", "TimeSpan"])
            .withColumn("RunId", lit(run_id))
            .select("RunId", *fact_columns)
        )

        return self._write_jdbc_in_chunks(
            df_stage,
            table="stg.factChirpStackDeviceGatewayMetrics",
            chunk_column="DevEui",
        )

    def create_emptiness_dataframe(self) -> DataFrame:
        spark = self.get_spark()
        return spark.createDataFrame([(True,)], ["is_empty"])

@resource
def spark_resource(init_context):
    master_url = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    default_app_name = os.getenv("SPARK_APP_NAME", "FactLecturasIngestion")
    default_user_code = __name__.split(".")[0]
    app_context = _build_spark_app_context(
        init_context,
        default_app_name=default_app_name,
        default_user_code=default_user_code,
    )
    resource_profile = _build_spark_resource_profile(app_context["job_type"])
    worker_mem = os.getenv("SPARK_WORKER_MEM", "4G")
    executor_mem = os.getenv("SPARK_EXECUTOR_MEM", "3G")
    executor_cores = _build_executor_cores(resource_profile)
    master_status_url = _build_spark_master_status_url(master_url)
    resource_wait_seconds = int(
        os.getenv("SPARK_RESOURCE_WAIT_SECONDS", str(DEFAULT_SPARK_RESOURCE_WAIT_SECONDS))
    )
    resource_poll_seconds = int(
        os.getenv("SPARK_RESOURCE_POLL_SECONDS", str(DEFAULT_SPARK_RESOURCE_POLL_SECONDS))
    )
    driver_host = os.getenv("SPARK_DRIVER_HOST", "")
    driver_bind_address = os.getenv("SPARK_DRIVER_BIND_ADDRESS", "")
    driver_port = os.getenv("SPARK_DRIVER_PORT", "")
    driver_block_manager_port = os.getenv("SPARK_DRIVER_BLOCK_MANAGER_PORT", "")
    block_manager_port = os.getenv("SPARK_BLOCK_MANAGER_PORT", "")
    if resource_profile["name"] == "observability" or app_context["job_name"] == "120_observability_dim_aspnetusers":
        driver_port = os.getenv("SPARK_OBSERVABILITY_DRIVER_PORT", driver_port)
        driver_block_manager_port = os.getenv("SPARK_OBSERVABILITY_DRIVER_BLOCK_MANAGER_PORT", driver_block_manager_port)
        block_manager_port = os.getenv("SPARK_OBSERVABILITY_BLOCK_MANAGER_PORT", block_manager_port)
    worker_network_timeout_seconds = int(
        os.getenv(
            "SPARK_WORKER_NETWORK_TIMEOUT_SECONDS",
            str(DEFAULT_SPARK_WORKER_NETWORK_TIMEOUT_SECONDS),
        )
    )
    adls_account_name = os.getenv("ADLS_ACCOUNT_NAME", "")
    adls_container_name = os.getenv("ADLS_CONTAINER_NAME", "")
    adls_base_path = os.getenv("ADLS_BASE_PATH", "")
    adls_account_key = os.getenv("ADLS_ACCOUNT_KEY")
    ozone_s3_endpoint = os.getenv("OZONE_S3_ENDPOINT", "")
    ozone_s3_region = os.getenv("OZONE_S3_REGION", "us-east-1")
    ozone_s3_access_key = os.getenv("OZONE_S3_ACCESS_KEY")
    ozone_s3_secret_key = os.getenv("OZONE_S3_SECRET_KEY")
    ozone_s3_path_style_access = os.getenv("OZONE_S3_PATH_STYLE_ACCESS", "true")
    ozone_s3_bucket_probe = os.getenv("OZONE_S3_BUCKET_PROBE", "0")
    ozone_s3_change_detection_mode = os.getenv(
        "OZONE_S3_CHANGE_DETECTION_MODE",
        "none",
    )
    jdbc_host = os.getenv("MSSQL_HOST", "localhost")
    jdbc_port = int(os.getenv("MSSQL_PORT", "1433"))
    jdbc_database = os.getenv("APP_DB", "")
    jdbc_user = os.getenv("MSSQL_USER", "")
    jdbc_driver = os.getenv("MSSQL_DRIVER", "")
    jdbc_password = os.getenv("MSSQL_PASSWORD", "")
    jdbc_write_num_chunks = int(os.getenv("JDBC_WRITE_NUM_CHUNKS", "1"))
    jdbc_write_target_partitions = int(os.getenv("JDBC_WRITE_TARGET_PARTITIONS", "2"))
    jdbc_write_batch_size = int(os.getenv("JDBC_WRITE_BATCH_SIZE", "50"))
    spark_res = SparkResource(
        master_url=master_url,
        app_name=app_context["app_name"],
        worker_mem=worker_mem,
        executor_mem=executor_mem,
        executor_cores=executor_cores,
        user_code=app_context["user_code"],
        job_name=app_context["job_name"],
        job_type=app_context["job_type"],
        run_id=app_context["run_id"],
        master_status_url=master_status_url,
        resource_wait_seconds=resource_wait_seconds,
        resource_poll_seconds=resource_poll_seconds,
        driver_host=driver_host,
        driver_bind_address=driver_bind_address,
        driver_port=driver_port,
        driver_block_manager_port=driver_block_manager_port,
        block_manager_port=block_manager_port,
        worker_network_timeout_seconds=worker_network_timeout_seconds,
        resource_profile=resource_profile,
        adls_account_name=adls_account_name,
        adls_container_name=adls_container_name,
        adls_base_path=adls_base_path,
        adls_account_key=adls_account_key,
        ozone_s3_endpoint=ozone_s3_endpoint,
        ozone_s3_region=ozone_s3_region,
        ozone_s3_access_key=ozone_s3_access_key,
        ozone_s3_secret_key=ozone_s3_secret_key,
        ozone_s3_path_style_access=ozone_s3_path_style_access,
        ozone_s3_bucket_probe=ozone_s3_bucket_probe,
        ozone_s3_change_detection_mode=ozone_s3_change_detection_mode,
        jdbc_host=jdbc_host,
        jdbc_port=jdbc_port,
        jdbc_database=jdbc_database,
        jdbc_user=jdbc_user,
        jdbc_driver=jdbc_driver,
        jdbc_password=jdbc_password,
        jdbc_write_num_chunks=jdbc_write_num_chunks,
        jdbc_write_target_partitions=jdbc_write_target_partitions,
        jdbc_write_batch_size=jdbc_write_batch_size,
    )
    try:
        yield spark_res
    finally:
        spark_res.stop()
