import importlib.util
import sys
import types
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _install_optional_stubs() -> None:
    dagster_module = sys.modules.get("dagster")
    if dagster_module is None:
        try:
            import dagster as dagster_module
        except Exception:
            dagster_module = types.ModuleType("dagster")
            dagster_module.__path__ = [str(PROJECT_ROOT.parent)]
            sys.modules["dagster"] = dagster_module
    if not hasattr(dagster_module, "__path__"):
        dagster_module.__path__ = [str(PROJECT_ROOT.parent)]

    class _DagsterBase:
        pass

    def _decorator(*_args, **_kwargs):
        def _wrap(fn):
            return fn
        return _wrap

    def _resource(fn=None, **kwargs):
        return fn if fn else _decorator(**kwargs)

    if not hasattr(dagster_module, "IOManager"):
        dagster_module.IOManager = _DagsterBase
    if not hasattr(dagster_module, "InputContext"):
        dagster_module.InputContext = _DagsterBase
    if not hasattr(dagster_module, "OutputContext"):
        dagster_module.OutputContext = _DagsterBase
    if not hasattr(dagster_module, "io_manager"):
        dagster_module.io_manager = _decorator
    if not hasattr(dagster_module, "resource"):
        dagster_module.resource = _resource

    if "pyspark" not in sys.modules and importlib.util.find_spec("pyspark") is None:
        pyspark = types.ModuleType("pyspark")

        class _SparkConf:
            def __init__(self):
                self._settings = {}

            def setAppName(self, app_name):
                self._settings["spark.app.name"] = app_name
                return self

            def setMaster(self, master):
                self._settings["spark.master"] = master
                return self

            def set(self, key, value):
                self._settings[key] = str(value)
                return self

            def get(self, key, default=None):
                return self._settings.get(key, default)

        class _StorageLevel:
            MEMORY_AND_DISK = object()

        pyspark.SparkConf = _SparkConf
        pyspark.StorageLevel = _StorageLevel

        pyspark_sql = types.ModuleType("pyspark.sql")

        class _DataFrame:
            pass

        class _SparkSession:
            pass

        pyspark_sql.DataFrame = _DataFrame
        pyspark_sql.SparkSession = _SparkSession

        pyspark_functions = types.ModuleType("pyspark.sql.functions")

        def _function(*_args, **_kwargs):
            return None

        pyspark_functions.col = _function
        pyspark_functions.lit = _function
        pyspark_functions.max = _function
        pyspark_functions.upper = _function

        pyspark_types = types.ModuleType("pyspark.sql.types")

        class _Type:
            def __init__(self, *_args, **_kwargs):
                pass

        pyspark_types.BinaryType = _Type
        pyspark_types.DoubleType = _Type
        pyspark_types.StringType = _Type
        pyspark_types.StructField = _Type
        pyspark_types.StructType = _Type
        pyspark_types.TimestampType = _Type

        sys.modules["pyspark"] = pyspark
        sys.modules["pyspark.sql"] = pyspark_sql
        sys.modules["pyspark.sql.functions"] = pyspark_functions
        sys.modules["pyspark.sql.types"] = pyspark_types


def _load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


_install_optional_stubs()
spark_module = _load_module(
    "projectA_spark_resource",
    PROJECT_ROOT / "src" / "projectA" / "defs" / "resources" / "spark.py",
)
spark_df_io_manager_module = _load_module(
    "projectA_spark_df_io_manager",
    PROJECT_ROOT
    / "src"
    / "projectA"
    / "defs"
    / "resources"
    / "spark_df_io_manager.py",
)
HADOOP_AWS_PACKAGE = spark_module.HADOOP_AWS_PACKAGE
MSSQL_JDBC_PACKAGE = spark_module.MSSQL_JDBC_PACKAGE
MYSQL_JDBC_PACKAGE = spark_module.MYSQL_JDBC_PACKAGE
build_executor_cores = spark_module._build_executor_cores
build_spark_master_status_url = spark_module._build_spark_master_status_url
build_spark_resource_profile = spark_module._build_spark_resource_profile
build_spark_retry_conf = spark_module._build_spark_retry_conf
build_s3a_tuning_conf = spark_module._build_s3a_tuning_conf
spark_master_available_resources = spark_module._spark_master_available_resources
SparkResource = spark_module.SparkResource
SparkDataFrameParquetIOManager = (
    spark_df_io_manager_module.SparkDataFrameParquetIOManager
)


class _Context:
    def get_identifier(self):
        return ["run-id", "op-name", "result"]


class _FakeDataFrame:
    def __init__(self):
        self.repartition_calls = []

    def repartition(self, partitions):
        self.repartition_calls.append(partitions)
        return self


def _spark_resource(
        ozone_s3_endpoint: str = "",
        adls_account_name: str = "",
        adls_account_key: str | None = None,
        master_url: str = "local[1]",
        executor_mem: str = "1G",
        executor_cores: int = 1,
        app_name: str = "spark-ozone-config-test",
        job_type: str = "default",
        **resource_kwargs) -> SparkResource:
    return SparkResource(
        master_url=master_url,
        worker_mem="1G",
        executor_mem=executor_mem,
        executor_cores=executor_cores,
        app_name=app_name,
        job_type=job_type,
        adls_account_name=adls_account_name,
        adls_container_name="container",
        adls_base_path="base",
        adls_account_key=adls_account_key,
        ozone_s3_endpoint=ozone_s3_endpoint,
        ozone_s3_region="us-east-1",
        ozone_s3_access_key="access",
        ozone_s3_secret_key="secret",
        ozone_s3_path_style_access="true",
        ozone_s3_bucket_probe="0",
        ozone_s3_change_detection_mode="none",
        jdbc_host="localhost",
        jdbc_port=1433,
        jdbc_database="db",
        jdbc_user="user",
        jdbc_driver="driver",
        jdbc_password="password",
        jdbc_write_num_chunks=1,
        jdbc_write_target_partitions=1,
        jdbc_write_batch_size=1,
        **resource_kwargs,
    )


class SparkOzoneConfigTests(unittest.TestCase):
    def test_ozone_s3_options_are_empty_without_endpoint(self) -> None:
        spark_res = _spark_resource()

        self.assertEqual(spark_res._build_ozone_s3_options(), {})

    def test_ozone_s3_options_include_s3a_connection_settings(self) -> None:
        spark_res = _spark_resource("http://ozone-01:9878")

        options = spark_res._build_ozone_s3_options()

        self.assertEqual(options["fs.s3a.endpoint"], "http://ozone-01:9878")
        self.assertEqual(options["fs.s3a.path.style.access"], "true")
        self.assertEqual(options["fs.s3a.bucket.probe"], "0")
        self.assertEqual(options["fs.s3a.change.detection.mode"], "none")
        self.assertEqual(options["fs.s3a.connection.ssl.enabled"], "false")
        self.assertEqual(options["fs.s3a.fast.upload.buffer"], "disk")
        self.assertEqual(options["fs.s3a.multipart.size"], "128M")
        self.assertEqual(options["fs.s3a.access.key"], "access")
        self.assertEqual(options["fs.s3a.secret.key"], "secret")

    def test_s3a_tuning_uses_defaults_and_env_overrides(self) -> None:
        options = build_s3a_tuning_conf(
            {
                "SPARK_S3A_MULTIPART_SIZE": "64M",
                "SPARK_S3A_FAST_UPLOAD_BUFFER": "bytebuffer",
            }
        )

        self.assertEqual(options["fs.s3a.connection.maximum"], "64")
        self.assertEqual(options["fs.s3a.multipart.size"], "64M")
        self.assertEqual(options["fs.s3a.fast.upload.buffer"], "bytebuffer")

    def test_spark_df_io_manager_repartitions_remote_writes(self) -> None:
        manager = SparkDataFrameParquetIOManager(
            "s3a://readings-etl/dagster_io/spark_dfs/projectA",
            write_partitions=4,
        )
        df = _FakeDataFrame()

        self.assertIs(manager._prepare_for_write(df), df)
        self.assertEqual(df.repartition_calls, [4])

    def test_spark_df_io_manager_keeps_local_writes_unchanged(self) -> None:
        manager = SparkDataFrameParquetIOManager(
            "/data/dagster_io/spark_dfs/projectA",
            write_partitions=4,
        )
        df = _FakeDataFrame()

        self.assertIs(manager._prepare_for_write(df), df)
        self.assertEqual(df.repartition_calls, [])

    def test_spark_packages_preserve_adls_jdbc_without_dynamic_hadoop_aws(self) -> None:
        spark_res = _spark_resource(
            ozone_s3_endpoint="http://ozone-01:9878",
            adls_account_name="proddatalake",
            adls_account_key="adls-key",
        )

        packages = spark_res._spark_packages()

        self.assertIn(MSSQL_JDBC_PACKAGE, packages)
        self.assertNotIn(HADOOP_AWS_PACKAGE, packages)

    def test_spark_df_io_manager_joins_remote_s3a_paths(self) -> None:
        manager = SparkDataFrameParquetIOManager(
            "s3a://sampleOrganization-etl/dagster_io/spark_dfs/sampleOrganization"
        )

        path = manager._path_for(_Context())

        self.assertEqual(
            path,
            "s3a://sampleOrganization-etl/dagster_io/spark_dfs/sampleOrganization/"
            "run-id/op-name/result",
        )

    def test_spark_master_available_resources_uses_alive_workers_only(self) -> None:
        resources = spark_master_available_resources(
            {
                "workers": [
                    {"state": "ALIVE", "coresFree": 2, "memoryFree": 2048},
                    {"state": "DEAD", "coresFree": 8, "memoryFree": 8192},
                    {"state": "ALIVE", "coresFree": 1, "memoryFree": "1g"},
                ]
            }
        )

        self.assertEqual(resources, {"cores_free": 3, "memory_free_mb": 3072})

    def test_spark_master_available_resources_handles_lowercase_keys(self) -> None:
        resources = spark_master_available_resources(
            {
                "workers": [
                    {"state": "ALIVE", "coresfree": 4, "memoryfree": 6144},
                    {"state": "DEAD", "coresfree": 2, "memoryfree": 2048},
                    {"state": "ALIVE", "coresfree": 4, "memoryfree": 6144},
                ]
            }
        )

        self.assertEqual(resources, {"cores_free": 8, "memory_free_mb": 12288})

    def test_spark_resource_profile_uses_job_type_overrides(self) -> None:
        profile = build_spark_resource_profile(
            "lecturas",
            {
                "SPARK_LECTURAS_MIN_CORES": "3",
                "SPARK_LECTURAS_MIN_MEMORY_MB": "4096",
                "SPARK_LECTURAS_CORES_MAX": "4",
            },
        )

        self.assertEqual(profile["name"], "lecturas")
        self.assertEqual(profile["min_cores"], 3)
        self.assertEqual(profile["min_memory_mb"], 4096)
        self.assertEqual(profile["cores_max"], 4)


    def test_observability_profile_uses_executor_sized_defaults(self) -> None:
        profile = build_spark_resource_profile("observability", {})

        self.assertEqual(profile["name"], "observability")
        self.assertEqual(profile["min_cores"], 2)
        self.assertEqual(profile["min_memory_mb"], 2048)
        self.assertEqual(profile["cores_max"], 2)


    def test_mysql_jdbc_package_is_included_when_configured(self) -> None:
        spark_res = _spark_resource(mysql_jdbc_package=MYSQL_JDBC_PACKAGE)

        self.assertIn(MYSQL_JDBC_PACKAGE, spark_res._spark_packages())

    def test_spark_retry_policy_uses_defaults_and_env_overrides(self) -> None:
        policy = build_spark_retry_conf(
            {
                "SPARK_TASK_MAX_FAILURES": "6",
                "SPARK_SHUFFLE_IO_RETRY_WAIT": "20s",
            }
        )

        self.assertEqual(policy["spark.task.maxFailures"], "6")
        self.assertEqual(policy["spark.stage.maxConsecutiveAttempts"], "8")
        self.assertEqual(policy["spark.shuffle.io.retryWait"], "20s")

    def test_spark_master_status_url_derives_from_external_master_url(self) -> None:
        status_url = build_spark_master_status_url(
            "spark://172.168.10.80:7077",
            {
                "SPARK_MASTER_STATUS_URL": "http://spark-master:8080/json/",
                "SPARK_MASTER_UI_PORT": "8081",
            },
        )

        self.assertEqual(status_url, "http://172.168.10.80:8081/json/")

    def test_spark_master_status_url_keeps_internal_compose_port(self) -> None:
        status_url = build_spark_master_status_url(
            "spark://spark-master:7077",
            {"SPARK_MASTER_UI_PORT": "8081"},
        )

        self.assertEqual(status_url, "http://spark-master:8080/json/")

    def test_legacy_worker_cores_fallback_does_not_exceed_profile_cap(self) -> None:
        profile = build_spark_resource_profile("lecturas", {})

        executor_cores = build_executor_cores(
            profile,
            {"SPARK_WORKER_CORES": "4"},
        )

        self.assertEqual(executor_cores, 2)

    def test_spark_app_context_uses_dagster_run_tags(self) -> None:
        class _Run:
            job_name = "010_Sensor_lecturas_ingestion"
            run_id = "abcdef123456"
            tags = {"user_code": "projectA", "job_type": "lecturas"}

        class _InitContext:
            dagster_run = _Run()

        context = spark_module._build_spark_app_context(
            _InitContext(),
            default_app_name="FactLecturasIngestion",
            default_user_code="projectA",
        )

        self.assertEqual(context["job_type"], "lecturas")
        self.assertEqual(
            context["app_name"],
            "projectA/010_Sensor_lecturas_ingestion/abcdef12",
        )

    def test_spark_app_context_infers_maintenance_job_type_from_job_name(self) -> None:
        class _Run:
            job_name = "089_maintenance_spark_healthcheck"
            run_id = "abcdef123456"
            tags = {"user_code": "projectA"}

        class _InitContext:
            dagster_run = _Run()

        context = spark_module._build_spark_app_context(
            _InitContext(),
            default_app_name="FactLecturasIngestion",
            default_user_code="projectA",
        )

        self.assertEqual(context["job_type"], "maintenance")

    def test_wait_for_spark_capacity_fails_when_cluster_is_full(self) -> None:
        spark_res = _spark_resource(
            master_url="spark://spark-master:7077",
            job_type="lecturas",
            resource_wait_seconds=0,
        )
        spark_res._fetch_spark_master_status = lambda: {
            "workers": [{"state": "ALIVE", "coresFree": 1, "memoryFree": 4096}]
        }

        with self.assertRaisesRegex(RuntimeError, "Insufficient Spark resources"):
            spark_res._wait_for_spark_capacity()

    def test_spark_conf_sets_resource_limits_and_app_name(self) -> None:
        spark_res = _spark_resource(
            master_url="spark://spark-master:7077",
            executor_mem="2G",
            executor_cores=1,
            app_name="projectA/010_Sensor_lecturas_ingestion/abcdef12",
            job_type="lecturas",
            driver_host="10.0.0.5",
            driver_port="17001",
            driver_block_manager_port="17002",
            block_manager_port="17101",
        )

        conf = spark_res._build_spark_conf()

        self.assertEqual(
            conf.get("spark.app.name"),
            "projectA/010_Sensor_lecturas_ingestion/abcdef12",
        )
        self.assertEqual(conf.get("spark.executor.memory"), "2G")
        self.assertEqual(conf.get("spark.executor.cores"), "1")
        self.assertEqual(conf.get("spark.cores.max"), "2")
        self.assertEqual(conf.get("spark.driver.host"), "10.0.0.5")
        self.assertEqual(conf.get("spark.driver.bindAddress"), "0.0.0.0")
        self.assertEqual(conf.get("spark.driver.port"), "17001")
        self.assertEqual(conf.get("spark.driver.blockManager.port"), "17002")
        self.assertEqual(conf.get("spark.blockManager.port"), "17101")
        self.assertEqual(conf.get("spark.task.maxFailures"), "8")
        self.assertEqual(conf.get("spark.shuffle.io.maxRetries"), "10")
        self.assertEqual(conf.get("spark.network.timeout"), "800s")
        self.assertEqual(conf.get("spark.sql.maxPlanStringLength"), "20000")
        self.assertEqual(conf.get("spark.sql.debug.maxToStringFields"), "100")

    def test_wait_for_spark_capacity_fails_when_worker_network_is_unreachable(self) -> None:
        spark_res = _spark_resource(
            master_url="spark://spark-master:7077",
            job_type="lecturas",
            block_manager_port="37179",
        )
        spark_res._fetch_spark_master_status = lambda: {
            "workers": [
                {
                    "state": "ALIVE",
                    "host": "172.21.0.4",
                    "coresFree": 2,
                    "memoryFree": 4096,
                }
            ]
        }
        spark_res._worker_endpoint_is_routable = lambda host, port: False

        with self.assertRaisesRegex(RuntimeError, "block manager endpoints"):
            spark_res._wait_for_spark_capacity()

    def test_validate_port_assignments_raises_on_collision(self) -> None:
        spark_res = _spark_resource(
            master_url="spark://spark-master:7077",
            driver_port="37079",
            driver_block_manager_port="37078",
            block_manager_port="37079",
        )
        with self.assertRaisesRegex(RuntimeError, "port conflict"):
            spark_res._validate_port_assignments()

    def test_validate_port_assignments_passes_with_distinct_ports(self) -> None:
        spark_res = _spark_resource(
            master_url="spark://spark-master:7077",
            driver_port="37077",
            driver_block_manager_port="37078",
            block_manager_port="37079",
        )
        spark_res._validate_port_assignments()


if __name__ == "__main__":
    unittest.main()
