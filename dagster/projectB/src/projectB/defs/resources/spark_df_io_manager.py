from __future__ import annotations

import os
from typing import Optional
from dagster import IOManager, InputContext, OutputContext, io_manager

DEFAULT_SPARK_DF_IO_BASE_PATH = "/data/dagster_io/spark_dfs"
DEFAULT_SPARK_DF_IO_WRITE_PARTITIONS = 2
SPARK_DF_IO_BASE_PATH_ENV = "SPARK_DF_IO_BASE_PATH"
SPARK_DF_IO_WRITE_PARTITIONS_ENV = "SPARK_DF_IO_WRITE_PARTITIONS"


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


class SparkDataFrameParquetIOManager(IOManager):
    def __init__(
        self,
        base_path: str,
        write_partitions: int = DEFAULT_SPARK_DF_IO_WRITE_PARTITIONS,
    ):
        self._base_path = base_path
        self._write_partitions = write_partitions

    def _is_remote_path(self) -> bool:
        return "://" in self._base_path

    def _join_remote_path(self, ident) -> str:
        parts = [str(part).strip("/") for part in ident]
        return "/".join([self._base_path.rstrip("/"), *parts])

    def _path_for(self, context: Optional[OutputContext | InputContext] = None) -> str:
        ident = context.get_identifier()
        if self._is_remote_path():
            return self._join_remote_path(ident)

        return os.path.join(self._base_path, *ident)

    def _prepare_for_write(self, obj):
        if self._is_remote_path() and self._write_partitions > 0:
            return obj.repartition(self._write_partitions)
        return obj

    def handle_output(self, context: OutputContext, obj):
        if obj is None:
            return

        path = self._path_for(context)

        if not self._is_remote_path():
            os.makedirs(os.path.dirname(path), exist_ok=True)

        self._prepare_for_write(obj).write.mode("overwrite").parquet(path)

        context.add_output_metadata({"parquet_path": path})

    def load_input(self, context: InputContext):
        upstream = context.upstream_output
        if upstream is None:
            raise RuntimeError("Missing upstream output for Spark DataFrame input.")

        path = self._path_for(upstream)
        spark = context.resources.spark.get_spark()
        return spark.read.parquet(path)


@io_manager(required_resource_keys={"spark"})
def spark_df_io_manager():
    base_path = os.getenv(
        SPARK_DF_IO_BASE_PATH_ENV,
        DEFAULT_SPARK_DF_IO_BASE_PATH,
    ) or DEFAULT_SPARK_DF_IO_BASE_PATH
    write_partitions = _env_int(
        SPARK_DF_IO_WRITE_PARTITIONS_ENV,
        DEFAULT_SPARK_DF_IO_WRITE_PARTITIONS,
    )
    return SparkDataFrameParquetIOManager(
        base_path=base_path,
        write_partitions=write_partitions,
    )
