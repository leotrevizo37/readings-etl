from __future__ import annotations

import os
from typing import Optional
from dagster import IOManager, InputContext, OutputContext, io_manager


class SparkDataFrameParquetIOManager(IOManager):
    def __init__(self, base_path: str):
        self._base_path = base_path

    def _path_for(self, context: Optional[OutputContext | InputContext] = None) -> str:
        ident = context.get_identifier()
        return os.path.join(self._base_path, *ident)

    def handle_output(self, context: OutputContext, obj):
        if obj is None:
            return

        path = self._path_for(context)

        os.makedirs(os.path.dirname(path), exist_ok=True)

        obj.write.mode("overwrite").parquet(path)

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
    return SparkDataFrameParquetIOManager(base_path="/data/dagster_io/spark_dfs")
