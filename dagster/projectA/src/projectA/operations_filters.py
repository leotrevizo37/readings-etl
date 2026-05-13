from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def limit_operations_rows_to_closed_hours(
    devices_df: DataFrame,
    max_local_hour: datetime,
) -> DataFrame:
    return devices_df.filter(col("LocalTimeSpan") <= max_local_hour)
