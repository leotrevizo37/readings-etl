from dagster import SourceAsset, AssetKey

dbo_sensors = SourceAsset(
    key=AssetKey(["projectC", "dbo_sensors"]),
    group_name="projectC",
)
dbo_sensor_tys = SourceAsset(
    key=AssetKey(["projectC", "dbo_sensor_tys"]),
    group_name="projectC",
)
dbo_devices = SourceAsset(
    key=AssetKey(["projectC", "dbo_devices"]),
    group_name="projectC",
)
dbo_device_tys = SourceAsset(
    key=AssetKey(["projectC", "dbo_device_tys"]),
    group_name="projectC",
)
dbo_sublocations = SourceAsset(
    key=AssetKey(["projectC", "dbo_sublocations"]),
    group_name="projectC",
)
dbo_sublocation_tys = SourceAsset(
    key=AssetKey(["projectC", "dbo_sublocation_tys"]),
    group_name="projectC",
)