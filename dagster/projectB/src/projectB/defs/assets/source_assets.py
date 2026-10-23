from dagster import SourceAsset, AssetKey


dbo_sensors = SourceAsset(
    key=AssetKey(["projectB", "dbo_sensors"]),
    group_name="projectB",
)
dbo_sensor_tys = SourceAsset(
    key=AssetKey(["projectB", "dbo_sensor_tys"]),
    group_name="projectB",
)
dbo_devices = SourceAsset(
    key=AssetKey(["projectB", "dbo_devices"]),
    group_name="projectB",
)
dbo_device_tys = SourceAsset(
    key=AssetKey(["projectB", "dbo_device_tys"]),
    group_name="projectB",
)
dbo_sublocations = SourceAsset(
    key=AssetKey(["projectB", "dbo_sublocations"]),
    group_name="projectB",
)
dbo_sublocation_tys = SourceAsset(
    key=AssetKey(["projectB", "dbo_sublocation_tys"]),
    group_name="projectB",
)
dbo_aspnetusers = SourceAsset(
    key=AssetKey(["projectB", "dbo_aspnetusers"]),
    group_name="projectB",
)
dbo_systemlogs = SourceAsset(
    key=AssetKey(["projectB", "dbo_systemlogs"]),
    group_name="projectB",
)
