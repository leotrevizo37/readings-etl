from dagster import SourceAsset, AssetKey


dbo_sensors = SourceAsset(
    key=AssetKey(["projectD", "dbo_sensors"]),
    group_name="projectD",
)
dbo_sensor_tys = SourceAsset(
    key=AssetKey(["projectD", "dbo_sensor_tys"]),
    group_name="projectD",
)
dbo_devices = SourceAsset(
    key=AssetKey(["projectD", "dbo_devices"]),
    group_name="projectD",
)
dbo_device_tys = SourceAsset(
    key=AssetKey(["projectD", "dbo_device_tys"]),
    group_name="projectD",
)
dbo_sublocations = SourceAsset(
    key=AssetKey(["projectD", "dbo_sublocations"]),
    group_name="projectD",
)
dbo_sublocation_tys = SourceAsset(
    key=AssetKey(["projectD", "dbo_sublocation_tys"]),
    group_name="projectD",
)
dbo_aspnetusers = SourceAsset(
    key=AssetKey(["projectD", "dbo_aspnetusers"]),
    group_name="projectD",
)
dbo_systemlogs = SourceAsset(
    key=AssetKey(["projectD", "dbo_systemlogs"]),
    group_name="projectD",
)
