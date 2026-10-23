from dagster import SourceAsset, AssetKey

dbo_sensors = SourceAsset(
    key=AssetKey(["projectA", "dbo_sensors"]),
    group_name="projectA",
)
dbo_sensor_tys = SourceAsset(
    key=AssetKey(["projectA", "dbo_sensor_tys"]),
    group_name="projectA",
)
dbo_devices = SourceAsset(
    key=AssetKey(["projectA", "dbo_devices"]),
    group_name="projectA",
)
dbo_device_tys = SourceAsset(
    key=AssetKey(["projectA", "dbo_device_tys"]),
    group_name="projectA",
)
dbo_sublocations = SourceAsset(
    key=AssetKey(["projectA", "dbo_sublocations"]),
    group_name="projectA",
)
dbo_sublocation_tys = SourceAsset(
    key=AssetKey(["projectA", "dbo_sublocation_tys"]),
    group_name="projectA",
)
dbo_aspnetusers = SourceAsset(
    key=AssetKey(["projectA", "dbo_aspnetusers"]),
    group_name="projectA",
)
dbo_systemlogs = SourceAsset(
    key=AssetKey(["projectA", "dbo_systemlogs"]),
    group_name="projectA",
)
