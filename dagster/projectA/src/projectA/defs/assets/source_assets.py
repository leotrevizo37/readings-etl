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
dbo_ProgramProfileTys = SourceAsset(
    key=AssetKey(["projectA", "dbo_ProgramProfileTys"]),
    group_name="projectA",
)
dbo_ProgramProfiles = SourceAsset(
    key=AssetKey(["projectA", "dbo_ProgramProfiles"]),
    group_name="projectA",
)
dbo_ProgramSchedules = SourceAsset(
    key=AssetKey(["projectA", "dbo_ProgramSchedules"]),
    group_name="projectA",
)
dbo_GroupSensors = SourceAsset(
    key=AssetKey(["projectA", "dbo_GroupSensors"]),
    group_name="projectA",
)
dbo_GroupHasSchedules = SourceAsset(
    key=AssetKey(["projectA", "dbo_GroupHasSchedules"]),
    group_name="projectA",
)
dbo_GroupHasSensors = SourceAsset(
    key=AssetKey(["projectA", "dbo_GroupHasSensors"]),
    group_name="projectA",
)
