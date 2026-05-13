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
dbo_ProgramProfileTys = SourceAsset(
    key=AssetKey(["projectB", "dbo_ProgramProfileTys"]),
    group_name="projectB",
)
dbo_ProgramProfiles = SourceAsset(
    key=AssetKey(["projectB", "dbo_ProgramProfiles"]),
    group_name="projectB",
)
dbo_ProgramSchedules = SourceAsset(
    key=AssetKey(["projectB", "dbo_ProgramSchedules"]),
    group_name="projectB",
)
dbo_GroupSensors = SourceAsset(
    key=AssetKey(["projectB", "dbo_GroupSensors"]),
    group_name="projectB",
)
dbo_GroupHasSchedules = SourceAsset(
    key=AssetKey(["projectB", "dbo_GroupHasSchedules"]),
    group_name="projectB",
)
dbo_GroupHasSensors = SourceAsset(
    key=AssetKey(["projectB", "dbo_GroupHasSensors"]),
    group_name="projectB",
)
