from dagster import SourceAsset, AssetKey

dbo_sensors = SourceAsset(key=AssetKey("dbo_sensors"))
dbo_sensor_tys = SourceAsset(key=AssetKey("dbo_sensor_tys"))
dbo_devices = SourceAsset(key=AssetKey("dbo_devices"))
dbo_device_tys = SourceAsset(key=AssetKey("dbo_device_tys"))
dbo_sublocations = SourceAsset(key=AssetKey("dbo_sublocations"))
dbo_sublocation_tys = SourceAsset(key=AssetKey("dbo_sublocation_tys"))
dwh_readings = SourceAsset(key=AssetKey("dwh_readings"))
