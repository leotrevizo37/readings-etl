IF
    NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'dwh'
    )
    BEGIN
        EXEC ('CREATE SCHEMA dwh');
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'stg'
    )
    BEGIN
        EXEC ('CREATE SCHEMA stg');
    END;
GO

IF OBJECT_ID(N'dwh.dimTelemetryProdDimensions', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.dimTelemetryProdDimensions
        (
            SensorId UNIQUEIDENTIFIER NOT NULL,
            SensorTyId UNIQUEIDENTIFIER NOT NULL,
            DeviceId UNIQUEIDENTIFIER NOT NULL,
            ModelId UNIQUEIDENTIFIER NULL,
            SensorHubId UNIQUEIDENTIFIER NULL,
            PhysicalIdentifier NVARCHAR(255) NULL,
            PerEvent BIT NULL,
            sensor_name NVARCHAR(255) NULL,
            sensor_type NVARCHAR(255) NULL,
            model_name NVARCHAR(255) NULL,
            hub_model_name NVARCHAR(255) NULL,
            hub_name NVARCHAR(255) NULL,
            DeviceTyId UNIQUEIDENTIFIER NOT NULL,
            SubLocationId UNIQUEIDENTIFIER NOT NULL,
            Active BIT NOT NULL,
            device_name NVARCHAR(255) NULL,
            device_type NVARCHAR(255) NULL,
            SubLocationTyId UNIQUEIDENTIFIER NOT NULL,
            sublocation_name NVARCHAR(255) NULL,
            location_name NVARCHAR(255) NULL,
            city_name NVARCHAR(255) NULL,
            CityId UNIQUEIDENTIFIER NOT NULL,
            TimeZone NVARCHAR(255) NOT NULL,
            LocationId UNIQUEIDENTIFIER NOT NULL,
            sublocation_type NVARCHAR(255) NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_dimTelemetryProdDimensions_ModifiedAt
            DEFAULT (SYSUTCDATETIME())
        );
    END;
GO

IF
    OBJECT_ID(N'dwh.dimTelemetryProdDimensions', N'U') IS NOT NULL
    AND COL_LENGTH(N'dwh.dimTelemetryProdDimensions', N'ModelId') IS NULL
    BEGIN
        ALTER TABLE dwh.dimTelemetryProdDimensions
        ADD ModelId UNIQUEIDENTIFIER NULL;
    END;
GO

IF
    OBJECT_ID(N'dwh.dimTelemetryProdDimensions', N'U') IS NOT NULL
    AND COL_LENGTH(N'dwh.dimTelemetryProdDimensions', N'SensorHubId') IS NULL
    BEGIN
        ALTER TABLE dwh.dimTelemetryProdDimensions
        ADD SensorHubId UNIQUEIDENTIFIER NULL;
    END;
GO

IF
    OBJECT_ID(N'dwh.dimTelemetryProdDimensions', N'U') IS NOT NULL
    AND COL_LENGTH(N'dwh.dimTelemetryProdDimensions', N'PhysicalIdentifier') IS NULL
    BEGIN
        ALTER TABLE dwh.dimTelemetryProdDimensions
        ADD PhysicalIdentifier NVARCHAR(255) NULL;
    END;
GO

IF
    OBJECT_ID(N'dwh.dimTelemetryProdDimensions', N'U') IS NOT NULL
    AND COL_LENGTH(N'dwh.dimTelemetryProdDimensions', N'PerEvent') IS NULL
    BEGIN
        ALTER TABLE dwh.dimTelemetryProdDimensions
        ADD PerEvent BIT NULL;
    END;
GO

IF
    OBJECT_ID(N'dwh.dimTelemetryProdDimensions', N'U') IS NOT NULL
    AND COL_LENGTH(N'dwh.dimTelemetryProdDimensions', N'model_name') IS NULL
    BEGIN
        ALTER TABLE dwh.dimTelemetryProdDimensions
        ADD model_name NVARCHAR(255) NULL;
    END;
GO

IF
    OBJECT_ID(N'dwh.dimTelemetryProdDimensions', N'U') IS NOT NULL
    AND COL_LENGTH(N'dwh.dimTelemetryProdDimensions', N'hub_model_name') IS NULL
    BEGIN
        ALTER TABLE dwh.dimTelemetryProdDimensions
        ADD hub_model_name NVARCHAR(255) NULL;
    END;
GO

IF
    OBJECT_ID(N'dwh.dimTelemetryProdDimensions', N'U') IS NOT NULL
    AND COL_LENGTH(N'dwh.dimTelemetryProdDimensions', N'hub_name') IS NULL
    BEGIN
        ALTER TABLE dwh.dimTelemetryProdDimensions
        ADD hub_name NVARCHAR(255) NULL;
    END;
GO

IF
    OBJECT_ID(N'dwh.dimTelemetryProdDimensions', N'U') IS NOT NULL
    AND COL_LENGTH(N'dwh.dimTelemetryProdDimensions', N'TimeZone') IS NULL
    BEGIN
        ALTER TABLE dwh.dimTelemetryProdDimensions
        ADD TimeZone NVARCHAR(255) NULL;
    END;
GO

IF OBJECT_ID(N'stg.dimTelemetryProdDimensions', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.dimTelemetryProdDimensions
        (
            RunId NVARCHAR(100) NOT NULL,
            SensorId UNIQUEIDENTIFIER NOT NULL,
            SensorTyId UNIQUEIDENTIFIER NOT NULL,
            DeviceId UNIQUEIDENTIFIER NOT NULL,
            ModelId UNIQUEIDENTIFIER NULL,
            SensorHubId UNIQUEIDENTIFIER NULL,
            PhysicalIdentifier NVARCHAR(255) NULL,
            PerEvent BIT NULL,
            sensor_name NVARCHAR(255) NULL,
            sensor_type NVARCHAR(255) NULL,
            model_name NVARCHAR(255) NULL,
            hub_model_name NVARCHAR(255) NULL,
            hub_name NVARCHAR(255) NULL,
            DeviceTyId UNIQUEIDENTIFIER NOT NULL,
            SubLocationId UNIQUEIDENTIFIER NOT NULL,
            Active BIT NOT NULL,
            device_name NVARCHAR(255) NULL,
            device_type NVARCHAR(255) NULL,
            SubLocationTyId UNIQUEIDENTIFIER NOT NULL,
            sublocation_name NVARCHAR(255) NULL,
            location_name NVARCHAR(255) NULL,
            city_name NVARCHAR(255) NULL,
            CityId UNIQUEIDENTIFIER NOT NULL,
            TimeZone NVARCHAR(255) NOT NULL,
            LocationId UNIQUEIDENTIFIER NOT NULL,
            sublocation_type NVARCHAR(255) NULL
        );
    END;
GO

IF
    OBJECT_ID(N'stg.dimTelemetryProdDimensions', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.dimTelemetryProdDimensions', N'ModelId') IS NULL
    BEGIN
        ALTER TABLE stg.dimTelemetryProdDimensions
        ADD ModelId UNIQUEIDENTIFIER NULL;
    END;
GO

IF
    OBJECT_ID(N'stg.dimTelemetryProdDimensions', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.dimTelemetryProdDimensions', N'SensorHubId') IS NULL
    BEGIN
        ALTER TABLE stg.dimTelemetryProdDimensions
        ADD SensorHubId UNIQUEIDENTIFIER NULL;
    END;
GO

IF
    OBJECT_ID(N'stg.dimTelemetryProdDimensions', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.dimTelemetryProdDimensions', N'PhysicalIdentifier') IS NULL
    BEGIN
        ALTER TABLE stg.dimTelemetryProdDimensions
        ADD PhysicalIdentifier NVARCHAR(255) NULL;
    END;
GO

IF
    OBJECT_ID(N'stg.dimTelemetryProdDimensions', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.dimTelemetryProdDimensions', N'PerEvent') IS NULL
    BEGIN
        ALTER TABLE stg.dimTelemetryProdDimensions
        ADD PerEvent BIT NULL;
    END;
GO

IF
    OBJECT_ID(N'stg.dimTelemetryProdDimensions', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.dimTelemetryProdDimensions', N'model_name') IS NULL
    BEGIN
        ALTER TABLE stg.dimTelemetryProdDimensions
        ADD model_name NVARCHAR(255) NULL;
    END;
GO

IF
    OBJECT_ID(N'stg.dimTelemetryProdDimensions', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.dimTelemetryProdDimensions', N'hub_model_name') IS NULL
    BEGIN
        ALTER TABLE stg.dimTelemetryProdDimensions
        ADD hub_model_name NVARCHAR(255) NULL;
    END;
GO

IF
    OBJECT_ID(N'stg.dimTelemetryProdDimensions', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.dimTelemetryProdDimensions', N'hub_name') IS NULL
    BEGIN
        ALTER TABLE stg.dimTelemetryProdDimensions
        ADD hub_name NVARCHAR(255) NULL;
    END;
GO

IF
    OBJECT_ID(N'stg.dimTelemetryProdDimensions', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.dimTelemetryProdDimensions', N'TimeZone') IS NULL
    BEGIN
        ALTER TABLE stg.dimTelemetryProdDimensions
        ADD TimeZone NVARCHAR(255) NULL;
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes AS i
        INNER JOIN sys.objects AS o
            ON i.object_id = o.object_id
        INNER JOIN sys.schemas AS s
            ON o.schema_id = s.schema_iD
        WHERE
            s.name = N'dwh'
            AND o.name = N'dimTelemetryProdDimensions'
            AND i.name = N'ix_dimTelemetryProdDimensions_SensorId'
    )
    BEGIN
        CREATE UNIQUE INDEX ix_dimTelemetryProdDimensions_SensorId
            ON dwh.dimTelemetryProdDimensions (SensorId)
            WITH (IGNORE_DUP_KEY = ON);
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes AS i
        INNER JOIN sys.objects AS o
            ON i.object_id = o.object_id
        INNER JOIN sys.schemas AS s
            ON o.schema_id = s.schema_iD
        WHERE
            s.name = N'stg'
            AND o.name = N'dimTelemetryProdDimensions'
            AND i.name = N'ix_stg_dimTelemetryProdDimensions_run'
    )
    BEGIN
        CREATE INDEX ix_stg_dimTelemetryProdDimensions_run
            ON stg.dimTelemetryProdDimensions (RunId, SensorId);
    END;
GO
