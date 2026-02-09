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

IF OBJECT_ID(N'dwh.dimSidonProdDimensions', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.dimSidonProdDimensions
        (
            SensorId UNIQUEIDENTIFIER NOT NULL,
            SensorTyId UNIQUEIDENTIFIER NOT NULL,
            DeviceId UNIQUEIDENTIFIER NOT NULL,
            _sensor_name NVARCHAR(255) NULL,
            _sensor_type NVARCHAR(255) NULL,
            DeviceTyId UNIQUEIDENTIFIER NOT NULL,
            SubLocationId UNIQUEIDENTIFIER NOT NULL,
            _device_name NVARCHAR(255) NULL,
            _device_type NVARCHAR(255) NULL,
            SubLocationTyId UNIQUEIDENTIFIER NOT NULL,
            _sublocation_name NVARCHAR(255) NULL,
            LocationId UNIQUEIDENTIFIER NOT NULL,
            _sublocation_type NVARCHAR(255) NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_dimSidonProdDimensions_ModifiedAt
            DEFAULT (SYSUTCDATETIME())
        );
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
            AND o.name = N'dimSidonProdDimensions'
            AND i.name = N'ix_dimSidonProdDimensions_SensorId'
    )
    BEGIN
        CREATE UNIQUE INDEX ix_dimSidonProdDimensions_SensorId
            ON dwh.dimSidonProdDimensions (SensorId)
            WITH (IGNORE_DUP_KEY = ON);
    END;
GO
