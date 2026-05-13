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

IF OBJECT_ID(N'dwh.factReadingsMeasurement', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.factReadingsMeasurement
        (
            OperationId UNIQUEIDENTIFIER NOT NULL,
            SensorId UNIQUEIDENTIFIER NOT NULL,
            SensorTyId UNIQUEIDENTIFIER NOT NULL,
            DeviceId UNIQUEIDENTIFIER NOT NULL,
            LocationId UNIQUEIDENTIFIER NOT NULL,
            DeviceType VARCHAR(255) NOT NULL,
            SensorType VARCHAR(255) NOT NULL,
            LocalTimeSpan DATETIME2(0) NOT NULL,
            MeasurementValue DECIMAL(9, 4) NULL,
            MeasurementStdDev DECIMAL(9, 4) NULL,
            Anomalies INT NULL,
            ReadingsCount INT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factReadingsMeasurement_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_factReadingsMeasurement
            PRIMARY KEY CLUSTERED (OperationId)
            WITH (IGNORE_DUP_KEY = ON) -- noqa: PRS
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
            AND o.name = N'factReadingsMeasurement'
            AND i.name = N'ix_factReadingsMeasurement_OperationId_SensorId'
    )
    BEGIN
        CREATE INDEX ix_factReadingsMeasurement_OperationId_SensorId
            ON dwh.factReadingsMeasurement (OperationId, SensorId);
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
            AND o.name = N'factReadingsMeasurement'
            AND i.name = N'ix_factReadingsMeasurement_OperationId_SensorTyId'
    )
    BEGIN
        CREATE INDEX ix_factReadingsMeasurement_OperationId_SensorTyId
            ON dwh.factReadingsMeasurement (OperationId, SensorTyId);
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
            AND o.name = N'factReadingsMeasurement'
            AND i.name = N'ix_factReadingsMeasurement_timespan_SensorId'
    )
    BEGIN
        CREATE INDEX ix_factReadingsMeasurement_timespan_sensorid
            ON dwh.factReadingsMeasurement (LocalTimeSpan, SensorId);
    END;
GO
