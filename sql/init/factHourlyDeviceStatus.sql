SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

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

IF OBJECT_ID(N'dwh.factHourlyDeviceStatus', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.factHourlyDeviceStatus
        (
            OperationId UNIQUEIDENTIFIER NOT NULL,
            DeviceId UNIQUEIDENTIFIER NOT NULL,
            LocalTimeSpan DATETIME2(0) NOT NULL,
            StatusCode SMALLINT NOT NULL,
            Anomalies INT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factHourlyDeviceStatus_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            Fecha AS CONVERT(DATE, LocalTimeSpan) PERSISTED,
            Hora AS CONVERT(TIME(0), LocalTimeSpan) PERSISTED,
            CONSTRAINT PK_FactHourlyDeviceStatus
            PRIMARY KEY CLUSTERED (OperationId)
            WITH (IGNORE_DUP_KEY = ON)
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
            AND o.name = N'factHourlyDeviceStatus'
            AND i.name = N'ix_factDeviceStatusEvent_time_span_deviceid'
    )
    BEGIN
        CREATE INDEX ix_factDeviceStatusEvent_time_span_deviceid
            ON dwh.factHourlyDeviceStatus (LocalTimeSpan, DeviceId);
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
            AND o.name = N'factHourlyDeviceStatus'
            AND i.name = N'ix_factDeviceStatusEvent_localTimeSpan_desc'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX ix_factDeviceStatusEvent_localTimeSpan_desc
            ON dwh.factHourlyDeviceStatus (LocalTimeSpan DESC);
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
            AND o.name = N'factHourlyDeviceStatus'
            AND i.name = N'UX_factHourlyDeviceStatus_Device_Time'
    )
    BEGIN
        CREATE UNIQUE INDEX UX_factHourlyDeviceStatus_Device_Time
            ON dwh.factHourlyDeviceStatus (DeviceId, LocalTimeSpan)
            WITH (IGNORE_DUP_KEY = ON);
    END;
GO
