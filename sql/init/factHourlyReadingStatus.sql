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

IF OBJECT_ID(N'dwh.factHourlyReadingStatus', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.factHourlyReadingStatus
        (
            OperationId UNIQUEIDENTIFIER NOT NULL,
            SensorTyId UNIQUEIDENTIFIER NOT NULL,
            StatusCode SMALLINT NOT NULL,
            MeasurementValue DECIMAL(9, 4) NULL,
            Anomalies INT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factHourlyReadingStatus_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_factHourlyReadingStatus
            PRIMARY KEY CLUSTERED (OperationId, SensorTyId)
            WITH (IGNORE_DUP_KEY = ON), -- noqa: PRS
            CONSTRAINT FK_factHourlyReadingStatus_factHourlyDeviceStatus
            FOREIGN KEY (OperationId)
            REFERENCES dwh.factHourlyDeviceStatus (OperationId)
            ON DELETE CASCADE
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
            AND o.name = N'factHourlyReadingStatus'
            AND i.name = N'ix_factHourlyReadingStatus_OperationId_SensorTyId'
    )
    BEGIN
        CREATE INDEX ix_factHourlyReadingStatus_OperationId_SensorTyId
            ON dwh.factHourlyReadingStatus (OperationId, SensorTyId);
    END;
GO
