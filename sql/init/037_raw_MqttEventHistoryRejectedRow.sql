IF
    NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'raw'
    )
    BEGIN
        EXEC ('CREATE SCHEMA raw');
    END;
GO

IF OBJECT_ID(N'raw.MqttEventHistoryRejectedRow', N'U') IS NULL
    BEGIN
        CREATE TABLE raw.MqttEventHistoryRejectedRow
        (
            RejectedRowId BIGINT IDENTITY (1, 1) NOT NULL,
            RunId NVARCHAR(100) NOT NULL,
            Tenant NVARCHAR(100) NOT NULL,
            FechaInicio NVARCHAR(40) NOT NULL,
            FechaFin NVARCHAR(40) NOT NULL,
            RequestedLimit INT NOT NULL,
            FailureReason NVARCHAR(1000) NOT NULL,
            RowJson NVARCHAR(4000) NOT NULL,
            FetchedAt DATETIME2(6) NOT NULL,
            CreatedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_MqttEventHistoryRejectedRow_CreatedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_MqttEventHistoryRejectedRow
            PRIMARY KEY CLUSTERED (RejectedRowId)
        );
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            object_id = OBJECT_ID(N'raw.MqttEventHistoryRejectedRow')
            AND name = N'ix_MqttEventHistoryRejectedRow_createdAt'
    )
    BEGIN
        CREATE INDEX ix_MqttEventHistoryRejectedRow_createdAt
            ON raw.MqttEventHistoryRejectedRow (CreatedAt);
    END;
GO
