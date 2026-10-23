IF OBJECT_ID(N'observability.factChirpStackDeviceMetrics', N'U') IS NULL
    BEGIN
        CREATE TABLE observability.factChirpStackDeviceMetrics
        (
            DevEui NVARCHAR(16) NOT NULL,
            TimeSpan DATETIME2(0) NOT NULL,
            TenantId UNIQUEIDENTIFIER NOT NULL,
            ApplicationId UNIQUEIDENTIFIER NOT NULL,
            DeviceProfileId UNIQUEIDENTIFIER NOT NULL,
            UplinkIntervalSeconds INT NULL,
            ExpectedUplinkCount DECIMAL(18, 6) NULL,
            ReceivedUplinkCount BIGINT NULL,
            AverageRssi DECIMAL(18, 6) NULL,
            AverageSnr DECIMAL(18, 6) NULL,
            LinkErrorCount BIGINT NULL,
            LinkErrorCountsJson NVARCHAR(MAX) NULL,
            BatteryLevel DECIMAL(7, 4) NULL,
            ExternalPowerSource BIT NULL,
            BatteryLevelSource NVARCHAR(32) NULL,
            BatteryObservedAt DATETIME2(0) NULL,
            MetricsAvailable BIT NOT NULL,
            ExtractedAt DATETIME2(0) NOT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factChirpStackDeviceMetrics_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT CK_factChirpStackDeviceMetrics_UplinkInterval
            CHECK (UplinkIntervalSeconds IS NULL OR UplinkIntervalSeconds >= 0),
            CONSTRAINT CK_factChirpStackDeviceMetrics_ExpectedUplinks
            CHECK (ExpectedUplinkCount IS NULL OR ExpectedUplinkCount >= 0),
            CONSTRAINT CK_factChirpStackDeviceMetrics_ReceivedUplinks
            CHECK (ReceivedUplinkCount IS NULL OR ReceivedUplinkCount >= 0),
            CONSTRAINT CK_factChirpStackDeviceMetrics_LinkErrors
            CHECK (LinkErrorCount IS NULL OR LinkErrorCount >= 0),
            CONSTRAINT CK_factChirpStackDeviceMetrics_LinkErrorCountsJson
            CHECK (
                LinkErrorCountsJson IS NULL
                OR ISJSON(LinkErrorCountsJson) = 1
            ),
            CONSTRAINT CK_factChirpStackDeviceMetrics_Battery
            CHECK (
                (
                    BatteryLevel IS NULL
                    AND BatteryLevelSource IS NULL
                    AND BatteryObservedAt IS NULL
                )
                OR (
                    BatteryLevel BETWEEN 0 AND 100
                    AND BatteryLevelSource IN (N'DEVICE_METRICS', N'DEVICE_STATUS')
                    AND BatteryObservedAt IS NOT NULL
                )
            ),
            CONSTRAINT CK_factChirpStackDeviceMetrics_Availability
            CHECK (
                (
                    MetricsAvailable = 0
                    AND ReceivedUplinkCount IS NULL
                    AND AverageRssi IS NULL
                    AND AverageSnr IS NULL
                    AND LinkErrorCount IS NULL
                )
                OR (
                    MetricsAvailable = 1
                    AND ReceivedUplinkCount IS NOT NULL
                    AND LinkErrorCount IS NOT NULL
                    AND (
                        ReceivedUplinkCount = 0
                        OR (AverageRssi IS NOT NULL AND AverageSnr IS NOT NULL)
                    )
                )
            ),
            CONSTRAINT PK_factChirpStackDeviceMetrics
            PRIMARY KEY CLUSTERED (DevEui, TimeSpan)
        );
    END;
GO

IF
    COL_LENGTH(
        N'observability.factChirpStackDeviceMetrics',
        N'LinkErrorCountsJson'
    ) IS NULL
    BEGIN
        ALTER TABLE observability.factChirpStackDeviceMetrics
        ADD LinkErrorCountsJson NVARCHAR(MAX) NULL;
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.check_constraints
        WHERE
            parent_object_id = OBJECT_ID(
                N'observability.factChirpStackDeviceMetrics'
            )
            AND name = N'CK_factChirpStackDeviceMetrics_LinkErrorCountsJson'
    )
    BEGIN
        ALTER TABLE observability.factChirpStackDeviceMetrics
        ADD CONSTRAINT CK_factChirpStackDeviceMetrics_LinkErrorCountsJson
        CHECK (
            LinkErrorCountsJson IS NULL
            OR ISJSON(LinkErrorCountsJson) = 1
        );
    END;
GO

IF OBJECT_ID(N'stg.factChirpStackDeviceMetrics', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.factChirpStackDeviceMetrics
        (
            RunId NVARCHAR(100) NOT NULL,
            DevEui NVARCHAR(16) NOT NULL,
            TimeSpan DATETIME2(0) NOT NULL,
            TenantId UNIQUEIDENTIFIER NOT NULL,
            ApplicationId UNIQUEIDENTIFIER NOT NULL,
            DeviceProfileId UNIQUEIDENTIFIER NOT NULL,
            UplinkIntervalSeconds INT NULL,
            ExpectedUplinkCount DECIMAL(18, 6) NULL,
            ReceivedUplinkCount BIGINT NULL,
            AverageRssi DECIMAL(18, 6) NULL,
            AverageSnr DECIMAL(18, 6) NULL,
            LinkErrorCount BIGINT NULL,
            LinkErrorCountsJson NVARCHAR(MAX) NULL,
            BatteryLevel DECIMAL(7, 4) NULL,
            ExternalPowerSource BIT NULL,
            BatteryLevelSource NVARCHAR(32) NULL,
            BatteryObservedAt DATETIME2(0) NULL,
            MetricsAvailable BIT NOT NULL,
            ExtractedAt DATETIME2(0) NOT NULL
        );
    END;
GO

IF
    COL_LENGTH(
        N'stg.factChirpStackDeviceMetrics',
        N'LinkErrorCountsJson'
    ) IS NULL
    BEGIN
        ALTER TABLE stg.factChirpStackDeviceMetrics
        ADD LinkErrorCountsJson NVARCHAR(MAX) NULL;
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM ctl.IngestionControl
        WHERE IngestionName = N'factChirpStackMetrics'
    )
    BEGIN
        INSERT INTO ctl.IngestionControl (IngestionName)
        VALUES (N'factChirpStackMetrics');
    END;
GO
IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            object_id = OBJECT_ID(N'observability.factChirpStackDeviceMetrics')
            AND name = N'ix_factChirpStackDeviceMetrics_timeSpan'
    )
    BEGIN
        CREATE INDEX ix_factChirpStackDeviceMetrics_timeSpan
            ON observability.factChirpStackDeviceMetrics (TimeSpan DESC, DevEui);
    END;
GO
IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            object_id = OBJECT_ID(N'stg.factChirpStackDeviceMetrics')
            AND name = N'ix_stg_factChirpStackDeviceMetrics_run'
    )
    BEGIN
        CREATE INDEX ix_stg_factChirpStackDeviceMetrics_run
            ON stg.factChirpStackDeviceMetrics (RunId, DevEui, TimeSpan);
    END;
GO
