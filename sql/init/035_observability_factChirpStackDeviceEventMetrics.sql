IF OBJECT_ID(N'observability.factChirpStackDeviceEventMetrics', N'U') IS NULL
    BEGIN
        CREATE TABLE observability.factChirpStackDeviceEventMetrics
        (
            DevEui NVARCHAR(16) NOT NULL,
            TimeSpan DATETIME2(0) NOT NULL,
            TenantId UNIQUEIDENTIFIER NOT NULL,
            CapturedUplinkCount BIGINT NOT NULL,
            ObservedGatewayCount INT NOT NULL,
            MultiGatewayUplinkCount BIGINT NOT NULL,
            EventLogCountsJson NVARCHAR(MAX) NOT NULL,
            RadioProfileCountsJson NVARCHAR(MAX) NULL,
            UplinkContextCountsJson NVARCHAR(MAX) NULL,
            RadioFieldQualityJson NVARCHAR(MAX) NULL,
            FrameCounterSummaryJson NVARCHAR(MAX) NULL,
            ExtractedAt DATETIME2(0) NOT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factChirpStackDeviceEventMetrics_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT CK_factChirpStackDeviceEventMetrics_Counts
            CHECK (
                CapturedUplinkCount >= 0
                AND ObservedGatewayCount >= 0
                AND MultiGatewayUplinkCount >= 0
                AND MultiGatewayUplinkCount <= CapturedUplinkCount
            ),
            CONSTRAINT CK_factChirpStackDeviceEventMetrics_LogsJson
            CHECK (ISJSON(EventLogCountsJson) = 1),
            CONSTRAINT CK_observability_factChirpStackDeviceEventMetrics_RadioJson
            CHECK (
                (RadioProfileCountsJson IS NULL OR ISJSON(RadioProfileCountsJson) = 1)
                AND (UplinkContextCountsJson IS NULL OR ISJSON(UplinkContextCountsJson) = 1)
                AND (RadioFieldQualityJson IS NULL OR ISJSON(RadioFieldQualityJson) = 1)
                AND (FrameCounterSummaryJson IS NULL OR ISJSON(FrameCounterSummaryJson) = 1)
            ),
            CONSTRAINT PK_factChirpStackDeviceEventMetrics
            PRIMARY KEY CLUSTERED (DevEui, TimeSpan)
        );
    END;
GO

IF OBJECT_ID(N'stg.factChirpStackDeviceEventMetrics', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.factChirpStackDeviceEventMetrics
        (
            RunId NVARCHAR(100) NOT NULL,
            DevEui NVARCHAR(16) NOT NULL,
            TimeSpan DATETIME2(0) NOT NULL,
            TenantId UNIQUEIDENTIFIER NOT NULL,
            CapturedUplinkCount BIGINT NOT NULL,
            ObservedGatewayCount INT NOT NULL,
            MultiGatewayUplinkCount BIGINT NOT NULL,
            EventLogCountsJson NVARCHAR(MAX) NOT NULL,
            RadioProfileCountsJson NVARCHAR(MAX) NULL,
            UplinkContextCountsJson NVARCHAR(MAX) NULL,
            RadioFieldQualityJson NVARCHAR(MAX) NULL,
            FrameCounterSummaryJson NVARCHAR(MAX) NULL,
            ExtractedAt DATETIME2(0) NOT NULL,
            CONSTRAINT CK_stg_factChirpStackDeviceEventMetrics_RadioJson
            CHECK (
                (RadioProfileCountsJson IS NULL OR ISJSON(RadioProfileCountsJson) = 1)
                AND (UplinkContextCountsJson IS NULL OR ISJSON(UplinkContextCountsJson) = 1)
                AND (RadioFieldQualityJson IS NULL OR ISJSON(RadioFieldQualityJson) = 1)
                AND (FrameCounterSummaryJson IS NULL OR ISJSON(FrameCounterSummaryJson) = 1)
            )
        );
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            object_id = OBJECT_ID(
                N'observability.factChirpStackDeviceEventMetrics'
            )
            AND name = N'ix_factChirpStackDeviceEventMetrics_timeSpan'
    )
    BEGIN
        CREATE INDEX ix_factChirpStackDeviceEventMetrics_timeSpan
            ON observability.factChirpStackDeviceEventMetrics
            (TimeSpan DESC, DevEui);
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            object_id = OBJECT_ID(N'stg.factChirpStackDeviceEventMetrics')
            AND name = N'ix_stg_factChirpStackDeviceEventMetrics_run'
    )
    BEGIN
        CREATE INDEX ix_stg_factChirpStackDeviceEventMetrics_run
            ON stg.factChirpStackDeviceEventMetrics
            (RunId, DevEui, TimeSpan);
    END;
GO

IF COL_LENGTH(N'observability.factChirpStackDeviceEventMetrics', N'RadioProfileCountsJson') IS NULL
    BEGIN
        ALTER TABLE observability.factChirpStackDeviceEventMetrics
        ADD RadioProfileCountsJson NVARCHAR(MAX) NULL;
    END;
GO

IF COL_LENGTH(N'observability.factChirpStackDeviceEventMetrics', N'UplinkContextCountsJson') IS NULL
    BEGIN
        ALTER TABLE observability.factChirpStackDeviceEventMetrics
        ADD UplinkContextCountsJson NVARCHAR(MAX) NULL;
    END;
GO

IF COL_LENGTH(N'observability.factChirpStackDeviceEventMetrics', N'RadioFieldQualityJson') IS NULL
    BEGIN
        ALTER TABLE observability.factChirpStackDeviceEventMetrics
        ADD RadioFieldQualityJson NVARCHAR(MAX) NULL;
    END;
GO

IF COL_LENGTH(N'observability.factChirpStackDeviceEventMetrics', N'FrameCounterSummaryJson') IS NULL
    BEGIN
        ALTER TABLE observability.factChirpStackDeviceEventMetrics
        ADD FrameCounterSummaryJson NVARCHAR(MAX) NULL;
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.check_constraints
        WHERE
            parent_object_id = OBJECT_ID(N'observability.factChirpStackDeviceEventMetrics')
            AND name = N'CK_observability_factChirpStackDeviceEventMetrics_RadioJson'
    )
    BEGIN
        ALTER TABLE observability.factChirpStackDeviceEventMetrics
        ADD CONSTRAINT CK_observability_factChirpStackDeviceEventMetrics_RadioJson
        CHECK (
            (RadioProfileCountsJson IS NULL OR ISJSON(RadioProfileCountsJson) = 1)
            AND (UplinkContextCountsJson IS NULL OR ISJSON(UplinkContextCountsJson) = 1)
            AND (RadioFieldQualityJson IS NULL OR ISJSON(RadioFieldQualityJson) = 1)
            AND (FrameCounterSummaryJson IS NULL OR ISJSON(FrameCounterSummaryJson) = 1)
        );
    END;
GO

IF COL_LENGTH(N'stg.factChirpStackDeviceEventMetrics', N'RadioProfileCountsJson') IS NULL
    BEGIN
        ALTER TABLE stg.factChirpStackDeviceEventMetrics
        ADD RadioProfileCountsJson NVARCHAR(MAX) NULL;
    END;
GO

IF COL_LENGTH(N'stg.factChirpStackDeviceEventMetrics', N'UplinkContextCountsJson') IS NULL
    BEGIN
        ALTER TABLE stg.factChirpStackDeviceEventMetrics
        ADD UplinkContextCountsJson NVARCHAR(MAX) NULL;
    END;
GO

IF COL_LENGTH(N'stg.factChirpStackDeviceEventMetrics', N'RadioFieldQualityJson') IS NULL
    BEGIN
        ALTER TABLE stg.factChirpStackDeviceEventMetrics
        ADD RadioFieldQualityJson NVARCHAR(MAX) NULL;
    END;
GO

IF COL_LENGTH(N'stg.factChirpStackDeviceEventMetrics', N'FrameCounterSummaryJson') IS NULL
    BEGIN
        ALTER TABLE stg.factChirpStackDeviceEventMetrics
        ADD FrameCounterSummaryJson NVARCHAR(MAX) NULL;
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.check_constraints
        WHERE
            parent_object_id = OBJECT_ID(N'stg.factChirpStackDeviceEventMetrics')
            AND name = N'CK_stg_factChirpStackDeviceEventMetrics_RadioJson'
    )
    BEGIN
        ALTER TABLE stg.factChirpStackDeviceEventMetrics
        ADD CONSTRAINT CK_stg_factChirpStackDeviceEventMetrics_RadioJson
        CHECK (
            (RadioProfileCountsJson IS NULL OR ISJSON(RadioProfileCountsJson) = 1)
            AND (UplinkContextCountsJson IS NULL OR ISJSON(UplinkContextCountsJson) = 1)
            AND (RadioFieldQualityJson IS NULL OR ISJSON(RadioFieldQualityJson) = 1)
            AND (FrameCounterSummaryJson IS NULL OR ISJSON(FrameCounterSummaryJson) = 1)
        );
    END;
GO
