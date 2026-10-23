IF OBJECT_ID(N'observability.factChirpStackDeviceGatewayMetrics', N'U') IS NULL
    BEGIN
        CREATE TABLE observability.factChirpStackDeviceGatewayMetrics
        (
            DevEui NVARCHAR(16) NOT NULL,
            GatewayId NVARCHAR(16) NOT NULL,
            TimeSpan DATETIME2(0) NOT NULL,
            TenantId UNIQUEIDENTIFIER NOT NULL,
            GatewayReceptionCount BIGINT NOT NULL,
            ReceptionShare DECIMAL(18, 6) NOT NULL,
            AverageRssi DECIMAL(18, 6) NULL,
            MinRssi DECIMAL(18, 6) NULL,
            MaxRssi DECIMAL(18, 6) NULL,
            AverageSnr DECIMAL(18, 6) NULL,
            MinSnr DECIMAL(18, 6) NULL,
            MaxSnr DECIMAL(18, 6) NULL,
            RadioReceptionStatsJson NVARCHAR(MAX) NULL,
            ReceptionFieldQualityJson NVARCHAR(MAX) NULL,
            FirstReceivedAt DATETIME2(6) NOT NULL,
            LastReceivedAt DATETIME2(6) NOT NULL,
            ExtractedAt DATETIME2(0) NOT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factChirpStackDeviceGatewayMetrics_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT CK_factChirpStackDeviceGatewayMetrics_Reception
            CHECK (
                GatewayReceptionCount > 0
                AND ReceptionShare > 0
                AND ReceptionShare <= 1
                AND FirstReceivedAt <= LastReceivedAt
                AND MinRssi <= AverageRssi
                AND AverageRssi <= MaxRssi
                AND MinSnr <= AverageSnr
                AND AverageSnr <= MaxSnr
            ),
            CONSTRAINT PK_factChirpStackDeviceGatewayMetrics
            PRIMARY KEY CLUSTERED (DevEui, GatewayId, TimeSpan),
            CONSTRAINT CK_observability_factChirpStackDeviceGatewayMetrics_RadioJson
            CHECK (
                (RadioReceptionStatsJson IS NULL OR ISJSON(RadioReceptionStatsJson) = 1)
                AND (ReceptionFieldQualityJson IS NULL OR ISJSON(ReceptionFieldQualityJson) = 1)
            )
        );
    END;
GO

IF OBJECT_ID(N'observability.factChirpStackDeviceGatewayMetrics', N'U') IS NOT NULL
    BEGIN
        ALTER TABLE observability.factChirpStackDeviceGatewayMetrics
        ALTER COLUMN AverageRssi DECIMAL(18, 6) NULL;
        ALTER TABLE observability.factChirpStackDeviceGatewayMetrics
        ALTER COLUMN MinRssi DECIMAL(18, 6) NULL;
        ALTER TABLE observability.factChirpStackDeviceGatewayMetrics
        ALTER COLUMN MaxRssi DECIMAL(18, 6) NULL;
        ALTER TABLE observability.factChirpStackDeviceGatewayMetrics
        ALTER COLUMN AverageSnr DECIMAL(18, 6) NULL;
        ALTER TABLE observability.factChirpStackDeviceGatewayMetrics
        ALTER COLUMN MinSnr DECIMAL(18, 6) NULL;
        ALTER TABLE observability.factChirpStackDeviceGatewayMetrics
        ALTER COLUMN MaxSnr DECIMAL(18, 6) NULL;
    END;
GO

IF OBJECT_ID(N'stg.factChirpStackDeviceGatewayMetrics', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.factChirpStackDeviceGatewayMetrics
        (
            RunId NVARCHAR(100) NOT NULL,
            DevEui NVARCHAR(16) NOT NULL,
            GatewayId NVARCHAR(16) NOT NULL,
            TimeSpan DATETIME2(0) NOT NULL,
            TenantId UNIQUEIDENTIFIER NOT NULL,
            GatewayReceptionCount BIGINT NOT NULL,
            ReceptionShare DECIMAL(18, 6) NOT NULL,
            AverageRssi DECIMAL(18, 6) NULL,
            MinRssi DECIMAL(18, 6) NULL,
            MaxRssi DECIMAL(18, 6) NULL,
            AverageSnr DECIMAL(18, 6) NULL,
            MinSnr DECIMAL(18, 6) NULL,
            MaxSnr DECIMAL(18, 6) NULL,
            RadioReceptionStatsJson NVARCHAR(MAX) NULL,
            ReceptionFieldQualityJson NVARCHAR(MAX) NULL,
            FirstReceivedAt DATETIME2(6) NOT NULL,
            LastReceivedAt DATETIME2(6) NOT NULL,
            ExtractedAt DATETIME2(0) NOT NULL,
            CONSTRAINT CK_stg_factChirpStackDeviceGatewayMetrics_RadioJson
            CHECK (
                (RadioReceptionStatsJson IS NULL OR ISJSON(RadioReceptionStatsJson) = 1)
                AND (ReceptionFieldQualityJson IS NULL OR ISJSON(ReceptionFieldQualityJson) = 1)
            )
        );
    END;
GO

IF OBJECT_ID(N'stg.factChirpStackDeviceGatewayMetrics', N'U') IS NOT NULL
    BEGIN
        ALTER TABLE stg.factChirpStackDeviceGatewayMetrics
        ALTER COLUMN AverageRssi DECIMAL(18, 6) NULL;
        ALTER TABLE stg.factChirpStackDeviceGatewayMetrics
        ALTER COLUMN MinRssi DECIMAL(18, 6) NULL;
        ALTER TABLE stg.factChirpStackDeviceGatewayMetrics
        ALTER COLUMN MaxRssi DECIMAL(18, 6) NULL;
        ALTER TABLE stg.factChirpStackDeviceGatewayMetrics
        ALTER COLUMN AverageSnr DECIMAL(18, 6) NULL;
        ALTER TABLE stg.factChirpStackDeviceGatewayMetrics
        ALTER COLUMN MinSnr DECIMAL(18, 6) NULL;
        ALTER TABLE stg.factChirpStackDeviceGatewayMetrics
        ALTER COLUMN MaxSnr DECIMAL(18, 6) NULL;
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            object_id = OBJECT_ID(
                N'observability.factChirpStackDeviceGatewayMetrics'
            )
            AND name = N'ix_factChirpStackDeviceGatewayMetrics_gateway'
    )
    BEGIN
        CREATE INDEX ix_factChirpStackDeviceGatewayMetrics_gateway
            ON observability.factChirpStackDeviceGatewayMetrics
            (GatewayId, TimeSpan DESC, DevEui);
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            object_id = OBJECT_ID(N'stg.factChirpStackDeviceGatewayMetrics')
            AND name = N'ix_stg_factChirpStackDeviceGatewayMetrics_run'
    )
    BEGIN
        CREATE INDEX ix_stg_factChirpStackDeviceGatewayMetrics_run
            ON stg.factChirpStackDeviceGatewayMetrics
            (RunId, DevEui, GatewayId, TimeSpan);
    END;
GO

IF COL_LENGTH(N'observability.factChirpStackDeviceGatewayMetrics', N'RadioReceptionStatsJson') IS NULL
    BEGIN
        ALTER TABLE observability.factChirpStackDeviceGatewayMetrics
        ADD RadioReceptionStatsJson NVARCHAR(MAX) NULL;
    END;
GO

IF COL_LENGTH(N'observability.factChirpStackDeviceGatewayMetrics', N'ReceptionFieldQualityJson') IS NULL
    BEGIN
        ALTER TABLE observability.factChirpStackDeviceGatewayMetrics
        ADD ReceptionFieldQualityJson NVARCHAR(MAX) NULL;
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.check_constraints
        WHERE
            parent_object_id = OBJECT_ID(N'observability.factChirpStackDeviceGatewayMetrics')
            AND name = N'CK_observability_factChirpStackDeviceGatewayMetrics_RadioJson'
    )
    BEGIN
        ALTER TABLE observability.factChirpStackDeviceGatewayMetrics
        ADD CONSTRAINT CK_observability_factChirpStackDeviceGatewayMetrics_RadioJson
        CHECK (
            (RadioReceptionStatsJson IS NULL OR ISJSON(RadioReceptionStatsJson) = 1)
            AND (ReceptionFieldQualityJson IS NULL OR ISJSON(ReceptionFieldQualityJson) = 1)
        );
    END;
GO

IF COL_LENGTH(N'stg.factChirpStackDeviceGatewayMetrics', N'RadioReceptionStatsJson') IS NULL
    BEGIN
        ALTER TABLE stg.factChirpStackDeviceGatewayMetrics
        ADD RadioReceptionStatsJson NVARCHAR(MAX) NULL;
    END;
GO

IF COL_LENGTH(N'stg.factChirpStackDeviceGatewayMetrics', N'ReceptionFieldQualityJson') IS NULL
    BEGIN
        ALTER TABLE stg.factChirpStackDeviceGatewayMetrics
        ADD ReceptionFieldQualityJson NVARCHAR(MAX) NULL;
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.check_constraints
        WHERE
            parent_object_id = OBJECT_ID(N'stg.factChirpStackDeviceGatewayMetrics')
            AND name = N'CK_stg_factChirpStackDeviceGatewayMetrics_RadioJson'
    )
    BEGIN
        ALTER TABLE stg.factChirpStackDeviceGatewayMetrics
        ADD CONSTRAINT CK_stg_factChirpStackDeviceGatewayMetrics_RadioJson
        CHECK (
            (RadioReceptionStatsJson IS NULL OR ISJSON(RadioReceptionStatsJson) = 1)
            AND (ReceptionFieldQualityJson IS NULL OR ISJSON(ReceptionFieldQualityJson) = 1)
        );
    END;
GO
