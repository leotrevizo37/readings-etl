IF OBJECT_ID(N'observability.factChirpStackGatewayMetrics', N'U') IS NULL
    BEGIN
        CREATE TABLE observability.factChirpStackGatewayMetrics
        (
            GatewayId NVARCHAR(16) NOT NULL,
            TimeSpan DATETIME2(0) NOT NULL,
            TenantId UNIQUEIDENTIFIER NOT NULL,
            GatewayName NVARCHAR(255) NOT NULL,
            ReceivedPacketCount BIGINT NULL,
            TransmittedPacketCount BIGINT NULL,
            TxOkCount BIGINT NULL,
            TxErrorCount BIGINT NULL,
            TxPacketStatusCountsJson NVARCHAR(MAX) NULL,
            GatewayState NVARCHAR(20) NOT NULL,
            GatewayLastSeenAt DATETIME2(0) NULL,
            StatsIntervalSeconds INT NULL,
            MetricsAvailable BIT NOT NULL,
            ExtractedAt DATETIME2(0) NOT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factChirpStackGatewayMetrics_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT CK_factChirpStackGatewayMetrics_Counts
            CHECK (
                (ReceivedPacketCount IS NULL OR ReceivedPacketCount >= 0)
                AND (TransmittedPacketCount IS NULL OR TransmittedPacketCount >= 0)
                AND (TxOkCount IS NULL OR TxOkCount >= 0)
                AND (TxErrorCount IS NULL OR TxErrorCount >= 0)
            ),
            CONSTRAINT CK_factChirpStackGatewayMetrics_TxStatusJson
            CHECK (
                TxPacketStatusCountsJson IS NULL
                OR ISJSON(TxPacketStatusCountsJson) = 1
            ),
            CONSTRAINT CK_factChirpStackGatewayMetrics_State
            CHECK (GatewayState IN (N'NEVER_SEEN', N'ONLINE', N'OFFLINE', N'UNKNOWN')),
            CONSTRAINT CK_factChirpStackGatewayMetrics_LastSeen
            CHECK (
                (
                    GatewayState = N'NEVER_SEEN'
                    AND GatewayLastSeenAt IS NULL
                )
                OR (
                    GatewayState = N'UNKNOWN'
                    AND GatewayLastSeenAt IS NULL
                    AND ISNULL(ReceivedPacketCount, 0) = 0
                    AND ISNULL(TransmittedPacketCount, 0) = 0
                    AND ISNULL(TxOkCount, 0) = 0
                    AND ISNULL(TxErrorCount, 0) = 0
                )
                OR (
                    GatewayState = N'ONLINE'
                    AND (
                        GatewayLastSeenAt IS NOT NULL
                        OR ISNULL(ReceivedPacketCount, 0) > 0
                        OR ISNULL(TransmittedPacketCount, 0) > 0
                        OR ISNULL(TxOkCount, 0) > 0
                        OR ISNULL(TxErrorCount, 0) > 0
                    )
                )
                OR (
                    GatewayState = N'OFFLINE'
                    AND GatewayLastSeenAt IS NOT NULL
                )
            ),
            CONSTRAINT CK_factChirpStackGatewayMetrics_StatsInterval
            CHECK (StatsIntervalSeconds IS NULL OR StatsIntervalSeconds >= 0),
            CONSTRAINT CK_factChirpStackGatewayMetrics_Availability
            CHECK (
                (
                    MetricsAvailable = 0
                    AND ReceivedPacketCount IS NULL
                    AND TransmittedPacketCount IS NULL
                    AND TxOkCount IS NULL
                    AND TxErrorCount IS NULL
                )
                OR (
                    MetricsAvailable = 1
                    AND ReceivedPacketCount IS NOT NULL
                    AND TransmittedPacketCount IS NOT NULL
                    AND TxOkCount IS NOT NULL
                    AND TxErrorCount IS NOT NULL
                )
            ),
            CONSTRAINT PK_factChirpStackGatewayMetrics
            PRIMARY KEY CLUSTERED (GatewayId, TimeSpan)
        );
    END;
GO

IF
    EXISTS (
        SELECT 1
        FROM sys.check_constraints
        WHERE
            parent_object_id = OBJECT_ID(N'observability.factChirpStackGatewayMetrics')
            AND name = N'CK_factChirpStackGatewayMetrics_State'
            AND [definition] NOT LIKE N'%UNKNOWN%'
    )
    BEGIN
        ALTER TABLE observability.factChirpStackGatewayMetrics
        DROP CONSTRAINT CK_factChirpStackGatewayMetrics_State;
    END;
GO
IF
    NOT EXISTS (
        SELECT 1
        FROM sys.check_constraints
        WHERE
            parent_object_id = OBJECT_ID(N'observability.factChirpStackGatewayMetrics')
            AND name = N'CK_factChirpStackGatewayMetrics_State'
    )
    BEGIN
        ALTER TABLE observability.factChirpStackGatewayMetrics
        ADD CONSTRAINT CK_factChirpStackGatewayMetrics_State
        CHECK (GatewayState IN (N'NEVER_SEEN', N'ONLINE', N'OFFLINE', N'UNKNOWN'));
    END;
GO
IF
    EXISTS (
        SELECT 1
        FROM sys.check_constraints
        WHERE
            parent_object_id = OBJECT_ID(N'observability.factChirpStackGatewayMetrics')
            AND name = N'CK_factChirpStackGatewayMetrics_LastSeen'
            AND [definition] NOT LIKE N'%ReceivedPacketCount%'
    )
    BEGIN
        ALTER TABLE observability.factChirpStackGatewayMetrics
        DROP CONSTRAINT CK_factChirpStackGatewayMetrics_LastSeen;
    END;
GO
IF
    NOT EXISTS (
        SELECT 1
        FROM sys.check_constraints
        WHERE
            parent_object_id = OBJECT_ID(N'observability.factChirpStackGatewayMetrics')
            AND name = N'CK_factChirpStackGatewayMetrics_LastSeen'
    )
    BEGIN
        ALTER TABLE observability.factChirpStackGatewayMetrics
        ADD CONSTRAINT CK_factChirpStackGatewayMetrics_LastSeen
        CHECK (
            (
                GatewayState = N'NEVER_SEEN'
                AND GatewayLastSeenAt IS NULL
            )
            OR (
                GatewayState = N'UNKNOWN'
                AND GatewayLastSeenAt IS NULL
                AND ISNULL(ReceivedPacketCount, 0) = 0
                AND ISNULL(TransmittedPacketCount, 0) = 0
                AND ISNULL(TxOkCount, 0) = 0
                AND ISNULL(TxErrorCount, 0) = 0
            )
            OR (
                GatewayState = N'ONLINE'
                AND (
                    GatewayLastSeenAt IS NOT NULL
                    OR ISNULL(ReceivedPacketCount, 0) > 0
                    OR ISNULL(TransmittedPacketCount, 0) > 0
                    OR ISNULL(TxOkCount, 0) > 0
                    OR ISNULL(TxErrorCount, 0) > 0
                )
            )
            OR (
                GatewayState = N'OFFLINE'
                AND GatewayLastSeenAt IS NOT NULL
            )
        );
    END;
GO

IF
    COL_LENGTH(
        N'observability.factChirpStackGatewayMetrics',
        N'TxPacketStatusCountsJson'
    ) IS NULL
    BEGIN
        ALTER TABLE observability.factChirpStackGatewayMetrics
        ADD TxPacketStatusCountsJson NVARCHAR(MAX) NULL;
    END;
GO
IF
    NOT EXISTS (
        SELECT 1
        FROM sys.check_constraints
        WHERE
            parent_object_id = OBJECT_ID(
                N'observability.factChirpStackGatewayMetrics'
            )
            AND name = N'CK_factChirpStackGatewayMetrics_TxStatusJson'
    )
    BEGIN
        ALTER TABLE observability.factChirpStackGatewayMetrics
        ADD CONSTRAINT CK_factChirpStackGatewayMetrics_TxStatusJson
        CHECK (
            TxPacketStatusCountsJson IS NULL
            OR ISJSON(TxPacketStatusCountsJson) = 1
        );
    END;
GO
IF OBJECT_ID(N'stg.factChirpStackGatewayMetrics', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.factChirpStackGatewayMetrics
        (
            RunId NVARCHAR(100) NOT NULL,
            GatewayId NVARCHAR(16) NOT NULL,
            TimeSpan DATETIME2(0) NOT NULL,
            TenantId UNIQUEIDENTIFIER NOT NULL,
            GatewayName NVARCHAR(255) NOT NULL,
            ReceivedPacketCount BIGINT NULL,
            TransmittedPacketCount BIGINT NULL,
            TxOkCount BIGINT NULL,
            TxErrorCount BIGINT NULL,
            TxPacketStatusCountsJson NVARCHAR(MAX) NULL,
            GatewayState NVARCHAR(20) NOT NULL,
            GatewayLastSeenAt DATETIME2(0) NULL,
            StatsIntervalSeconds INT NULL,
            MetricsAvailable BIT NOT NULL,
            ExtractedAt DATETIME2(0) NOT NULL
        );
    END;
GO

IF
    COL_LENGTH(
        N'stg.factChirpStackGatewayMetrics',
        N'TxPacketStatusCountsJson'
    ) IS NULL
    BEGIN
        ALTER TABLE stg.factChirpStackGatewayMetrics
        ADD TxPacketStatusCountsJson NVARCHAR(MAX) NULL;
    END;
GO
IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            object_id = OBJECT_ID(N'observability.factChirpStackGatewayMetrics')
            AND name = N'ix_factChirpStackGatewayMetrics_timeSpan'
    )
    BEGIN
        CREATE INDEX ix_factChirpStackGatewayMetrics_timeSpan
            ON observability.factChirpStackGatewayMetrics (TimeSpan DESC, TenantId, GatewayId);
    END;
GO
IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            object_id = OBJECT_ID(N'stg.factChirpStackGatewayMetrics')
            AND name = N'ix_stg_factChirpStackGatewayMetrics_run'
    )
    BEGIN
        CREATE INDEX ix_stg_factChirpStackGatewayMetrics_run
            ON stg.factChirpStackGatewayMetrics (RunId, GatewayId, TimeSpan);
    END;
GO
