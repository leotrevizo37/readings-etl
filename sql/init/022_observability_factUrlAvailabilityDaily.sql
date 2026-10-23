IF OBJECT_ID(N'observability.factUrlAvailabilityDaily', N'U') IS NULL
    BEGIN
        CREATE TABLE observability.factUrlAvailabilityDaily
        (
            Url VARCHAR(850) NOT NULL,
            [Date] DATE NOT NULL,
            UptimePercentage DECIMAL(7, 4) NOT NULL,
            AvgLatencySeconds DECIMAL(18, 6) NOT NULL,
            Latency95thPercentileSeconds DECIMAL(18, 6) NOT NULL,
            IsUp BIT NOT NULL,
            TimeoutsPresent BIT NOT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factUrlAvailabilityDaily_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_factUrlAvailabilityDaily
            PRIMARY KEY CLUSTERED ([Date], Url)
        );
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM ctl.IngestionControl
        WHERE IngestionName = N'factUrlAvailabilityDaily'
    )
    BEGIN
        INSERT INTO ctl.IngestionControl (IngestionName)
        VALUES (N'factUrlAvailabilityDaily');
    END;
GO
