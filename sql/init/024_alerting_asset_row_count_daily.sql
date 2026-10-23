SET QUOTED_IDENTIFIER ON;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'alerting'
    )
    BEGIN
        EXEC ('CREATE SCHEMA alerting');
    END;
GO

IF OBJECT_ID(N'alerting.asset_row_count_daily', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.asset_row_count_daily (
            observation_date DATE NOT NULL,
            code_location NVARCHAR(100) NOT NULL,
            asset_key NVARCHAR(500) NOT NULL,
            metadata_key NVARCHAR(100) NOT NULL,
            daily_materialization_count INT NOT NULL,
            daily_row_count_sum BIGINT NOT NULL,
            daily_row_count_average NUMERIC(20, 4) NOT NULL,
            daily_row_count_min BIGINT NOT NULL,
            daily_row_count_max BIGINT NOT NULL,
            first_result_count BIGINT NOT NULL,
            last_result_count BIGINT NOT NULL,
            average_1d NUMERIC(20, 4) NULL,
            average_7d NUMERIC(20, 4) NULL,
            average_28d NUMERIC(20, 4) NULL,
            observed_days_7d SMALLINT NOT NULL
            CONSTRAINT DF_asset_row_count_daily_observed_days_7d DEFAULT (0),
            observed_days_28d SMALLINT NOT NULL
            CONSTRAINT DF_asset_row_count_daily_observed_days_28d DEFAULT (0),
            change_vs_1d_pct NUMERIC(12, 4) NULL,
            change_vs_7d_pct NUMERIC(12, 4) NULL,
            change_vs_28d_pct NUMERIC(12, 4) NULL,
            latest_event_storage_id BIGINT NOT NULL,
            latest_run_id NVARCHAR(64) NULL,
            latest_materialized_at DATETIME2(3) NOT NULL,
            created_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_asset_row_count_daily_created_at
            DEFAULT (SYSUTCDATETIME()),
            updated_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_asset_row_count_daily_updated_at
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_asset_row_count_daily PRIMARY KEY NONCLUSTERED
            (observation_date, code_location, asset_key, metadata_key)
        );
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            name = N'ix_asset_row_count_daily_asset'
            AND object_id = OBJECT_ID(N'alerting.asset_row_count_daily')
    )
    BEGIN
        CREATE INDEX ix_asset_row_count_daily_asset
            ON alerting.asset_row_count_daily
            (code_location, asset_key, metadata_key, observation_date DESC);
    END;
GO
