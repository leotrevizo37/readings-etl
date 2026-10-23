SET QUOTED_IDENTIFIER ON;
GO

IF OBJECT_ID(N'alerting.alert_config', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_config (
            alert_config_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_config_v2 PRIMARY KEY,
            user_code NVARCHAR(100) COLLATE Latin1_General_100_BIN2 NOT NULL,
            version_number INT NOT NULL,
            config_status_id UNIQUEIDENTIFIER NOT NULL,
            created_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_config_v2_created DEFAULT (SYSUTCDATETIME()),
            updated_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_config_v2_updated DEFAULT (SYSUTCDATETIME()),
            activated_at DATETIME2(3) NULL,
            retired_at DATETIME2(3) NULL,
            version ROWVERSION,
            CONSTRAINT UQ_alert_config_v2_user_version
            UNIQUE (user_code, version_number),
            CONSTRAINT FK_alert_config_v2_status FOREIGN KEY (config_status_id)
            REFERENCES alerting.dim_config_status (config_status_id),
            CONSTRAINT CK_alert_config_v2_version CHECK (version_number > 0)
        );
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            name = N'ux_alert_config_v2_active'
            AND object_id = OBJECT_ID(N'alerting.alert_config')
    )
    BEGIN
        CREATE UNIQUE INDEX ux_alert_config_v2_active
            ON alerting.alert_config (user_code)
            WHERE
                config_status_id
                = '50000000-0000-0000-0000-000000000002';
    END;
GO

IF OBJECT_ID(N'alerting.alert_rule', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_rule (
            alert_rule_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_rule_v2 PRIMARY KEY,
            alert_config_id UNIQUEIDENTIFIER NOT NULL,
            alert_type_id UNIQUEIDENTIFIER NOT NULL,
            entity_key NVARCHAR(450) COLLATE Latin1_General_100_BIN2 NULL,
            is_enabled BIT NOT NULL,
            severity_id UNIQUEIDENTIFIER NOT NULL,
            evaluation_interval_seconds INT NOT NULL,
            created_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_rule_v2_created DEFAULT (SYSUTCDATETIME()),
            updated_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_rule_v2_updated DEFAULT (SYSUTCDATETIME()),
            version ROWVERSION,
            CONSTRAINT UQ_alert_rule_v2_config
            UNIQUE (alert_rule_id, alert_config_id),
            CONSTRAINT FK_alert_rule_v2_config FOREIGN KEY (alert_config_id)
            REFERENCES alerting.alert_config (alert_config_id),
            CONSTRAINT FK_alert_rule_v2_type FOREIGN KEY (alert_type_id)
            REFERENCES alerting.dim_alert_type (alert_type_id),
            CONSTRAINT FK_alert_rule_v2_severity FOREIGN KEY (severity_id)
            REFERENCES alerting.dim_severity (severity_id),
            CONSTRAINT CK_alert_rule_v2_interval
            CHECK (evaluation_interval_seconds >= 30),
            CONSTRAINT CK_alert_rule_v2_entity
            CHECK (entity_key IS NULL OR LEN(entity_key) > 0)
        );
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            name = N'ux_alert_rule_v2_default'
            AND object_id = OBJECT_ID(N'alerting.alert_rule')
    )
    BEGIN
        CREATE UNIQUE INDEX ux_alert_rule_v2_default
            ON alerting.alert_rule (alert_config_id, alert_type_id)
            WHERE entity_key IS NULL;
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            name = N'ux_alert_rule_v2_entity'
            AND object_id = OBJECT_ID(N'alerting.alert_rule')
    )
    BEGIN
        CREATE UNIQUE INDEX ux_alert_rule_v2_entity
            ON alerting.alert_rule (
                alert_config_id,
                alert_type_id,
                entity_key
            )
            WHERE entity_key IS NOT NULL;
    END;
GO

IF OBJECT_ID(N'alerting.alert_rule_no_readings', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_rule_no_readings (
            alert_rule_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_rule_no_readings PRIMARY KEY,
            no_readings_minutes INT NOT NULL,
            source_max_age_minutes INT NOT NULL,
            CONSTRAINT FK_alert_rule_no_readings_rule
            FOREIGN KEY (alert_rule_id)
            REFERENCES alerting.alert_rule (alert_rule_id),
            CONSTRAINT CK_alert_rule_no_readings_value
            CHECK (no_readings_minutes > 0),
            CONSTRAINT CK_alert_rule_no_readings_source_age
            CHECK (source_max_age_minutes > 0)
        );
    END;
GO

IF OBJECT_ID(N'alerting.alert_rule_pipeline_stale', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_rule_pipeline_stale (
            alert_rule_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_rule_pipeline_stale PRIMARY KEY,
            expected_interval_minutes INT NOT NULL,
            CONSTRAINT FK_alert_rule_pipeline_stale_rule
            FOREIGN KEY (alert_rule_id)
            REFERENCES alerting.alert_rule (alert_rule_id),
            CONSTRAINT CK_alert_rule_pipeline_stale_interval
            CHECK (expected_interval_minutes > 0)
        );
    END;
GO

IF OBJECT_ID(N'alerting.alert_rule_row_count_trend', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_rule_row_count_trend (
            alert_rule_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_rule_row_count_trend PRIMARY KEY,
            warning_drop_pct DECIMAL(6, 2) NOT NULL,
            critical_drop_pct DECIMAL(6, 2) NOT NULL,
            minimum_observed_days_7d TINYINT NOT NULL,
            minimum_observed_days_28d TINYINT NOT NULL,
            critical_severity_id UNIQUEIDENTIFIER NOT NULL,
            CONSTRAINT FK_alert_rule_row_count_trend_rule
            FOREIGN KEY (alert_rule_id)
            REFERENCES alerting.alert_rule (alert_rule_id),
            CONSTRAINT FK_alert_rule_row_count_trend_severity
            FOREIGN KEY (critical_severity_id)
            REFERENCES alerting.dim_severity (severity_id),
            CONSTRAINT CK_alert_rule_row_count_trend_warning
            CHECK (warning_drop_pct BETWEEN 0 AND 100),
            CONSTRAINT CK_alert_rule_row_count_trend_critical
            CHECK (
                critical_drop_pct BETWEEN 0 AND 100
                AND critical_drop_pct >= warning_drop_pct
            ),
            CONSTRAINT CK_alert_rule_row_count_trend_days_7d
            CHECK (minimum_observed_days_7d BETWEEN 1 AND 7),
            CONSTRAINT CK_alert_rule_row_count_trend_days_28d
            CHECK (minimum_observed_days_28d BETWEEN 1 AND 28)
        );
    END;
GO

IF OBJECT_ID(N'alerting.alert_detector_operational_config', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_detector_operational_config (
            alert_config_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_detector_operational_config PRIMARY KEY,
            sensor_lookback_hours INT NOT NULL,
            dagster_failure_lookback_minutes INT NOT NULL,
            dagster_failure_batch_size INT NOT NULL,
            CONSTRAINT FK_alert_detector_operational_config
            FOREIGN KEY (alert_config_id)
            REFERENCES alerting.alert_config (alert_config_id),
            CONSTRAINT CK_alert_detector_operational_lookback
            CHECK (
                sensor_lookback_hours > 0
                AND dagster_failure_lookback_minutes > 0
            ),
            CONSTRAINT CK_alert_detector_operational_batch
            CHECK (dagster_failure_batch_size BETWEEN 1 AND 1000)
        );
    END;
GO

IF OBJECT_ID(N'alerting.alert_detector_row_count_config', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_detector_row_count_config (
            alert_config_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_detector_row_count_config PRIMARY KEY,
            capture_interval_seconds INT NOT NULL,
            metadata_key NVARCHAR(100) NOT NULL,
            backfill_days INT NOT NULL,
            batch_size INT NOT NULL,
            CONSTRAINT FK_alert_detector_row_count_config
            FOREIGN KEY (alert_config_id)
            REFERENCES alerting.alert_config (alert_config_id),
            CONSTRAINT CK_alert_detector_row_count_interval
            CHECK (capture_interval_seconds >= 30),
            CONSTRAINT CK_alert_detector_row_count_metadata
            CHECK (LEN(metadata_key) > 0),
            CONSTRAINT CK_alert_detector_row_count_backfill
            CHECK (backfill_days > 0),
            CONSTRAINT CK_alert_detector_row_count_batch
            CHECK (batch_size BETWEEN 1 AND 1000)
        );
    END;
GO
