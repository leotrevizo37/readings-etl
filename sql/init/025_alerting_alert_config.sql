SET QUOTED_IDENTIFIER ON;
GO

IF
    NOT EXISTS (
        SELECT 1 FROM sys.schemas
        WHERE name = N'alerting'
    )
    BEGIN
        EXEC ('CREATE SCHEMA alerting');
    END;
GO

IF OBJECT_ID(N'alerting.alert_config', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_config (
            config_id TINYINT NOT NULL
            CONSTRAINT PK_alert_config PRIMARY KEY
            CONSTRAINT CK_alert_config_singleton CHECK (config_id = 1),
            user_code NVARCHAR(100) NOT NULL,
            enabled BIT NOT NULL
            CONSTRAINT DF_alert_config_enabled DEFAULT (1),
            severity NVARCHAR(20) NOT NULL
            CONSTRAINT DF_alert_config_severity DEFAULT (N'WARNING'),
            evaluation_interval_seconds INT NOT NULL
            CONSTRAINT DF_alert_config_evaluation_interval_seconds DEFAULT (300),
            delivery_interval_seconds INT NOT NULL
            CONSTRAINT DF_alert_config_delivery_interval_seconds DEFAULT (300),
            no_readings_minutes INT NOT NULL
            CONSTRAINT DF_alert_config_no_readings_minutes DEFAULT (60),
            source_max_age_minutes INT NOT NULL
            CONSTRAINT DF_alert_config_source_max_age_minutes DEFAULT (60),
            expected_interval_minutes INT NOT NULL
            CONSTRAINT DF_alert_config_expected_interval_minutes DEFAULT (720),
            warning_drop_pct DECIMAL(6, 2) NOT NULL
            CONSTRAINT DF_alert_config_warning_drop_pct DEFAULT (20),
            critical_drop_pct DECIMAL(6, 2) NOT NULL
            CONSTRAINT DF_alert_config_critical_drop_pct DEFAULT (40),
            minimum_observed_days_7d TINYINT NOT NULL
            CONSTRAINT DF_alert_config_minimum_observed_days_7d DEFAULT (7),
            minimum_observed_days_28d TINYINT NOT NULL
            CONSTRAINT DF_alert_config_minimum_observed_days_28d DEFAULT (28),
            open_mode NVARCHAR(20) NOT NULL
            CONSTRAINT DF_alert_config_open_mode DEFAULT (N'IMMEDIATE'),
            resolution_mode NVARCHAR(20) NOT NULL
            CONSTRAINT DF_alert_config_resolution_mode DEFAULT (N'WITH_NEXT'),
            reminder_mode NVARCHAR(20) NOT NULL
            CONSTRAINT DF_alert_config_reminder_mode DEFAULT (N'DAILY'),
            reminder_interval_minutes INT NOT NULL
            CONSTRAINT DF_alert_config_reminder_interval_minutes DEFAULT (1440),
            reminder_time TIME(0) NOT NULL
            CONSTRAINT DF_alert_config_reminder_time DEFAULT ('07:30'),
            timezone NVARCHAR(100) NOT NULL
            CONSTRAINT DF_alert_config_timezone DEFAULT (N'America/Mexico_City'),
            quiet_hours_enabled BIT NOT NULL
            CONSTRAINT DF_alert_config_quiet_hours_enabled DEFAULT (1),
            quiet_start TIME(0) NOT NULL
            CONSTRAINT DF_alert_config_quiet_start DEFAULT ('18:00'),
            quiet_end TIME(0) NOT NULL
            CONSTRAINT DF_alert_config_quiet_end DEFAULT ('08:00'),
            open_respect_quiet_hours BIT NOT NULL
            CONSTRAINT DF_alert_config_open_respect_quiet_hours DEFAULT (0),
            resolution_respect_quiet_hours BIT NOT NULL
            CONSTRAINT DF_alert_config_resolution_respect_quiet_hours DEFAULT (0),
            reminder_respect_quiet_hours BIT NOT NULL
            CONSTRAINT DF_alert_config_reminder_respect_quiet_hours DEFAULT (1),
            sensor_lookback_hours INT NOT NULL
            CONSTRAINT DF_alert_config_lookback DEFAULT (1),
            trend_enabled BIT NOT NULL
            CONSTRAINT DF_alert_config_trend_enabled DEFAULT (1),
            trend_capture_interval_seconds INT NOT NULL
            CONSTRAINT DF_alert_config_capture_interval DEFAULT (21600),
            trend_metadata_key NVARCHAR(100) NOT NULL
            CONSTRAINT DF_alert_config_metadata_key DEFAULT (N'row_count'),
            trend_backfill_days INT NOT NULL
            CONSTRAINT DF_alert_config_backfill DEFAULT (28),
            trend_batch_size INT NOT NULL
            CONSTRAINT DF_alert_config_batch DEFAULT (500),
            max_notification_attempts SMALLINT NOT NULL
            CONSTRAINT DF_alert_config_attempts DEFAULT (3),
            retry_delays_minutes NVARCHAR(1000) NOT NULL
            CONSTRAINT DF_alert_config_retries DEFAULT (N'[5,15,60]'),
            processing_timeout_minutes INT NOT NULL
            CONSTRAINT DF_alert_config_processing_timeout DEFAULT (30),
            created_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_config_created DEFAULT (SYSUTCDATETIME()),
            updated_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_config_updated DEFAULT (SYSUTCDATETIME()),
            version ROWVERSION,
            CONSTRAINT CK_alert_config_evaluation_interval_seconds CHECK (evaluation_interval_seconds >= 30),
            CONSTRAINT CK_alert_config_delivery_interval_seconds CHECK (delivery_interval_seconds >= 30),
            CONSTRAINT CK_alert_config_no_readings_minutes CHECK (no_readings_minutes > 0),
            CONSTRAINT CK_alert_config_source_max_age_minutes CHECK (source_max_age_minutes > 0),
            CONSTRAINT CK_alert_config_expected_interval_minutes CHECK (expected_interval_minutes > 0),
            CONSTRAINT CK_alert_config_warning_drop_pct CHECK (warning_drop_pct BETWEEN 0 AND 100),
            CONSTRAINT CK_alert_config_critical_drop_pct CHECK (critical_drop_pct BETWEEN 0 AND 100 AND critical_drop_pct >= warning_drop_pct),
            CONSTRAINT CK_alert_config_minimum_observed_days_7d CHECK (minimum_observed_days_7d BETWEEN 1 AND 7),
            CONSTRAINT CK_alert_config_minimum_observed_days_28d CHECK (minimum_observed_days_28d BETWEEN 1 AND 28),
            CONSTRAINT CK_alert_config_open_mode CHECK (open_mode IN (N'IMMEDIATE', N'WITH_NEXT', N'OFF')),
            CONSTRAINT CK_alert_config_resolution_mode CHECK (resolution_mode IN (N'IMMEDIATE', N'WITH_NEXT', N'OFF')),
            CONSTRAINT CK_alert_config_reminder_mode CHECK (reminder_mode IN (N'INTERVAL', N'DAILY', N'WITH_NEXT', N'OFF')),
            CONSTRAINT CK_alert_config_reminder_interval_minutes CHECK (reminder_interval_minutes > 0),
            CONSTRAINT CK_alert_config_severity CHECK (severity IN (N'INFO', N'WARNING', N'CRITICAL')),
            CONSTRAINT CK_alert_config_processing CHECK (
                sensor_lookback_hours > 0
                AND trend_capture_interval_seconds >= 30
                AND trend_backfill_days > 0
                AND trend_batch_size BETWEEN 1 AND 1000
                AND max_notification_attempts > 0
                AND processing_timeout_minutes > 0
                AND ISJSON(retry_delays_minutes) = 1
            )
        );
    END;
GO

IF OBJECT_ID(N'alerting.alert_rule', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_rule (
            rule_id INT IDENTITY (1, 1) NOT NULL
            CONSTRAINT PK_alert_rule PRIMARY KEY,
            alert_type NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            entity_id NVARCHAR(250) COLLATE Latin1_General_100_BIN2 NOT NULL
            CONSTRAINT DF_alert_rule_entity DEFAULT (N''),
            enabled BIT NULL,
            severity NVARCHAR(20) NULL,
            evaluation_interval_seconds INT NULL,
            delivery_interval_seconds INT NULL,
            no_readings_minutes INT NULL,
            source_max_age_minutes INT NULL,
            expected_interval_minutes INT NULL,
            warning_drop_pct DECIMAL(6, 2) NULL,
            critical_drop_pct DECIMAL(6, 2) NULL,
            minimum_observed_days_7d TINYINT NULL,
            minimum_observed_days_28d TINYINT NULL,
            open_mode NVARCHAR(20) NULL,
            resolution_mode NVARCHAR(20) NULL,
            reminder_mode NVARCHAR(20) NULL,
            reminder_interval_minutes INT NULL,
            reminder_time TIME(0) NULL,
            timezone NVARCHAR(100) NULL,
            quiet_hours_enabled BIT NULL,
            quiet_start TIME(0) NULL,
            quiet_end TIME(0) NULL,
            open_respect_quiet_hours BIT NULL,
            resolution_respect_quiet_hours BIT NULL,
            reminder_respect_quiet_hours BIT NULL,
            ticket_enabled BIT NOT NULL
            CONSTRAINT DF_alert_rule_ticket_enabled DEFAULT (0),
            ticket_provider NVARCHAR(100) NULL,
            ticket_destination NVARCHAR(250) NULL,
            ticket_category NVARCHAR(100) NULL,
            ticket_priority NVARCHAR(50) NULL,
            ticket_assignment_group NVARCHAR(250) NULL,
            ticket_create_after_minutes INT NULL,
            ticket_update_on_reminder BIT NULL,
            ticket_close_on_resolution BIT NULL,
            ticket_recurrence_mode NVARCHAR(20) NULL,
            created_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_rule_created DEFAULT (SYSUTCDATETIME()),
            updated_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_rule_updated DEFAULT (SYSUTCDATETIME()),
            version ROWVERSION,
            CONSTRAINT UQ_alert_rule_target UNIQUE (alert_type, entity_id),
            CONSTRAINT CK_alert_rule_type CHECK (alert_type IN (
                N'connection_lost', N'late_readings', N'pipeline_failure',
                N'pipeline_stale', N'dagster_run_failure',
                N'asset_row_count_trend'
            )),
            CONSTRAINT CK_alert_rule_evaluation_interval_seconds CHECK (evaluation_interval_seconds >= 30),
            CONSTRAINT CK_alert_rule_delivery_interval_seconds CHECK (delivery_interval_seconds >= 30),
            CONSTRAINT CK_alert_rule_no_readings_minutes CHECK (no_readings_minutes > 0),
            CONSTRAINT CK_alert_rule_source_max_age_minutes CHECK (source_max_age_minutes > 0),
            CONSTRAINT CK_alert_rule_expected_interval_minutes CHECK (expected_interval_minutes > 0),
            CONSTRAINT CK_alert_rule_warning_drop_pct CHECK (warning_drop_pct BETWEEN 0 AND 100),
            CONSTRAINT CK_alert_rule_critical_drop_pct CHECK (critical_drop_pct BETWEEN 0 AND 100 AND critical_drop_pct >= warning_drop_pct),
            CONSTRAINT CK_alert_rule_minimum_observed_days_7d CHECK (minimum_observed_days_7d BETWEEN 1 AND 7),
            CONSTRAINT CK_alert_rule_minimum_observed_days_28d CHECK (minimum_observed_days_28d BETWEEN 1 AND 28),
            CONSTRAINT CK_alert_rule_open_mode CHECK (open_mode IN (N'IMMEDIATE', N'WITH_NEXT', N'OFF')),
            CONSTRAINT CK_alert_rule_resolution_mode CHECK (resolution_mode IN (N'IMMEDIATE', N'WITH_NEXT', N'OFF')),
            CONSTRAINT CK_alert_rule_reminder_mode CHECK (reminder_mode IN (N'INTERVAL', N'DAILY', N'WITH_NEXT', N'OFF')),
            CONSTRAINT CK_alert_rule_reminder_interval_minutes CHECK (reminder_interval_minutes > 0),
            CONSTRAINT CK_alert_rule_severity CHECK (severity IN (N'INFO', N'WARNING', N'CRITICAL')),
            CONSTRAINT CK_alert_rule_ticket_delay
            CHECK (ticket_create_after_minutes >= 0),
            CONSTRAINT CK_alert_rule_ticket_recurrence CHECK (
                ticket_recurrence_mode IN (N'NEW', N'REOPEN', N'NONE')
            )
        );
    END;
GO
