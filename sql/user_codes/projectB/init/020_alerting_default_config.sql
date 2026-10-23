SET QUOTED_IDENTIFIER ON;
SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

BEGIN TRY
    BEGIN TRANSACTION;

    IF
        NOT EXISTS (
            SELECT 1
            FROM alerting.alert_config WITH (UPDLOCK, HOLDLOCK)
            WHERE user_code = N'projectB'
        )
        BEGIN
            DECLARE @config_id UNIQUEIDENTIFIER = NEWID();
            DECLARE @draft_status_id UNIQUEIDENTIFIER;

            SELECT @draft_status_id = config_status_id
            FROM alerting.dim_config_status
            WHERE code = N'DRAFT' AND is_active = 1;

            IF @draft_status_id IS NULL
                THROW 51040, 'The DRAFT configuration status is unavailable.', 1;

            INSERT INTO alerting.alert_config (
                alert_config_id, user_code, version_number, config_status_id
            )
            VALUES (@config_id, N'projectB', 1, @draft_status_id);

            INSERT INTO alerting.alert_rule (
                alert_rule_id, alert_config_id, alert_type_id, entity_key,
                is_enabled, severity_id, evaluation_interval_seconds
            )
            SELECT
                NEWID() AS alert_rule_id,
                @config_id AS alert_config_id,
                alert_type.alert_type_id,
                NULL AS entity_key,
                defaults.is_enabled,
                severity.severity_id,
                defaults.evaluation_interval_seconds
            FROM (
                VALUES
                (N'connection_lost', 1, N'CRITICAL', 300),
                (N'late_readings', 1, N'WARNING', 300),
                (N'pipeline_failure', 1, N'CRITICAL', 300),
                (N'pipeline_stale', 0, N'INFO', 300),
                (N'dagster_run_failure', 1, N'WARNING', 300),
                (N'asset_row_count_trend', 1, N'WARNING', 3600),
                (N'telemetry_device_configuration', 1, N'WARNING', 3600),
                (N'telemetry_alert_configuration', 1, N'WARNING', 3600)
            )
                AS defaults (
                    alert_type_code, is_enabled, severity_code,
                    evaluation_interval_seconds
                )
            INNER JOIN alerting.dim_alert_type AS alert_type
                ON
                    defaults.alert_type_code = alert_type.code
                    AND alert_type.is_active = 1
            INNER JOIN alerting.dim_severity AS severity
                ON
                    defaults.severity_code = severity.code
                    AND severity.is_active = 1;

            IF (
                SELECT COUNT(*)
                FROM alerting.alert_rule
                WHERE alert_config_id = @config_id
            ) <> 8
                THROW 51041, 'The default alert rules are incomplete.', 1;

            INSERT INTO alerting.alert_rule_no_readings (
                alert_rule_id, no_readings_minutes, source_max_age_minutes
            )
            SELECT
                alert_rule.alert_rule_id,
                CASE WHEN alert_type.code = N'late_readings' THEN 59 ELSE 1440 END AS no_readings_minutes,
                2880 AS source_max_age_minutes
            FROM alerting.alert_rule AS alert_rule
            INNER JOIN alerting.dim_alert_type AS alert_type
                ON alert_rule.alert_type_id = alert_type.alert_type_id
            WHERE
                alert_rule.alert_config_id = @config_id
                AND alert_type.code IN (N'connection_lost', N'late_readings');

            INSERT INTO alerting.alert_rule_pipeline_stale (
                alert_rule_id, expected_interval_minutes
            )
            SELECT
                alert_rule.alert_rule_id,
                1440 AS expected_interval_minutes
            FROM alerting.alert_rule AS alert_rule
            INNER JOIN alerting.dim_alert_type AS alert_type
                ON alert_rule.alert_type_id = alert_type.alert_type_id
            WHERE
                alert_rule.alert_config_id = @config_id
                AND alert_type.code = N'pipeline_stale';

            INSERT INTO alerting.alert_rule_row_count_trend (
                alert_rule_id, warning_drop_pct, critical_drop_pct,
                minimum_observed_days_7d, minimum_observed_days_28d,
                critical_severity_id
            )
            SELECT
                alert_rule.alert_rule_id,
                20 AS warning_drop_pct,
                50 AS critical_drop_pct,
                7 AS minimum_observed_days_7d,
                28 AS minimum_observed_days_28d,
                critical_severity.severity_id
            FROM alerting.alert_rule AS alert_rule
            INNER JOIN alerting.dim_alert_type AS alert_type
                ON alert_rule.alert_type_id = alert_type.alert_type_id
            INNER JOIN alerting.dim_severity AS critical_severity
                ON
                    critical_severity.code = N'CRITICAL'
                    AND critical_severity.is_active = 1
            WHERE
                alert_rule.alert_config_id = @config_id
                AND alert_type.code = N'asset_row_count_trend';

            INSERT INTO alerting.alert_detector_operational_config (
                alert_config_id, sensor_lookback_hours,
                dagster_failure_lookback_minutes, dagster_failure_batch_size
            )
            VALUES (@config_id, 72, 2880, 200);

            INSERT INTO alerting.alert_detector_row_count_config (
                alert_config_id, capture_interval_seconds,
                metadata_key, backfill_days, batch_size
            )
            VALUES (@config_id, 3600, N'row_count', 30, 400);

            IF
                EXISTS (
                    SELECT 1
                    FROM serving.vwValidacionConfiguracionesAlerta
                    WHERE
                        ConfiguracionAlertaId = @config_id
                        AND EsBloqueante = 1
                )
                THROW 51042, 'The default alert configuration is invalid.', 1;

            EXEC alerting.usp_AlertConfig_Activate
                @alert_config_id = @config_id;

        END;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    THROW;
END CATCH;
GO
