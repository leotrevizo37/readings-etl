SET QUOTED_IDENTIFIER ON;
GO

IF
    NOT EXISTS (
        SELECT 1 FROM sys.schemas
        WHERE name = N'serving'
    )
    BEGIN
        EXEC ('CREATE SCHEMA serving');
    END;
GO

CREATE OR ALTER VIEW serving.vwValidacionConfiguracionesAlerta
AS
WITH Configuraciones AS (
    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        status.code AS status_code
    FROM alerting.alert_config AS config
    INNER JOIN alerting.dim_config_status AS status
        ON config.config_status_id = status.config_status_id
),

Problemas AS (
    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        CAST(NULL AS UNIQUEIDENTIFIER) AS alert_rule_id,
        alert_type.code AS alert_type_code,
        CAST(NULL AS NVARCHAR(450)) AS entity_key,
        CAST(N'REGLA_DEFAULT_FALTANTE' AS NVARCHAR(100)) AS validation_code,
        CAST(
            N'Falta la regla default completa para ' + alert_type.code + N'.'
            AS NVARCHAR(500)
        ) AS validation_description
    FROM Configuraciones AS config
    CROSS JOIN alerting.dim_alert_type AS alert_type
    WHERE
        alert_type.is_active = 1
        AND NOT EXISTS (
            SELECT 1
            FROM alerting.alert_rule AS alert_rule
            WHERE
                alert_rule.alert_config_id = config.alert_config_id
                AND alert_rule.alert_type_id = alert_type.alert_type_id
                AND alert_rule.entity_key IS NULL
        )

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        NULL AS alert_rule_id,
        NULL AS alert_type_code,
        NULL AS entity_key,
        N'CONFIG_DETECTOR_OPERATIVO_FALTANTE' AS validation_code,
        N'Falta la configuración técnica del detector operativo.'
            AS validation_description
    FROM Configuraciones AS config
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM alerting.alert_detector_operational_config AS detector
            WHERE detector.alert_config_id = config.alert_config_id
        )

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        NULL AS alert_rule_id,
        NULL AS alert_type_code,
        NULL AS entity_key,
        N'CONFIG_DETECTOR_TENDENCIA_FALTANTE' AS validation_code,
        N'Falta la configuración técnica del detector de tendencias.'
            AS validation_description
    FROM Configuraciones AS config
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM alerting.alert_detector_row_count_config AS detector
            WHERE detector.alert_config_id = config.alert_config_id
        )

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        alert_rule.alert_rule_id,
        alert_type.code,
        alert_rule.entity_key,
        N'DETALLE_LECTURAS_FALTANTE' AS validation_code,
        N'La regla requiere umbrales completos de ausencia de lecturas.'
            AS validation_description
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_rule AS alert_rule
        ON config.alert_config_id = alert_rule.alert_config_id
    INNER JOIN alerting.dim_alert_type AS alert_type
        ON alert_rule.alert_type_id = alert_type.alert_type_id
    WHERE
        alert_type.code IN (N'connection_lost', N'late_readings')
        AND NOT EXISTS (
            SELECT 1
            FROM alerting.alert_rule_no_readings AS detail
            WHERE detail.alert_rule_id = alert_rule.alert_rule_id
        )

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        alert_rule.alert_rule_id,
        alert_type.code,
        alert_rule.entity_key,
        N'DETALLE_PIPELINE_ATRASADO_FALTANTE' AS validation_code,
        N'La regla requiere un intervalo esperado de ejecución.'
            AS validation_description
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_rule AS alert_rule
        ON config.alert_config_id = alert_rule.alert_config_id
    INNER JOIN alerting.dim_alert_type AS alert_type
        ON alert_rule.alert_type_id = alert_type.alert_type_id
    WHERE
        alert_type.code = N'pipeline_stale'
        AND NOT EXISTS (
            SELECT 1
            FROM alerting.alert_rule_pipeline_stale AS detail
            WHERE detail.alert_rule_id = alert_rule.alert_rule_id
        )

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        alert_rule.alert_rule_id,
        alert_type.code,
        alert_rule.entity_key,
        N'DETALLE_TENDENCIA_FALTANTE' AS validation_code,
        N'La regla requiere umbrales completos de tendencia de volumen.'
            AS validation_description
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_rule AS alert_rule
        ON config.alert_config_id = alert_rule.alert_config_id
    INNER JOIN alerting.dim_alert_type AS alert_type
        ON alert_rule.alert_type_id = alert_type.alert_type_id
    WHERE
        alert_type.code = N'asset_row_count_trend'
        AND NOT EXISTS (
            SELECT 1
            FROM alerting.alert_rule_row_count_trend AS detail
            WHERE detail.alert_rule_id = alert_rule.alert_rule_id
        )

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        alert_rule.alert_rule_id,
        alert_type.code,
        alert_rule.entity_key,
        N'DETALLE_LECTURAS_NO_PERMITIDO' AS validation_code,
        N'La regla contiene umbrales de lecturas que no corresponden a su tipo.'
            AS validation_description
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_rule AS alert_rule
        ON config.alert_config_id = alert_rule.alert_config_id
    INNER JOIN alerting.dim_alert_type AS alert_type
        ON alert_rule.alert_type_id = alert_type.alert_type_id
    INNER JOIN alerting.alert_rule_no_readings AS detail
        ON alert_rule.alert_rule_id = detail.alert_rule_id
    WHERE alert_type.code NOT IN (N'connection_lost', N'late_readings')

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        alert_rule.alert_rule_id,
        alert_type.code,
        alert_rule.entity_key,
        N'DETALLE_PIPELINE_ATRASADO_NO_PERMITIDO' AS validation_code,
        N'La regla contiene un intervalo de pipeline que no corresponde a su tipo.'
            AS validation_description
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_rule AS alert_rule
        ON config.alert_config_id = alert_rule.alert_config_id
    INNER JOIN alerting.dim_alert_type AS alert_type
        ON alert_rule.alert_type_id = alert_type.alert_type_id
    INNER JOIN alerting.alert_rule_pipeline_stale AS detail
        ON alert_rule.alert_rule_id = detail.alert_rule_id
    WHERE alert_type.code <> N'pipeline_stale'

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        alert_rule.alert_rule_id,
        alert_type.code,
        alert_rule.entity_key,
        N'DETALLE_TENDENCIA_NO_PERMITIDO' AS validation_code,
        N'La regla contiene umbrales de tendencia que no corresponden a su tipo.'
            AS validation_description
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_rule AS alert_rule
        ON config.alert_config_id = alert_rule.alert_config_id
    INNER JOIN alerting.dim_alert_type AS alert_type
        ON alert_rule.alert_type_id = alert_type.alert_type_id
    INNER JOIN alerting.alert_rule_row_count_trend AS detail
        ON alert_rule.alert_rule_id = detail.alert_rule_id
    WHERE alert_type.code <> N'asset_row_count_trend'

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        NULL AS alert_rule_id,
        NULL AS alert_type_code,
        NULL AS entity_key,
        N'CODIGO_USUARIO_INVALIDO' AS validation_code,
        N'El código de usuario no puede estar vacío.' AS validation_description
    FROM Configuraciones AS config
    WHERE LEN(LTRIM(RTRIM(config.user_code))) = 0

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        NULL AS alert_rule_id,
        NULL AS alert_type_code,
        NULL AS entity_key,
        N'CODIGO_USUARIO_INCONSISTENTE' AS validation_code,
        N'El código de usuario debe coincidir con el historial del tenant.'
            AS validation_description
    FROM Configuraciones AS config
    WHERE
        EXISTS (
            SELECT 1
            FROM alerting.alert_config AS previous_config
            INNER JOIN alerting.dim_config_status AS previous_status
                ON
                    previous_config.config_status_id
                    = previous_status.config_status_id
            WHERE
                previous_config.alert_config_id <> config.alert_config_id
                AND previous_config.user_code <> config.user_code
                AND previous_status.code IN (N'ACTIVE', N'RETIRED')
        )

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        NULL AS alert_rule_id,
        NULL AS alert_type_code,
        NULL AS entity_key,
        N'CLAVE_METADATA_TENDENCIA_INVALIDA' AS validation_code,
        N'El detector de tendencias debe leer la metadata row_count.'
            AS validation_description
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_detector_row_count_config AS detector
        ON config.alert_config_id = detector.alert_config_id
    WHERE detector.metadata_key COLLATE Latin1_General_100_BIN2 <> N'row_count'

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        alert_rule.alert_rule_id,
        alert_type.code,
        alert_rule.entity_key,
        N'CLAVE_SENSOR_INVALIDA' AS validation_code,
        N'La clave del sensor debe ser un GUID canónico en minúsculas.'
            AS validation_description
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_rule AS alert_rule
        ON config.alert_config_id = alert_rule.alert_config_id
    INNER JOIN alerting.dim_alert_type AS alert_type
        ON alert_rule.alert_type_id = alert_type.alert_type_id
    WHERE
        alert_type.code IN (N'connection_lost', N'late_readings')
        AND alert_rule.entity_key IS NOT NULL
        AND (
            TRY_CONVERT(UNIQUEIDENTIFIER, alert_rule.entity_key) IS NULL
            OR alert_rule.entity_key <> LOWER(CAST(TRY_CONVERT(UNIQUEIDENTIFIER, alert_rule.entity_key) AS NVARCHAR(36)))
        )

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        alert_rule.alert_rule_id,
        alert_type.code,
        alert_rule.entity_key,
        N'TIPO_ALERTA_INACTIVO' AS validation_code,
        N'La regla referencia un tipo de alerta inactivo.' AS validation_description
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_rule AS alert_rule
        ON config.alert_config_id = alert_rule.alert_config_id
    INNER JOIN alerting.dim_alert_type AS alert_type
        ON alert_rule.alert_type_id = alert_type.alert_type_id
    WHERE alert_type.is_active = 0

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        alert_rule.alert_rule_id,
        alert_type.code,
        alert_rule.entity_key,
        N'SEVERIDAD_INACTIVA' AS validation_code,
        N'La regla referencia una severidad inactiva.' AS validation_description
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_rule AS alert_rule
        ON config.alert_config_id = alert_rule.alert_config_id
    INNER JOIN alerting.dim_alert_type AS alert_type
        ON alert_rule.alert_type_id = alert_type.alert_type_id
    INNER JOIN alerting.dim_severity AS severity
        ON alert_rule.severity_id = severity.severity_id
    WHERE severity.is_active = 0

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        alert_rule.alert_rule_id,
        alert_type.code,
        alert_rule.entity_key,
        N'SEVERIDAD_CRITICA_TENDENCIA_INVALIDA' AS validation_code,
        N'La severidad crítica de tendencia debe usar el catálogo CRITICAL activo.'
            AS validation_description
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_rule AS alert_rule
        ON config.alert_config_id = alert_rule.alert_config_id
    INNER JOIN alerting.dim_alert_type AS alert_type
        ON alert_rule.alert_type_id = alert_type.alert_type_id
    INNER JOIN alerting.alert_rule_row_count_trend AS detail
        ON alert_rule.alert_rule_id = detail.alert_rule_id
    INNER JOIN alerting.dim_severity AS critical_severity
        ON detail.critical_severity_id = critical_severity.severity_id
    WHERE
        alert_type.code = N'asset_row_count_trend'
        AND (
            critical_severity.code <> N'CRITICAL'
            OR critical_severity.is_active = 0
        )

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        NULL AS alert_rule_id,
        NULL AS alert_type_code,
        NULL AS entity_key,
        N'VERSION_CONFIGURACION_NO_VIGENTE' AS validation_code,
        N'La versión debe ser posterior a todas las versiones activas o retiradas.'
            AS validation_description
    FROM Configuraciones AS config
    WHERE
        config.status_code IN (N'DRAFT', N'ACTIVE')
        AND EXISTS (
            SELECT 1
            FROM alerting.alert_config AS previous_config
            INNER JOIN alerting.dim_config_status AS previous_status
                ON
                    previous_config.config_status_id
                    = previous_status.config_status_id
            WHERE
                previous_config.alert_config_id <> config.alert_config_id
                AND previous_config.user_code = config.user_code
                AND previous_status.code IN (N'ACTIVE', N'RETIRED')
                AND previous_config.version_number >= config.version_number
        )
)

SELECT
    alert_config_id AS ConfiguracionAlertaId,
    user_code AS CodigoUsuario,
    version_number AS NumeroVersion,
    config_status_id AS EstadoConfiguracionId,
    status_code AS EstadoConfiguracionCodigo,
    alert_rule_id AS ReglaAlertaId,
    alert_type_code AS TipoAlertaCodigo,
    entity_key AS ClaveEntidad,
    validation_code AS CodigoValidacion,
    validation_description AS DescripcionValidacion,
    CAST(1 AS BIT) AS EsBloqueante
FROM Problemas;
GO
