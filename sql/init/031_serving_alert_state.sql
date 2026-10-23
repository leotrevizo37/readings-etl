SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER VIEW serving.vwEstadoAlertas
AS
WITH ticket_attempt_events AS (
    SELECT
        event.action_id,
        event.attempt_number,
        MIN(event.occurred_at) AS first_attempt_at
    FROM alerting.alert_action_event AS event
    INNER JOIN alerting.dim_action_type AS action_type
        ON event.action_type_id = action_type.action_type_id
    INNER JOIN alerting.dim_action_operation AS operation
        ON event.action_operation_id = operation.action_operation_id
    WHERE
        action_type.code = N'TICKET'
        AND operation.code = N'CREATE'
        AND event.attempt_number > 0
    GROUP BY
        event.action_id,
        event.attempt_number
),

email_attempt_events AS (
    SELECT
        event.action_id,
        event.attempt_number,
        MIN(event.occurred_at) AS first_attempt_at
    FROM alerting.alert_action_event AS event
    INNER JOIN alerting.dim_action_type AS action_type
        ON event.action_type_id = action_type.action_type_id
    INNER JOIN alerting.dim_action_operation AS operation
        ON event.action_operation_id = operation.action_operation_id
    WHERE
        action_type.code = N'EMAIL'
        AND operation.code = N'SEND'
        AND event.attempt_number > 0
    GROUP BY
        event.action_id,
        event.attempt_number
)

SELECT
    incident.alert_id AS AlertaId,
    incident.alert_code AS CodigoAlerta,
    incident.user_code AS CodigoUsuario,
    alert_type.alert_type_id AS TipoAlertaId,
    alert_type.code AS TipoAlertaCodigo,
    alert_type.name_es AS TipoAlertaNombre,
    severity.severity_id AS SeveridadId,
    severity.code AS SeveridadCodigo,
    severity.name_es AS SeveridadNombre,
    trigger_source.trigger_source_id AS FuenteDisparoId,
    trigger_source.code AS FuenteDisparoCodigo,
    trigger_source.name_es AS FuenteDisparoNombre,
    incident.entity_id AS EntidadId,
    incident.entity_key AS ClaveEntidad,
    incident.alert_config_id AS ConfiguracionAlertaId,
    incident.alert_rule_id AS ReglaAlertaId,
    detection.event_id AS EventoDeteccionId,
    detection.occurred_at AS FechaOcurrenciaDeteccion,
    detection.recorded_at AS FechaRegistroDeteccion,
    clearance.event_id AS EventoFinCondicionId,
    clearance.occurred_at AS FechaOcurrenciaFinCondicion,
    clearance.recorded_at AS FechaRegistroFinCondicion,
    CAST(CASE WHEN incident.is_point_in_time = 0 THEN 1 ELSE 0 END AS BIT)
        AS EsCondicionContinua,
    CAST(
        CASE
            WHEN incident.is_point_in_time = 1 THEN NULL
            WHEN incident.cleared_at IS NULL THEN 1
            ELSE 0
        END
        AS BIT
    ) AS CondicionActiva,
    detection.details AS DetalleDeteccion,
    detection.metadata_json AS MetadatosDeteccionJson,
    workflow.workflow_event_id AS EventoEstadoAlertaId,
    workflow.workflow_status_id AS EstadoAlertaId,
    workflow.status_code AS EstadoAlertaCodigo,
    workflow.status_name AS EstadoAlertaNombre,
    workflow.occurred_at AS FechaOcurrenciaEstadoAlerta,
    workflow.recorded_at AS FechaRegistroEstadoAlerta,
    workflow.closure_type_id AS TipoCierreId,
    workflow.closure_code AS TipoCierreCodigo,
    workflow.closure_name AS TipoCierreNombre,
    workflow.closed_by AS CerradaPor,
    workflow.closing_note AS NotaCierre,
    ticket_action.action_id AS AccionTicketId,
    ticket_reference.external_reference AS TicketIdExterno,
    ticket_metrics.created_at AS FechaCreacionTicket,
    ticket_attempts.last_attempt_at AS FechaUltimoIntentoCreacionTicket,
    ticket_attempts.attempt_count AS CantidadIntentosCreacionTicket,
    ticket_latest.action_status_id AS EstadoUltimaAccionTicketId,
    ticket_latest.status_code AS EstadoUltimaAccionTicketCodigo,
    ticket_latest.status_name AS EstadoUltimaAccionTicketNombre,
    ticket_latest.error_code AS UltimoCodigoErrorTicket,
    ticket_latest.error_message AS UltimoMensajeErrorTicket,
    email_action.action_id AS AccionCorreoId,
    email_attempts.last_attempt_at AS FechaUltimoIntentoCorreo,
    email_attempts.attempt_count AS CantidadIntentosCorreo,
    email_metrics.last_succeeded_at AS FechaUltimoCorreoExitoso,
    email_latest.action_status_id AS EstadoUltimaAccionCorreoId,
    email_latest.status_code AS EstadoUltimaAccionCorreoCodigo,
    email_latest.status_name AS EstadoUltimaAccionCorreoNombre,
    email_latest.error_code AS UltimoCodigoErrorCorreo,
    email_latest.error_message AS UltimoMensajeErrorCorreo,
    incident.is_legacy AS EsLegada,
    incident.legacy_alert_key AS ClaveLegada
FROM alerting.alert_incident AS incident
INNER JOIN alerting.dim_alert_type AS alert_type
    ON incident.alert_type_id = alert_type.alert_type_id
LEFT JOIN alerting.dim_severity AS severity
    ON incident.severity_id = severity.severity_id
INNER JOIN alerting.dim_trigger_source AS trigger_source
    ON incident.trigger_source_id = trigger_source.trigger_source_id
OUTER APPLY (
    SELECT TOP 1
        signal.event_id,
        signal.occurred_at,
        signal.recorded_at,
        signal.details,
        signal.metadata_json
    FROM alerting.alert_signal_event AS signal
    INNER JOIN alerting.dim_signal_type AS signal_type
        ON signal.signal_type_id = signal_type.signal_type_id
    WHERE
        signal.alert_id = incident.alert_id
        AND signal_type.code = N'DETECTED'
    ORDER BY signal.event_sequence DESC
) AS detection
OUTER APPLY (
    SELECT TOP 1
        signal.event_id,
        signal.occurred_at,
        signal.recorded_at
    FROM alerting.alert_signal_event AS signal
    INNER JOIN alerting.dim_signal_type AS signal_type
        ON signal.signal_type_id = signal_type.signal_type_id
    WHERE
        signal.alert_id = incident.alert_id
        AND signal_type.code = N'CLEARED'
    ORDER BY signal.event_sequence DESC
) AS clearance
OUTER APPLY (
    SELECT TOP 1
        event.workflow_event_id,
        event.workflow_status_id,
        status.code AS status_code,
        status.name_es AS status_name,
        event.occurred_at,
        event.recorded_at,
        event.closure_type_id,
        closure.code AS closure_code,
        closure.name_es AS closure_name,
        event.closed_by,
        event.closing_note
    FROM alerting.alert_workflow_event AS event
    INNER JOIN alerting.dim_workflow_status AS status
        ON event.workflow_status_id = status.workflow_status_id
    LEFT JOIN alerting.dim_closure_type AS closure
        ON event.closure_type_id = closure.closure_type_id
    WHERE event.alert_id = incident.alert_id
    ORDER BY event.event_sequence DESC
) AS workflow
OUTER APPLY (
    SELECT TOP 1 event.action_id
    FROM alerting.alert_action_event AS event
    INNER JOIN alerting.dim_action_type AS action_type
        ON event.action_type_id = action_type.action_type_id
    INNER JOIN alerting.dim_action_operation AS operation
        ON event.action_operation_id = operation.action_operation_id
    WHERE
        event.alert_id = incident.alert_id
        AND action_type.code = N'TICKET'
        AND operation.code = N'CREATE'
    ORDER BY event.event_sequence DESC
) AS ticket_action
OUTER APPLY (
    SELECT TOP 1
        event.action_status_id,
        status.code AS status_code,
        status.name_es AS status_name,
        event.error_code,
        event.error_message
    FROM alerting.alert_action_event AS event
    INNER JOIN alerting.dim_action_status AS status
        ON event.action_status_id = status.action_status_id
    INNER JOIN alerting.dim_action_type AS action_type
        ON event.action_type_id = action_type.action_type_id
    INNER JOIN alerting.dim_action_operation AS operation
        ON event.action_operation_id = operation.action_operation_id
    WHERE
        event.action_id = ticket_action.action_id
        AND action_type.code = N'TICKET'
        AND operation.code = N'CREATE'
    ORDER BY event.event_sequence DESC
) AS ticket_latest
OUTER APPLY (
    SELECT TOP 1 event.external_reference
    FROM alerting.alert_action_event AS event
    INNER JOIN alerting.dim_action_type AS action_type
        ON event.action_type_id = action_type.action_type_id
    INNER JOIN alerting.dim_action_operation AS operation
        ON event.action_operation_id = operation.action_operation_id
    WHERE
        event.action_id = ticket_action.action_id
        AND action_type.code = N'TICKET'
        AND operation.code = N'CREATE'
        AND event.external_reference IS NOT NULL
    ORDER BY event.event_sequence DESC
) AS ticket_reference
OUTER APPLY (
    SELECT MIN(event.occurred_at) AS created_at
    FROM alerting.alert_action_event AS event
    INNER JOIN alerting.dim_action_status AS status
        ON event.action_status_id = status.action_status_id
    INNER JOIN alerting.dim_action_type AS action_type
        ON event.action_type_id = action_type.action_type_id
    INNER JOIN alerting.dim_action_operation AS operation
        ON event.action_operation_id = operation.action_operation_id
    WHERE
        event.action_id = ticket_action.action_id
        AND action_type.code = N'TICKET'
        AND operation.code = N'CREATE'
        AND status.code IN (N'SUCCEEDED', N'DELIVERED')
) AS ticket_metrics
OUTER APPLY (
    SELECT
        COUNT(*) AS attempt_count,
        MAX(attempt.first_attempt_at) AS last_attempt_at
    FROM ticket_attempt_events AS attempt
    WHERE attempt.action_id = ticket_action.action_id
) AS ticket_attempts
OUTER APPLY (
    SELECT TOP 1 event.action_id
    FROM alerting.alert_action_event AS event
    INNER JOIN alerting.dim_action_type AS action_type
        ON event.action_type_id = action_type.action_type_id
    INNER JOIN alerting.dim_action_operation AS operation
        ON event.action_operation_id = operation.action_operation_id
    WHERE
        event.alert_id = incident.alert_id
        AND action_type.code = N'EMAIL'
        AND operation.code = N'SEND'
    ORDER BY event.event_sequence DESC
) AS email_action
OUTER APPLY (
    SELECT TOP 1
        event.action_status_id,
        status.code AS status_code,
        status.name_es AS status_name,
        event.error_code,
        event.error_message
    FROM alerting.alert_action_event AS event
    INNER JOIN alerting.dim_action_status AS status
        ON event.action_status_id = status.action_status_id
    INNER JOIN alerting.dim_action_type AS action_type
        ON event.action_type_id = action_type.action_type_id
    INNER JOIN alerting.dim_action_operation AS operation
        ON event.action_operation_id = operation.action_operation_id
    WHERE
        event.action_id = email_action.action_id
        AND action_type.code = N'EMAIL'
        AND operation.code = N'SEND'
    ORDER BY event.event_sequence DESC
) AS email_latest
OUTER APPLY (
    SELECT MAX(event.occurred_at) AS last_succeeded_at
    FROM alerting.alert_action_event AS event
    INNER JOIN alerting.dim_action_status AS status
        ON event.action_status_id = status.action_status_id
    INNER JOIN alerting.dim_action_type AS action_type
        ON event.action_type_id = action_type.action_type_id
    INNER JOIN alerting.dim_action_operation AS operation
        ON event.action_operation_id = operation.action_operation_id
    WHERE
        event.action_id = email_action.action_id
        AND action_type.code = N'EMAIL'
        AND operation.code = N'SEND'
        AND status.code IN (N'SUCCEEDED', N'DELIVERED')
) AS email_metrics
OUTER APPLY (
    SELECT
        COUNT(*) AS attempt_count,
        MAX(attempt.first_attempt_at) AS last_attempt_at
    FROM email_attempt_events AS attempt
    WHERE attempt.action_id = email_action.action_id
) AS email_attempts;
GO
