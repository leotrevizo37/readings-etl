SET QUOTED_IDENTIFIER ON;
GO

MERGE alerting.dim_alert_type AS target
USING (
    VALUES
    ('10000000-0000-0000-0000-000000000001', N'connection_lost', N'Comunicación perdida', N'El sensor superó el tiempo permitido sin comunicar lecturas.', 1, 1, 10),
    ('10000000-0000-0000-0000-000000000002', N'late_readings', N'Lecturas retrasadas', N'El sensor superó el umbral configurado para lecturas retrasadas.', 1, 1, 20),
    ('10000000-0000-0000-0000-000000000003', N'pipeline_failure', N'Fallo de ingesta', N'La última ejecución terminal de una ingesta terminó con error.', 1, 1, 30),
    ('10000000-0000-0000-0000-000000000004', N'pipeline_stale', N'Ingesta atrasada', N'La última ejecución exitosa superó su intervalo esperado.', 1, 1, 40),
    ('10000000-0000-0000-0000-000000000005', N'dagster_run_failure', N'Fallo de ejecución Dagster', N'Una corrida Dagster falló mientras materializaba un asset comprobable.', 1, 1, 50),
    ('10000000-0000-0000-0000-000000000006', N'asset_row_count_trend', N'Caída de volumen del asset', N'El volumen materializado cayó frente a los promedios configurados.', 1, 1, 60),
    ('10000000-0000-0000-0000-000000000007', N'telemetry_device_configuration', N'Configuración de dispositivo sospechosa', N'Incongruencias entre dispositivos, sensores y sus tipos en TELEMETRY.', 1, 1, 70),
    ('10000000-0000-0000-0000-000000000008', N'telemetry_alert_configuration', N'Configuración de alerta sospechosa', N'Incongruencias entre condiciones TELEMETRY, sensores y rangos esperados.', 1, 1, 80)
)
    AS source (
        alert_type_id, code, name_es, description_es, is_continuous,
        is_active, sort_order
    )
    ON target.alert_type_id = source.alert_type_id
WHEN MATCHED AND target.code = source.code
    THEN
    UPDATE
        SET
            name_es = source.name_es,
            description_es = source.description_es,
            is_continuous = CASE
                WHEN target.code = N'dagster_run_failure' THEN target.is_continuous
                ELSE source.is_continuous
            END,
            is_active = source.is_active,
            sort_order = source.sort_order
WHEN NOT MATCHED
    THEN
    INSERT (
        alert_type_id, code, name_es, description_es, is_continuous,
        is_active, sort_order
    )
    VALUES (
        source.alert_type_id, source.code, source.name_es,
        source.description_es, source.is_continuous, source.is_active,
        source.sort_order
    );
GO

MERGE alerting.dim_signal_type AS target
USING (
    VALUES
    ('20000000-0000-0000-0000-000000000001', N'DETECTED', N'Detectada', N'Comenzó una condición comprobada por el detector.', 1, 10),
    ('20000000-0000-0000-0000-000000000002', N'CLEARED', N'Condición terminada', N'Existe evidencia posterior que demuestra que la condición terminó.', 1, 20)
) AS source (signal_type_id, code, name_es, description_es, is_active, sort_order)
    ON target.signal_type_id = source.signal_type_id
WHEN MATCHED AND target.code = source.code
    THEN
    UPDATE
        SET
            name_es = source.name_es,
            description_es = source.description_es,
            is_active = source.is_active,
            sort_order = source.sort_order
WHEN NOT MATCHED
    THEN
    INSERT (signal_type_id, code, name_es, description_es, is_active, sort_order)
    VALUES (
        source.signal_type_id, source.code, source.name_es,
        source.description_es, source.is_active, source.sort_order
    );
GO

MERGE alerting.dim_severity AS target
USING (
    VALUES
    ('30000000-0000-0000-0000-000000000001', N'INFO', N'Informativa', N'Condición informativa que no requiere atención inmediata.', 1, 10),
    ('30000000-0000-0000-0000-000000000002', N'WARNING', N'Advertencia', N'Condición que requiere seguimiento operativo.', 1, 20),
    ('30000000-0000-0000-0000-000000000003', N'CRITICAL', N'Crítica', N'Condición que requiere atención prioritaria.', 1, 30)
) AS source (severity_id, code, name_es, description_es, is_active, sort_order)
    ON target.severity_id = source.severity_id
WHEN MATCHED AND target.code = source.code
    THEN
    UPDATE
        SET
            name_es = source.name_es,
            description_es = source.description_es,
            is_active = source.is_active,
            sort_order = source.sort_order
WHEN NOT MATCHED
    THEN
    INSERT (severity_id, code, name_es, description_es, is_active, sort_order)
    VALUES (
        source.severity_id, source.code, source.name_es,
        source.description_es, source.is_active, source.sort_order
    );
GO

MERGE alerting.dim_trigger_source AS target
USING (
    VALUES
    ('40000000-0000-0000-0000-000000000001', N'sensor_monitoring', N'Monitoreo de sensores', N'Evidencia operativa producida por el monitoreo de lecturas IoT.', 1, 10),
    ('40000000-0000-0000-0000-000000000002', N'dagster_etl', N'Control de ingestiones', N'Evidencia de ejecución registrada en el control de ingestiones.', 1, 20),
    ('40000000-0000-0000-0000-000000000003', N'dagster', N'Dagster', N'Evidencia obtenida de corridas o materializaciones de Dagster.', 1, 30)
)
    AS source (
        trigger_source_id, code, name_es, description_es, is_active, sort_order
    )
    ON target.trigger_source_id = source.trigger_source_id
WHEN MATCHED AND target.code = source.code
    THEN
    UPDATE
        SET
            name_es = source.name_es,
            description_es = source.description_es,
            is_active = source.is_active,
            sort_order = source.sort_order
WHEN NOT MATCHED
    THEN
    INSERT (
        trigger_source_id, code, name_es, description_es, is_active, sort_order
    )
    VALUES (
        source.trigger_source_id, source.code, source.name_es,
        source.description_es, source.is_active, source.sort_order
    );
GO

MERGE alerting.dim_config_status AS target
USING (
    VALUES
    ('50000000-0000-0000-0000-000000000001', N'DRAFT', N'Borrador', N'Configuración editable que todavía no gobierna la detección.', 1, 10),
    ('50000000-0000-0000-0000-000000000002', N'ACTIVE', N'Activa', N'Única versión vigente que gobierna nuevas detecciones.', 1, 20),
    ('50000000-0000-0000-0000-000000000003', N'RETIRED', N'Retirada', N'Versión histórica inmutable que dejó de estar vigente.', 1, 30)
)
    AS source (
        config_status_id, code, name_es, description_es, is_active, sort_order
    )
    ON target.config_status_id = source.config_status_id
WHEN MATCHED AND target.code = source.code
    THEN
    UPDATE
        SET
            name_es = source.name_es,
            description_es = source.description_es,
            is_active = source.is_active,
            sort_order = source.sort_order
WHEN NOT MATCHED
    THEN
    INSERT (
        config_status_id, code, name_es, description_es, is_active, sort_order
    )
    VALUES (
        source.config_status_id, source.code, source.name_es,
        source.description_es, source.is_active, source.sort_order
    );
GO

MERGE alerting.dim_workflow_status AS target
USING (
    VALUES
    ('60000000-0000-0000-0000-000000000001', N'NEW', N'Nueva', N'La alerta fue recibida y aún no ha sido gestionada.', 1, 10),
    ('60000000-0000-0000-0000-000000000002', N'OPEN', N'Abierta', N'La alerta está abierta y requiere seguimiento.', 1, 20),
    ('60000000-0000-0000-0000-000000000003', N'ACKNOWLEDGED', N'Reconocida', N'Una entidad responsable confirmó que conoce la alerta.', 1, 30),
    ('60000000-0000-0000-0000-000000000004', N'IN_PROGRESS', N'En atención', N'La alerta se encuentra bajo atención operativa.', 1, 40),
    ('60000000-0000-0000-0000-000000000005', N'RESOLVED', N'Resuelta', N'La causa de la alerta fue resuelta.', 1, 50),
    ('60000000-0000-0000-0000-000000000006', N'CLOSED', N'Cerrada', N'El ciclo de vida de la alerta terminó.', 1, 60)
)
    AS source (
        workflow_status_id, code, name_es, description_es, is_active, sort_order
    )
    ON target.workflow_status_id = source.workflow_status_id
WHEN MATCHED AND target.code = source.code
    THEN
    UPDATE
        SET
            name_es = source.name_es,
            description_es = source.description_es,
            is_active = source.is_active,
            sort_order = source.sort_order
WHEN NOT MATCHED
    THEN
    INSERT (
        workflow_status_id, code, name_es, description_es, is_active, sort_order
    )
    VALUES (
        source.workflow_status_id, source.code, source.name_es,
        source.description_es, source.is_active, source.sort_order
    );
GO

MERGE alerting.dim_closure_type AS target
USING (
    VALUES
    ('70000000-0000-0000-0000-000000000001', N'RESOLVED', N'Resuelta', N'La causa fue atendida y dejó de requerir seguimiento.', 1, 10),
    ('70000000-0000-0000-0000-000000000002', N'FALSE_POSITIVE', N'Falso positivo', N'La evidencia no representaba una condición real.', 1, 20),
    ('70000000-0000-0000-0000-000000000003', N'DUPLICATE', N'Duplicada', N'La alerta corresponde a otra incidencia ya registrada.', 1, 30),
    ('70000000-0000-0000-0000-000000000004', N'CONFIGURATION_CHANGE', N'Cambio de configuración', N'La alerta se cerró como consecuencia de una decisión de configuración.', 1, 40),
    ('70000000-0000-0000-0000-000000000005', N'NO_ACTION_REQUIRED', N'Sin acción requerida', N'La revisión determinó que no era necesaria una acción.', 1, 50),
    ('70000000-0000-0000-0000-000000000006', N'CANCELLED', N'Cancelada', N'El seguimiento fue cancelado de forma explícita.', 1, 60)
)
    AS source (
        closure_type_id, code, name_es, description_es, is_active, sort_order
    )
    ON target.closure_type_id = source.closure_type_id
WHEN MATCHED AND target.code = source.code
    THEN
    UPDATE
        SET
            name_es = source.name_es,
            description_es = source.description_es,
            is_active = source.is_active,
            sort_order = source.sort_order
WHEN NOT MATCHED
    THEN
    INSERT (
        closure_type_id, code, name_es, description_es, is_active, sort_order
    )
    VALUES (
        source.closure_type_id, source.code, source.name_es,
        source.description_es, source.is_active, source.sort_order
    );
GO

MERGE alerting.dim_action_type AS target
USING (
    VALUES
    ('80000000-0000-0000-0000-000000000001', N'EMAIL', N'Correo electrónico', N'Entrega de una notificación por correo electrónico.', 1, 10),
    ('80000000-0000-0000-0000-000000000002', N'TICKET', N'Ticket', N'Gestión de un ticket en un sistema externo.', 1, 20),
    ('80000000-0000-0000-0000-000000000003', N'PUSH_NOTIFICATION', N'Notificación push', N'Entrega de una notificación push.', 1, 30),
    ('80000000-0000-0000-0000-000000000004', N'WHATSAPP', N'WhatsApp', N'Entrega de una notificación mediante WhatsApp.', 1, 40),
    ('80000000-0000-0000-0000-000000000005', N'TEAMS', N'Microsoft Teams', N'Entrega de una notificación mediante Microsoft Teams.', 1, 50)
)
    AS source (
        action_type_id, code, name_es, description_es, is_active, sort_order
    )
    ON target.action_type_id = source.action_type_id
WHEN MATCHED AND target.code = source.code
    THEN
    UPDATE
        SET
            name_es = source.name_es,
            description_es = source.description_es,
            is_active = source.is_active,
            sort_order = source.sort_order
WHEN NOT MATCHED
    THEN
    INSERT (
        action_type_id, code, name_es, description_es, is_active, sort_order
    )
    VALUES (
        source.action_type_id, source.code, source.name_es,
        source.description_es, source.is_active, source.sort_order
    );
GO

MERGE alerting.dim_action_operation AS target
USING (
    VALUES
    ('90000000-0000-0000-0000-000000000001', N'SEND', N'Enviar', N'Enviar una notificación mediante el canal seleccionado.', 1, 10),
    ('90000000-0000-0000-0000-000000000002', N'CREATE', N'Crear', N'Crear una entidad en el sistema externo.', 1, 20),
    ('90000000-0000-0000-0000-000000000003', N'UPDATE', N'Actualizar', N'Actualizar una entidad existente en el sistema externo.', 1, 30),
    ('90000000-0000-0000-0000-000000000004', N'CLOSE', N'Cerrar', N'Cerrar una entidad existente en el sistema externo.', 1, 40),
    ('90000000-0000-0000-0000-000000000005', N'REOPEN', N'Reabrir', N'Reabrir una entidad existente en el sistema externo.', 1, 50)
)
    AS source (
        action_operation_id, code, name_es, description_es, is_active, sort_order
    )
    ON target.action_operation_id = source.action_operation_id
WHEN MATCHED AND target.code = source.code
    THEN
    UPDATE
        SET
            name_es = source.name_es,
            description_es = source.description_es,
            is_active = source.is_active,
            sort_order = source.sort_order
WHEN NOT MATCHED
    THEN
    INSERT (
        action_operation_id, code, name_es, description_es, is_active,
        sort_order
    )
    VALUES (
        source.action_operation_id, source.code, source.name_es,
        source.description_es, source.is_active, source.sort_order
    );
GO

MERGE alerting.dim_action_status AS target
USING (
    VALUES
    ('a0000000-0000-0000-0000-000000000001', N'PENDING', N'Pendiente', N'La acción está registrada y todavía no inicia.', 1, 10),
    ('a0000000-0000-0000-0000-000000000002', N'PROCESSING', N'En proceso', N'La acción está siendo ejecutada.', 1, 20),
    ('a0000000-0000-0000-0000-000000000003', N'SUCCEEDED', N'Exitosa', N'La operación terminó correctamente.', 1, 30),
    ('a0000000-0000-0000-0000-000000000004', N'DELIVERED', N'Entregada', N'El proveedor confirmó la entrega.', 1, 40),
    ('a0000000-0000-0000-0000-000000000005', N'RETRY_PENDING', N'Reintento pendiente', N'La acción espera un nuevo intento.', 1, 50),
    ('a0000000-0000-0000-0000-000000000006', N'FAILED', N'Fallida', N'La acción agotó o detuvo sus intentos sin éxito.', 1, 60),
    ('a0000000-0000-0000-0000-000000000007', N'CANCELLED', N'Cancelada', N'La acción fue cancelada y no continuará.', 1, 70)
)
    AS source (
        action_status_id, code, name_es, description_es, is_active, sort_order
    )
    ON target.action_status_id = source.action_status_id
WHEN MATCHED AND target.code = source.code
    THEN
    UPDATE
        SET
            name_es = source.name_es,
            description_es = source.description_es,
            is_active = source.is_active,
            sort_order = source.sort_order
WHEN NOT MATCHED
    THEN
    INSERT (
        action_status_id, code, name_es, description_es, is_active, sort_order
    )
    VALUES (
        source.action_status_id, source.code, source.name_es,
        source.description_es, source.is_active, source.sort_order
    );
GO
