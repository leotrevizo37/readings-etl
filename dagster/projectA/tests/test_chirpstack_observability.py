import importlib.util
import io
import json
import re
import sqlite3
import sys
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from email.message import Message
from pathlib import Path
from unittest.mock import Mock, PropertyMock, patch
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PROJECT_ROOT.parents[1]
PROJECT_SRC = PROJECT_ROOT / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

chirpstack_metrics = importlib.import_module(
    "projectA.defs.transformations.chirpstack_metrics"
)
build_device_metric_row = chirpstack_metrics.build_device_metric_row
build_device_event_metric_rows = chirpstack_metrics.build_device_event_metric_rows
build_gateway_metric_row = chirpstack_metrics.build_gateway_metric_row
normalize_dev_eui = chirpstack_metrics.normalize_dev_eui


def _chart(label, value, timestamp="2026-09-17T10:00:00Z"):
    datasets = [] if label is None else [{"label": label, "data": [value]}]
    return {
        "timestamps": [timestamp],
        "datasets": datasets,
        "kind": "ABSOLUTE",
    }


def _device_payload():
    return {
        "device": {
            "devEui": "24E124136E090452",
            "applicationId": "32bf78f8-d347-4dc7-a7ea-f4336bd98290",
            "deviceProfileId": "9e74cac7-6ab3-4f38-ba35-ff923657e589",
        },
        "deviceStatus": {
            "batteryLevel": 42,
            "externalPowerSource": False,
        },
        "unknownFutureField": {"ignored": True},
    }


def _application_payload():
    return {
        "application": {
            "id": "32bf78f8-d347-4dc7-a7ea-f4336bd98290",
            "tenantId": "087175fa-5bf9-4180-a02e-7c57fb3543e8",
        }
    }


def _profile_payload():
    return {
        "deviceProfile": {
            "id": "9e74cac7-6ab3-4f38-ba35-ff923657e589",
            "tenantId": "087175fa-5bf9-4180-a02e-7c57fb3543e8",
            "uplinkInterval": 600,
            "measurements": {"battery": {"kind": "GAUGE"}},
        }
    }


class ChirpStackMetricTransformationTests(unittest.TestCase):
    def test_device_hour_preserves_zero_errors_and_metric_battery(self) -> None:
        time_span = datetime(2026, 9, 17, 10, tzinfo=timezone.utc)
        row = build_device_metric_row(
            dev_eui="24e124136e090452",
            device_payload=_device_payload(),
            application_payload=_application_payload(),
            device_profile_payload=_profile_payload(),
            link_metrics_payload={
                "rxPackets": _chart("rx_count", 6),
                "gwRssi": _chart("rssi", -81.125),
                "gwSnr": _chart("snr", 7.25),
                "errors": _chart(None, None),
            },
            device_metrics_payload={
                "metrics": {"battery": _chart("battery", 89.5)},
                "states": {},
            },
            time_span=time_span,
            extracted_at=datetime(2026, 9, 17, 11, 2, tzinfo=timezone.utc),
        )

        self.assertEqual(row["DevEui"], "24e124136e090452")
        self.assertEqual(row["ExpectedUplinkCount"], Decimal("6.000000"))
        self.assertEqual(row["ReceivedUplinkCount"], 6)
        self.assertEqual(row["AverageRssi"], Decimal("-81.125000"))
        self.assertEqual(row["AverageSnr"], Decimal("7.250000"))
        self.assertEqual(row["LinkErrorCount"], 0)
        self.assertEqual(row["LinkErrorCountsJson"], "{}")
        self.assertEqual(row["BatteryLevel"], Decimal("89.5000"))
        self.assertEqual(row["BatteryLevelSource"], "DEVICE_METRICS")
        self.assertEqual(row["BatteryObservedAt"], time_span.replace(tzinfo=None))
        self.assertTrue(row["MetricsAvailable"])

    def test_missing_hour_remains_null_instead_of_becoming_zero(self) -> None:
        row = build_device_metric_row(
            dev_eui="24e124136e090452",
            device_payload=_device_payload(),
            application_payload=_application_payload(),
            device_profile_payload=_profile_payload(),
            link_metrics_payload={
                "rxPackets": _chart("rx_count", 3, "2026-09-17T09:00:00Z"),
                "gwRssi": _chart("rssi", -80, "2026-09-17T09:00:00Z"),
                "gwSnr": _chart("snr", 8, "2026-09-17T09:00:00Z"),
                "errors": _chart(None, None, "2026-09-17T09:00:00Z"),
            },
            device_metrics_payload={"metrics": {}, "states": {}},
            time_span=datetime(2026, 9, 17, 10, tzinfo=timezone.utc),
            extracted_at=datetime(2026, 9, 17, 11, 2, tzinfo=timezone.utc),
        )

        self.assertIsNone(row["ReceivedUplinkCount"])
        self.assertIsNone(row["AverageRssi"])
        self.assertIsNone(row["AverageSnr"])
        self.assertIsNone(row["LinkErrorCount"])
        self.assertEqual(row["BatteryLevel"], Decimal("42.0000"))
        self.assertEqual(row["BatteryLevelSource"], "DEVICE_STATUS")
        self.assertFalse(row["MetricsAvailable"])

    def test_rejects_misaligned_metric_series(self) -> None:
        malformed_chart = {
            "timestamps": ["2026-09-17T10:00:00Z"],
            "datasets": [{"label": "rx_count", "data": []}],
        }
        with self.assertRaisesRegex(ValueError, "align with timestamps"):
            build_device_metric_row(
                dev_eui="24e124136e090452",
                device_payload=_device_payload(),
                application_payload=_application_payload(),
                device_profile_payload=_profile_payload(),
                link_metrics_payload={
                    "rxPackets": malformed_chart,
                    "gwRssi": _chart("rssi", -80),
                    "gwSnr": _chart("snr", 8),
                    "errors": _chart(None, None),
                },
                device_metrics_payload=None,
                time_span=datetime(2026, 9, 17, 10, tzinfo=timezone.utc),
                extracted_at=datetime(2026, 9, 17, 11, 2, tzinfo=timezone.utc),
            )

    def test_rejects_values_outside_sql_contract(self) -> None:
        for field, value, message in (
            ("rxPackets", 9223372036854775808, "SQL BIGINT contract"),
            ("gwRssi", 1000000000000, "decimal contract"),
        ):
            link_metrics = {
                "rxPackets": _chart("rx_count", 1),
                "gwRssi": _chart("rssi", -80),
                "gwSnr": _chart("snr", 8),
                "errors": _chart(None, None),
            }
            label = "rx_count" if field == "rxPackets" else "rssi"
            link_metrics[field] = _chart(label, value)
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, message):
                build_device_metric_row(
                    dev_eui="24e124136e090452",
                    device_payload=_device_payload(),
                    application_payload=_application_payload(),
                    device_profile_payload=_profile_payload(),
                    link_metrics_payload=link_metrics,
                    device_metrics_payload=None,
                    time_span=datetime(2026, 9, 17, 10, tzinfo=timezone.utc),
                    extracted_at=datetime(2026, 9, 17, 11, 2, tzinfo=timezone.utc),
                )

    def test_gateway_row_uses_official_state_and_packet_statuses(self) -> None:
        row = build_gateway_metric_row(
            gateway_list_item={
                "tenantId": "087175fa-5bf9-4180-a02e-7c57fb3543e8",
                "gatewayId": "C0BA1FFFFE01F6D7",
                "state": "OFFLINE",
                "lastSeenAt": "2026-09-17T10:59:30Z",
            },
            gateway_payload={
                "gateway": {
                    "gatewayId": "c0ba1ffffe01f6d7",
                    "tenantId": "087175fa-5bf9-4180-a02e-7c57fb3543e8",
                    "name": "projecta-gateway",
                    "statsInterval": 30,
                }
            },
            gateway_metrics_payload={
                "rxPackets": _chart("rx_count", 20, "2026-09-17T11:00:00Z"),
                "txPackets": _chart("tx_count", 2, "2026-09-17T11:00:00Z"),
                "txPacketsPerStatus": {
                    "timestamps": ["2026-09-17T11:00:00Z"],
                    "datasets": [
                        {"label": "OK", "data": [2]},
                        {"label": "TOO_LATE", "data": [1]},
                    ],
                },
            },
            expected_tenant_id="087175fa-5bf9-4180-a02e-7c57fb3543e8",
            time_span=datetime(2026, 9, 17, 11, tzinfo=timezone.utc),
            extracted_at=datetime(2026, 9, 17, 11, 2, tzinfo=timezone.utc),
        )

        self.assertEqual(row["GatewayState"], "OFFLINE")
        self.assertEqual(row["GatewayLastSeenAt"], datetime(2026, 9, 17, 10, 59, 30))
        self.assertEqual(row["ReceivedPacketCount"], 20)
        self.assertEqual(row["TransmittedPacketCount"], 2)
        self.assertEqual(row["TxOkCount"], 2)
        self.assertEqual(row["TxErrorCount"], 1)
        self.assertEqual(
            row["TxPacketStatusCountsJson"],
            '{"OK":2,"TOO_LATE":1}',
        )
        self.assertTrue(row["MetricsAvailable"])

    def test_gateway_historical_state_uses_hourly_evidence(self) -> None:
        gateway_list_item = {
            "tenantId": "087175fa-5bf9-4180-a02e-7c57fb3543e8",
            "gatewayId": "C0BA1FFFFE01F6D7",
            "state": "OFFLINE",
            "lastSeenAt": "2026-09-17T10:59:30Z",
        }
        gateway_payload = {
            "gateway": {
                "gatewayId": "c0ba1ffffe01f6d7",
                "tenantId": "087175fa-5bf9-4180-a02e-7c57fb3543e8",
                "name": "projecta-gateway",
                "statsInterval": 30,
            }
        }
        for metrics, expected_state, metrics_available in (
            (
                {
                    "rxPackets": _chart("rx_count", 1),
                    "txPackets": _chart("tx_count", 0),
                    "txPacketsPerStatus": _chart("OK", 0),
                },
                "ONLINE",
                True,
            ),
            (
                {
                    "rxPackets": _chart("rx_count", 0),
                    "txPackets": _chart("tx_count", 0),
                    "txPacketsPerStatus": _chart("OK", 0),
                },
                "UNKNOWN",
                True,
            ),
            (
                {
                    "rxPackets": _chart("rx_count", 0, "2026-09-17T09:00:00Z"),
                    "txPackets": _chart("tx_count", 0, "2026-09-17T09:00:00Z"),
                    "txPacketsPerStatus": _chart("OK", 0, "2026-09-17T09:00:00Z"),
                },
                "UNKNOWN",
                False,
            ),
        ):
            with self.subTest(expected_state=expected_state, metrics_available=metrics_available):
                row = build_gateway_metric_row(
                    gateway_list_item=gateway_list_item,
                    gateway_payload=gateway_payload,
                    gateway_metrics_payload=metrics,
                    expected_tenant_id="087175fa-5bf9-4180-a02e-7c57fb3543e8",
                    time_span=datetime(2026, 9, 17, 10, tzinfo=timezone.utc),
                    extracted_at=datetime(2026, 9, 17, 11, 2, tzinfo=timezone.utc),
                )

                self.assertEqual(row["GatewayState"], expected_state)
                self.assertIsNone(row["GatewayLastSeenAt"])
                self.assertEqual(row["MetricsAvailable"], metrics_available)

    def test_event_rows_aggregate_uplinks_logs_and_gateway_receptions(self) -> None:
        tenant_id = "087175fa-5bf9-4180-a02e-7c57fb3543e8"
        dev_eui = "24e124136e090452"
        gateway_one = "c0ba1ffffe01f6d7"
        gateway_two = "c0ba1ffffe01f6d8"
        device_info = {"devEui": dev_eui, "tenantId": tenant_id}
        event_rows = [
            {
                "DevEui": dev_eui,
                "EventTime": datetime(2026, 9, 17, 10, 5, 0, 123456),
                "RawMessage": {
                    "deduplicationId": "9232a3a7-79f5-4301-90f7-fa878659d322",
                    "time": "2026-09-17T10:05:00.123456Z",
                    "deviceInfo": device_info,
                    "rxInfo": [
                        {"gatewayId": gateway_one, "rssi": -80, "snr": 8},
                        {"gatewayId": gateway_two, "rssi": -90, "snr": 3},
                    ],
                },
            },
            {
                "DevEui": dev_eui,
                "EventTime": datetime(2026, 9, 17, 10, 10),
                "RawMessage": {
                    "deduplicationId": "6fd801c3-7ee5-4be2-842b-d48475d34753",
                    "time": "2026-09-17T10:10:00Z",
                    "deviceInfo": device_info,
                    "rxInfo": [
                        {"gatewayId": gateway_one, "rssi": -70, "snr": 10},
                    ],
                },
            },
            {
                "DevEui": dev_eui,
                "EventTime": datetime(2026, 9, 17, 10, 15),
                "RawMessage": {
                    "time": "2026-09-17T10:15:00Z",
                    "deviceInfo": device_info,
                    "level": "ERROR",
                    "code": "UPLINK_F_CNT_RETRANSMISSION",
                    "description": "retransmission",
                },
            },
        ]

        device_rows, gateway_rows, rejected_rows = build_device_event_metric_rows(
            event_rows=event_rows,
            devices={dev_eui: tenant_id},
            from_utc=datetime(2026, 9, 17, 10),
            to_utc=datetime(2026, 9, 17, 11),
            extracted_at=datetime(2026, 9, 17, 11, 2),
        )

        self.assertEqual(device_rows[0]["CapturedUplinkCount"], 2)
        self.assertEqual(rejected_rows, [])
        self.assertEqual(device_rows[0]["ObservedGatewayCount"], 2)
        self.assertEqual(device_rows[0]["MultiGatewayUplinkCount"], 1)
        self.assertEqual(
            device_rows[0]["EventLogCountsJson"],
            '{"UPLINK_F_CNT_RETRANSMISSION":1}',
        )
        self.assertEqual(len(gateway_rows), 2)
        gateway_one_row = next(
            row for row in gateway_rows if row["GatewayId"] == gateway_one
        )
        self.assertEqual(gateway_one_row["GatewayReceptionCount"], 2)
        self.assertEqual(gateway_one_row["ReceptionShare"], Decimal("1.000000"))
        self.assertEqual(
            gateway_one_row["FirstReceivedAt"],
            datetime(2026, 9, 17, 10, 5, 0, 123456),
        )

    def test_event_reception_accepts_optional_rssi_snr_and_channel(self) -> None:
        tenant_id = "087175fa-5bf9-4180-a02e-7c57fb3543e8"
        dev_eui = "24e124136e090452"
        gateways = [
            "c0ba1ffffe01f6d7",
            "c0ba1ffffe01f6d8",
            "c0ba1ffffe01f6d9",
        ]
        _, gateway_rows, rejected_rows = build_device_event_metric_rows(
            event_rows=[
                {
                    "DevEui": dev_eui,
                    "EventTime": datetime(2026, 9, 17, 10, 5),
                    "RawMessage": {
                        "deduplicationId": "9232a3a7-79f5-4301-90f7-fa878659d322",
                        "time": "2026-09-17T10:05:00Z",
                        "deviceInfo": {
                            "devEui": dev_eui,
                            "tenantId": tenant_id,
                        },
                        "rxInfo": [
                            {"gatewayId": gateways[0], "rssi": -86},
                            {"gatewayId": gateways[1], "snr": 4.5},
                            {"gatewayId": gateways[2]},
                        ],
                    },
                }
            ],
            devices={dev_eui: tenant_id},
            from_utc=datetime(2026, 9, 17, 10),
            to_utc=datetime(2026, 9, 17, 11),
            extracted_at=datetime(2026, 9, 17, 11, 2),
        )

        rows_by_gateway = {row["GatewayId"]: row for row in gateway_rows}
        self.assertEqual(len(rows_by_gateway), 3)
        self.assertEqual(rejected_rows, [])
        self.assertEqual(
            rows_by_gateway[gateways[0]]["AverageRssi"],
            Decimal("-86.000000"),
        )
        self.assertIsNone(rows_by_gateway[gateways[0]]["AverageSnr"])
        self.assertIsNone(rows_by_gateway[gateways[1]]["AverageRssi"])
        self.assertEqual(
            rows_by_gateway[gateways[1]]["AverageSnr"],
            Decimal("4.500000"),
        )
        self.assertIsNone(rows_by_gateway[gateways[2]]["AverageRssi"])
        self.assertIsNone(rows_by_gateway[gateways[2]]["AverageSnr"])
        self.assertTrue(all(row["GatewayReceptionCount"] == 1 for row in gateway_rows))

    def test_invalid_event_is_rejected_without_discarding_valid_event(self) -> None:
        tenant_id = "087175fa-5bf9-4180-a02e-7c57fb3543e8"
        dev_eui = "24e124136e090452"
        query_metadata = {
            "Tenant": "projecta",
            "FechaInicio": "2026-09-17T04:00:00.000000Z",
            "FechaFin": "2026-09-17T05:00:00.000000Z",
            "RequestedLimit": 5000,
            "FetchedAt": datetime(2026, 9, 17, 11, 0),
        }
        event_rows = [
            {
                **query_metadata,
                "DevEui": dev_eui,
                "EventTime": datetime(2026, 9, 17, 10, 5),
                "SourceRowJson": '{"id":"valid"}',
                "RawMessage": {
                    "deduplicationId": "9232a3a7-79f5-4301-90f7-fa878659d322",
                    "time": "2026-09-17T10:05:00Z",
                    "deviceInfo": {
                        "devEui": dev_eui,
                        "tenantId": tenant_id,
                    },
                    "rxInfo": [{"gatewayId": "c0ba1ffffe01f6d7"}],
                },
            },
            {
                **query_metadata,
                "DevEui": dev_eui,
                "EventTime": datetime(2026, 9, 17, 10, 10),
                "SourceRowJson": '{"id":"invalid"}',
                "RawMessage": {
                    "deduplicationId": "6fd801c3-7ee5-4be2-842b-d48475d34753",
                    "time": "2026-09-17T10:10:00Z",
                    "deviceInfo": {
                        "devEui": dev_eui,
                        "tenantId": tenant_id,
                    },
                    "rxInfo": None,
                },
            },
        ]

        device_rows, gateway_rows, rejected_rows = build_device_event_metric_rows(
            event_rows=event_rows,
            devices={dev_eui: tenant_id},
            from_utc=datetime(2026, 9, 17, 10),
            to_utc=datetime(2026, 9, 17, 11),
            extracted_at=datetime(2026, 9, 17, 11, 2),
        )

        self.assertEqual(device_rows[0]["CapturedUplinkCount"], 1)
        self.assertEqual(len(gateway_rows), 1)
        self.assertEqual(len(rejected_rows), 1)
        self.assertEqual(rejected_rows[0]["RowJson"], '{"id":"invalid"}')
        self.assertEqual(
            rejected_rows[0]["FechaInicio"],
            query_metadata["FechaInicio"],
        )
        self.assertIn("rxInfo must be an array", rejected_rows[0]["FailureReason"])

    def test_rejects_unknown_gateway_state_and_invalid_eui(self) -> None:
        with self.assertRaisesRegex(ValueError, "16-character hexadecimal"):
            normalize_dev_eui("not-an-eui")
        gateway_list_item = {
            "tenantId": "087175fa-5bf9-4180-a02e-7c57fb3543e8",
            "gatewayId": "c0ba1ffffe01f6d7",
            "state": "DEGRADED",
            "lastSeenAt": "2026-09-17T10:59:30Z",
        }
        with self.assertRaisesRegex(ValueError, "unknown gateway state"):
            build_gateway_metric_row(
                gateway_list_item=gateway_list_item,
                gateway_payload={},
                gateway_metrics_payload={},
                expected_tenant_id="087175fa-5bf9-4180-a02e-7c57fb3543e8",
                time_span=datetime(2026, 9, 17, 10, tzinfo=timezone.utc),
                extracted_at=datetime(2026, 9, 17, 11, 2, tzinfo=timezone.utc),
            )


class ChirpStackApiResourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        dagster_module = importlib.import_module("dagster")
        original_resource = getattr(dagster_module, "resource", None)
        if original_resource is None:
            dagster_module.resource = lambda fn=None, **_kwargs: (
                fn if fn else lambda wrapped: wrapped
            )
        path = (
            PROJECT_SRC
            / "projectA"
            / "defs"
            / "resources"
            / "chirpstack.py"
        )
        spec = importlib.util.spec_from_file_location(
            "projectA_chirpstack_resource",
            path,
        )
        cls.resource_module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        try:
            spec.loader.exec_module(cls.resource_module)
        finally:
            if original_resource is None:
                del dagster_module.resource

    def test_gateway_pagination_and_authorization_header(self) -> None:
        requests = []

        def fake_urlopen(request, timeout):
            requests.append((request, timeout))
            query = parse_qs(urlparse(request.full_url).query)
            offset = int(query["offset"][0])
            result = (
                [{"gatewayId": "0000000000000001"}, {"gatewayId": "0000000000000002"}]
                if offset == 0
                else [{"gatewayId": "0000000000000003"}]
            )
            return io.BytesIO(
                json.dumps({"totalCount": 3, "result": result}).encode()
            )

        resource = self.resource_module.ChirpStackApiResource(
            base_url="http://chirpstack.test",
            token="private-token",
            timeout_seconds=15,
            page_size=2,
        )
        with patch.object(
            self.resource_module,
            "urlopen",
            side_effect=fake_urlopen,
        ):
            gateways = resource.list_gateways(
                "087175fa-5bf9-4180-a02e-7c57fb3543e8"
            )

        self.assertEqual(len(gateways), 3)
        self.assertEqual(len(requests), 2)
        self.assertEqual(requests[0][1], 15)
        headers = {
            key.lower(): value
            for key, value in requests[0][0].header_items()
        }
        self.assertEqual(
            headers["grpc-metadata-authorization"],
            "Bearer private-token",
        )
        self.assertNotIn("private-token", requests[0][0].full_url)

    def test_metric_request_uses_closed_hour_contract(self) -> None:
        captured_requests = []

        def fake_urlopen(request, timeout):
            captured_requests.append(request)
            return io.BytesIO(b"{}")

        resource = self.resource_module.ChirpStackApiResource(
            base_url="https://chirpstack.test/",
            token="private-token",
            timeout_seconds=15,
            page_size=100,
        )
        with patch.object(
            self.resource_module,
            "urlopen",
            side_effect=fake_urlopen,
        ):
            resource.get_device_link_metrics(
                "24e124136e090452",
                datetime(2026, 9, 17, 10, tzinfo=timezone.utc),
                datetime(2026, 9, 17, 11, tzinfo=timezone.utc),
            )

        parsed = urlparse(captured_requests[0].full_url)
        query = parse_qs(parsed.query)
        self.assertEqual(parsed.path, "/api/devices/24e124136e090452/link-metrics")
        self.assertEqual(query["start"], ["2026-09-17T10:00:00Z"])
        self.assertEqual(query["end"], ["2026-09-17T11:00:00Z"])
        self.assertEqual(query["aggregation"], ["HOUR"])


class MqttEventHistoryApiResourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = (
            PROJECT_SRC
            / "projectA"
            / "defs"
            / "resources"
            / "mqtt_event_history.py"
        )
        spec = importlib.util.spec_from_file_location(
            "projectA_mqtt_event_history_resource",
            path,
        )
        cls.resource_module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(cls.resource_module)

    def test_request_uses_mqtt_history_contract_and_basic_auth(self) -> None:
        payload = {
            "ok": True,
            "tenant": "projecta",
            "fecha_inicio": "2026-09-17T04:00:00",
            "fecha_fin": "2026-09-17T05:00:00",
            "total": 1,
            "data": [
                {
                    "id": "9232a3a7-79f5-4301-90f7-fa878659d322",
                    "fecha": "2026-09-17T04:30:00",
                    "deveui": "24e124136e090452",
                    "tenant": "projecta",
                    "mensaje_raw": json.dumps(
                        {
                            "time": "2026-09-17T10:00:00.123456Z",
                            "deviceInfo": {
                                "devEui": "24e124136e090452",
                            },
                        }
                    ),
                }
            ],
        }

        class Headers:
            @staticmethod
            def get_content_type():
                return "application/json"

        class Response(io.BytesIO):
            status = 200
            headers = Headers()

        resource = self.resource_module.MqttEventHistoryApiResource(
            url="http://mqtt-history.test/api/lecturas/tenant",
            user="api-user",
            password="private-password",
            timeout_seconds=15,
        )
        requests = []

        def fake_urlopen(request, timeout):
            requests.append((request, timeout))
            return Response(json.dumps(payload).encode())

        with patch.object(self.resource_module, "urlopen", side_effect=fake_urlopen):
            rows = resource.fetch_range(
                datetime(2026, 9, 17, 4),
                datetime(2026, 9, 17, 5),
            )

        parsed = urlparse(requests[0][0].full_url)
        query = parse_qs(parsed.query)
        headers = {
            key.lower(): value for key, value in requests[0][0].header_items()
        }
        self.assertEqual(query["tenant"], ["projecta"])
        self.assertEqual(query["limit"], ["5000"])
        self.assertEqual(query["fechaInicio"], ["2026-09-17T10:00:00.000000Z"])
        self.assertEqual(query["fechaFin"], ["2026-09-17T11:00:00.000000Z"])
        self.assertEqual(requests[0][1], 15)
        self.assertTrue(headers["authorization"].startswith("Basic "))
        self.assertNotIn("private-password", requests[0][0].full_url)
        self.assertEqual(rows[0]["DevEui"], "24e124136e090452")
        self.assertEqual(
            rows[0]["EventTime"],
            datetime(2026, 9, 17, 10, 0, 0, 123456),
        )

    def test_splits_a_saturated_window(self) -> None:
        resource = self.resource_module.MqttEventHistoryApiResource(
            url="http://mqtt-history.test/api/lecturas/tenant",
            user="api-user",
            password="private-password",
            timeout_seconds=15,
        )
        first_id = "9232a3a7-79f5-4301-90f7-fa878659d322"
        second_id = "6fd801c3-7ee5-4be2-842b-d48475d34753"

        def fake_request(from_local, to_local):
            if to_local - from_local == timedelta(hours=1):
                raise self.resource_module.MqttEventHistoryWindowTooLarge()
            return [
                {
                    "MqttId": first_id if from_local.minute == 0 else second_id,
                }
            ]

        with patch.object(resource, "_request_window", side_effect=fake_request):
            rows = resource.fetch_range(
                datetime(2026, 9, 17, 4),
                datetime(2026, 9, 17, 5),
            )

        self.assertEqual([row["MqttId"] for row in rows], [first_id, second_id])
        self.assertEqual(resource.windows_split, 1)
        self.assertEqual(resource.windows_completed, 2)

    def test_rejects_response_total_smaller_than_data(self) -> None:
        resource = self.resource_module.MqttEventHistoryApiResource(
            url="http://mqtt-history.test/api/lecturas/tenant",
            user="api-user",
            password="private-password",
            timeout_seconds=15,
        )
        from_local = datetime(2026, 9, 17, 4)
        to_local = datetime(2026, 9, 17, 5)

        with self.assertRaisesRegex(ValueError, "total does not match data"):
            resource._validate_payload(
                {
                    "ok": True,
                    "tenant": "projecta",
                    "fecha_inicio": "2026-09-17T04:00:00",
                    "fecha_fin": "2026-09-17T05:00:00",
                    "total": 0,
                    "data": [{}],
                },
                from_local,
                to_local,
                {
                    "tenant": "projecta",
                    "fechaInicio": "2026-09-17T10:00:00.000000Z",
                    "fechaFin": "2026-09-17T11:00:00.000000Z",
                    "limit": 5000,
                },
                datetime(2026, 9, 17, 5),
            )

    def test_retries_a_transient_transport_failure(self) -> None:
        payload = {
            "ok": True,
            "tenant": "projecta",
            "fecha_inicio": "2026-09-17T04:00:00",
            "fecha_fin": "2026-09-17T05:00:00",
            "total": 0,
            "data": [],
        }

        class Headers:
            @staticmethod
            def get_content_type():
                return "application/json"

        class Response(io.BytesIO):
            status = 200
            headers = Headers()

        resource = self.resource_module.MqttEventHistoryApiResource(
            url="http://mqtt-history.test/api/lecturas/tenant",
            user="api-user",
            password="private-password",
            timeout_seconds=15,
        )
        with patch.object(
            self.resource_module,
            "urlopen",
            side_effect=[URLError("temporary"), Response(json.dumps(payload).encode())],
        ) as urlopen_mock, patch.object(self.resource_module, "sleep"):
            rows = resource.fetch_range(
                datetime(2026, 9, 17, 4),
                datetime(2026, 9, 17, 5),
            )

        self.assertEqual(rows, [])
        self.assertEqual(urlopen_mock.call_count, 2)

    def test_invalid_api_row_is_truncated_and_does_not_discard_valid_row(self) -> None:
        valid_raw = {
            "time": "2026-09-17T10:00:00Z",
            "deviceInfo": {"devEui": "24e124136e090452"},
        }
        payload = {
            "ok": True,
            "tenant": "projecta",
            "fecha_inicio": "2026-09-17T04:00:00",
            "fecha_fin": "2026-09-17T05:00:00",
            "total": 2,
            "data": [
                {
                    "id": "9232a3a7-79f5-4301-90f7-fa878659d322",
                    "fecha": "2026-09-17T04:30:00",
                    "deveui": "24e124136e090452",
                    "tenant": "projecta",
                    "mensaje_raw": json.dumps(valid_raw),
                },
                {
                    "id": "6fd801c3-7ee5-4be2-842b-d48475d34753",
                    "fecha": "2026-09-17T04:31:00",
                    "deveui": "24e124136e090452",
                    "tenant": "projecta",
                    "mensaje_raw": json.dumps({"payload": "x" * 5000}),
                },
            ],
        }

        class Headers:
            @staticmethod
            def get_content_type():
                return "application/json"

        class Response(io.BytesIO):
            status = 200
            headers = Headers()

        resource = self.resource_module.MqttEventHistoryApiResource(
            url="http://mqtt-history.test/api/lecturas/tenant",
            user="api-user",
            password="private-password",
            timeout_seconds=15,
        )
        with patch.object(
            self.resource_module,
            "urlopen",
            return_value=Response(json.dumps(payload).encode()),
        ):
            rows = resource.fetch_range(
                datetime(2026, 9, 17, 4),
                datetime(2026, 9, 17, 5),
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(len(resource.rejected_rows), 1)
        rejected = resource.rejected_rows[0]
        self.assertEqual(rejected["FechaInicio"], "2026-09-17T10:00:00.000000Z")
        self.assertEqual(rejected["FechaFin"], "2026-09-17T11:00:00.000000Z")
        self.assertEqual(rejected["RequestedLimit"], 5000)
        self.assertEqual(len(rejected["RowJson"]), 4000)
        self.assertIn("'time'", rejected["FailureReason"])


class ChirpStackContractTests(unittest.TestCase):
    def test_chirpstack_historical_state_and_merges_match_all_code_locations(self) -> None:
        code_locations = {
            "projectA": REPOSITORY_ROOT / "dagster" / "projectA" / "src" / "projectA",
            "projectB": REPOSITORY_ROOT / "dagster" / "projectB" / "src" / "projectB",
            "projectD": REPOSITORY_ROOT / "dagster" / "projectD" / "src" / "projectD",
        }
        sql_sources = {}
        for code_location, source_root in code_locations.items():
            transformation_path = (
                source_root / "defs" / "transformations" / "chirpstack_metrics.py"
            )
            spec = importlib.util.spec_from_file_location(
                f"{code_location}_chirpstack_metrics_integration",
                transformation_path,
            )
            self.assertIsNotNone(spec)
            self.assertIsNotNone(spec.loader)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            for received_packet_count, expected_state in ((0, "UNKNOWN"), (1, "ONLINE")):
                with self.subTest(
                    code_location=code_location,
                    received_packet_count=received_packet_count,
                ):
                    row = module.build_gateway_metric_row(
                        gateway_list_item={
                            "tenantId": "087175fa-5bf9-4180-a02e-7c57fb3543e8",
                            "gatewayId": "C0BA1FFFFE01F6D7",
                            "state": "ONLINE",
                            "lastSeenAt": "2026-09-17T10:59:30Z",
                        },
                        gateway_payload={
                            "gateway": {
                                "gatewayId": "c0ba1ffffe01f6d7",
                                "tenantId": "087175fa-5bf9-4180-a02e-7c57fb3543e8",
                                "name": "Gateway",
                                "statsInterval": 30,
                            }
                        },
                        gateway_metrics_payload={
                            "rxPackets": _chart("rx_count", received_packet_count),
                            "txPackets": _chart("tx_count", 0),
                            "txPacketsPerStatus": _chart("OK", 0),
                        },
                        expected_tenant_id="087175fa-5bf9-4180-a02e-7c57fb3543e8",
                        time_span=datetime(2026, 9, 17, 10, tzinfo=timezone.utc),
                        extracted_at=datetime(2026, 9, 17, 11, 2, tzinfo=timezone.utc),
                    )
                    self.assertEqual(row["GatewayState"], expected_state)
                    self.assertTrue(row["MetricsAvailable"])
                    self.assertIsNone(row["GatewayLastSeenAt"])
            sql_sources[code_location] = (
                source_root / "defs" / "resources" / "sql_server.py"
            ).read_text(encoding="utf-8")

        for constant_name in (
            "FACT_CHIRPSTACK_DEVICE_METRICS_MERGE_SQL",
            "FACT_CHIRPSTACK_GATEWAY_METRICS_MERGE_SQL",
            "FACT_CHIRPSTACK_DEVICE_EVENT_METRICS_MERGE_SQL",
            "FACT_CHIRPSTACK_DEVICE_GATEWAY_METRICS_MERGE_SQL",
        ):
            marker = f'{constant_name} = """'
            projectA_merge = sql_sources["projectA"].split(marker, 1)[1].split('"""', 1)[0]
            for code_location in ('projectB', 'projectD'):
                with self.subTest(
                    constant_name=constant_name,
                    code_location=code_location,
                ):
                    merge = sql_sources[code_location].split(marker, 1)[1].split('"""', 1)[0]
                    self.assertEqual(merge, projectA_merge)

    def test_sql_contract_has_hourly_keys_and_official_gateway_state(self) -> None:
        sql_directory = (
            REPOSITORY_ROOT
            / "sql"
            / "init"
        )
        ddl_paths = [
            sql_directory / "023_observability_serving_vwSensorSensorHealth.sql",
            sql_directory / "024_observability_factChirpStackDeviceMetrics.sql",
            sql_directory / "025_observability_factChirpStackGatewayMetrics.sql",
            sql_directory / "026_observability_chirpstackGatewayViews.sql",
            sql_directory / "027_serving_sensorCommunicationHealthViews.sql",
            sql_directory / "034_observability_vwSensorDevEuiUplinkReadingsDaily.sql",
            sql_directory / "035_observability_factChirpStackDeviceEventMetrics.sql",
            sql_directory / "036_observability_factChirpStackDeviceGatewayMetrics.sql",
            sql_directory / "037_raw_MqttEventHistoryRejectedRow.sql",
        ]
        sql = "\n".join(path.read_text(encoding="utf-8") for path in ddl_paths)

        self.assertTrue(all(path.is_file() for path in ddl_paths))
        projectA_sql_directory = (
            REPOSITORY_ROOT / "sql" / "user_codes" / "projectA" / "init"
        )
        self.assertFalse(any(
            projectA_sql_directory.glob("*ChirpStack*.sql")
        ))
        self.assertIn("PRIMARY KEY CLUSTERED (DevEui, TimeSpan)", sql)
        self.assertIn("PRIMARY KEY CLUSTERED (GatewayId, TimeSpan)", sql)
        self.assertIn(
            "PRIMARY KEY CLUSTERED (DevEui, GatewayId, TimeSpan)",
            sql,
        )
        self.assertIn("LinkErrorCountsJson NVARCHAR(MAX) NULL", sql)
        self.assertIn("TxPacketStatusCountsJson NVARCHAR(MAX) NULL", sql)
        self.assertIn("raw.MqttEventHistoryRejectedRow", sql)
        self.assertIn("RowJson NVARCHAR(4000) NOT NULL", sql)
        self.assertIn("GatewayState NVARCHAR(20) NOT NULL", sql)
        self.assertIn("N'UNKNOWN'", sql)
        self.assertIn("ISNULL(ReceivedPacketCount, 0) = 0", sql)
        self.assertNotIn("GatewayState = N'UNKNOWN'\n                    AND GatewayLastSeenAt IS NULL\n                    AND MetricsAvailable = 0", sql)
        self.assertIn("UnknownGatewayCount", sql)
        self.assertIn("BatteryLevel DECIMAL(7, 4) NULL", sql)
        self.assertIn("serving.vwSensorSensorCommunicationHealthHourly", sql)
        self.assertIn("serving.vwSensorSensorCoverageHourly", sql)
        self.assertIn("serving.vwSensorSensorCoverageDaily", sql)
        self.assertIn("serving.vwSensorSensorCoverageWeekly", sql)
        self.assertIn("observability.vwSensorDevEuiUplinkReadingsDaily", sql)
        self.assertIn("COUNT_BIG(*) AS SensorIdCount", sql)
        self.assertNotIn("CREATE OR ALTER VIEW serving.vwSensorSensorHealth", sql)
        self.assertIn("serving.vwSensorGatewayHealthHourly", sql)
        self.assertIn("observability.vwChirpStackTenantGatewayHealthHourly", sql)
        self.assertIn("N'DATA_INGESTION_PROBLEM'", sql)
        self.assertIn("N'DEVICE_NOT_COMMUNICATING'", sql)
        self.assertIn("N'GATEWAY_PROBLEM'", sql)
        self.assertLess(
            sql.index("WHEN device.ReceivedUplinkCount > 0"),
            sql.index("WHEN device.ExpectedUplinkCount IS NULL"),
        )
        self.assertNotIn("LastSeenAt DATETIME2(0)", sql.split(
            "CREATE TABLE observability.factChirpStackDeviceMetrics",
            1,
        )[1].split("CREATE TABLE stg.factChirpStackDeviceMetrics", 1)[0])

    def test_registration_and_configuration_are_scoped_to_projectA(self) -> None:
        compose = (
            REPOSITORY_ROOT / "compose" / "docker-compose.yml"
        ).read_text(encoding="utf-8")
        defs_source = (
            PROJECT_SRC / "projectA" / "defs" / "__init__.py"
        ).read_text(encoding="utf-8")
        asset = (
            PROJECT_SRC
            / "projectA"
            / "defs"
            / "assets"
            / "observability_chirpstack_metrics.py"
        ).read_text(encoding="utf-8")
        schedule = (
            PROJECT_SRC
            / "projectA"
            / "defs"
            / "schedules"
            / "chirpstack_metrics_scheduler.py"
        ).read_text(encoding="utf-8")
        job = (
            PROJECT_SRC
            / "projectA"
            / "defs"
            / "jobs"
            / "chirpstack_metrics_job.py"
        ).read_text(encoding="utf-8")

        self.assertIn('"chirpstack_api": chirpstack_api_resource', defs_source)
        self.assertIn(
            '"mqtt_event_history_api": mqtt_event_history_api_resource',
            defs_source,
        )
        self.assertIn("observability_chirpstackmetrics", defs_source)
        self.assertIn("observability_chirpstackmqttmetrics", defs_source)
        self.assertIn("PROJECTA_CHIRPSTACK_API_URL", compose)
        self.assertIn("PROJECTA_MQTT_EVENT_HISTORY_API_URL", compose)
        self.assertIn(
            "PROJECTA_MQTT_EVENT_HISTORY_REJECTED_ROW_RETENTION_DAYS",
            compose,
        )
        self.assertIn("MQTT_INGESTION_NAME = \"factChirpStackMqttMetrics\"", asset)
        self.assertIn("to_utc = extracted_at", asset)
        self.assertIn("last_loaded_at = sql.get_watermark(INGESTION_NAME)", asset)
        self.assertIn("last_loaded_at = sql.get_watermark(MQTT_INGESTION_NAME)", asset)
        self.assertIn("while window_from_utc < to_utc", asset)
        self.assertIn("except (TypeError, ValueError):", asset)
        self.assertIn('"valid_dev_eui_count"', asset)
        self.assertIn('"invalid_physical_identifier_count"', asset)
        self.assertIn('"invalid_physical_identifiers"', asset)
        self.assertIn('group_name="projectA"', asset)
        self.assertIn('key_prefix=["projectA"]', asset)
        self.assertIn(
            'AssetKey(["projectA", "observability_chirpstackmqttmetrics"])',
            job,
        )
        self.assertIn('name="023_Sensor_chirpstack_metrics_hourly_at_30"', schedule)
        self.assertIn('cron_schedule="30 * * * *"', schedule)
        self.assertIn('execution_timezone="America/Ciudad_Juarez"', schedule)
        self.assertIn(
            "default_status=DefaultScheduleStatus.RUNNING",
            schedule,
        )
        self.assertNotIn("SAMPLERETAIL_CHIRPSTACK_API_URL", compose)

    def test_staging_and_merges_are_run_scoped(self) -> None:
        sql_resource = (
            PROJECT_SRC
            / "projectA"
            / "defs"
            / "resources"
            / "sql_server.py"
        ).read_text(encoding="utf-8")
        spark_resource = (
            PROJECT_SRC
            / "projectA"
            / "defs"
            / "resources"
            / "spark.py"
        ).read_text(encoding="utf-8")

        self.assertIn("FROM stg.factChirpStackDeviceMetrics", sql_resource)
        self.assertIn("FROM stg.factChirpStackGatewayMetrics", sql_resource)
        self.assertIn("FROM stg.factChirpStackDeviceEventMetrics", sql_resource)
        self.assertIn("FROM stg.factChirpStackDeviceGatewayMetrics", sql_resource)
        self.assertIn("def write_mqtt_event_history_rejected_rows", sql_resource)
        self.assertIn("DATEADD(DAY, -?, SYSUTCDATETIME())", sql_resource)
        self.assertGreaterEqual(sql_resource.count("WHERE RunId = ?"), 2)
        self.assertIn("def merge_chirpstack_metrics", sql_resource)
        self.assertIn("def merge_chirpstack_mqtt_metrics", sql_resource)
        self.assertIn("def clear_chirpstack_mqtt_metrics_stage", sql_resource)
        self.assertIn("conn.rollback()", sql_resource)
        self.assertEqual(
            sql_resource.count(
                "target.MetricsAvailable = 0\n"
                "    OR source.MetricsAvailable = 1"
            ),
            2,
        )
        self.assertIn(
            "source.ReceivedUplinkCount > target.ReceivedUplinkCount",
            sql_resource,
        )
        self.assertIn(
            "source.ReceivedPacketCount > target.ReceivedPacketCount",
            sql_resource,
        )
        self.assertIn(
            "source.CapturedUplinkCount > target.CapturedUplinkCount",
            sql_resource,
        )
        self.assertIn(
            "candidate.GatewayReceptionCount < existing.GatewayReceptionCount",
            sql_resource,
        )
        self.assertIn("OPENJSON(target.EventLogCountsJson)", sql_resource)
        self.assertIn("ExtractedAt,\n        CASE", sql_resource)
        self.assertIn(
            '.dropDuplicates(["DevEui", "TimeSpan"])',
            spark_resource,
        )
        self.assertIn(
            '.dropDuplicates(["GatewayId", "TimeSpan"])',
            spark_resource,
        )


@pytest.mark.parametrize(
    "scenario",
    (
        "decreased_count",
        "missing_gateway",
        "different_run",
        "missing_radio",
    ),
)
def test_mqtt_merge_source_preserves_gateway_evidence(scenario):
    module = importlib.import_module("projectA.defs.resources.sql_server")
    statement = module.FACT_CHIRPSTACK_DEVICE_GATEWAY_METRICS_MERGE_SQL
    source_select = statement.split("USING (", 1)[1].split(") AS source", 1)[0]
    source_select = source_select.replace("#ChirpStackMqttAccepted", "accepted")
    radio_update = statement.split("target.AverageRssi = ", 1)[1].split(
        ",\n        target.MinRssi", 1
    )[0]
    target = {
        "DevEui": "0000000000000001",
        "GatewayId": "0000000000000002",
        "TimeSpan": "2026-09-17 10:00:00",
        "TenantId": "00000000-0000-0000-0000-000000000003",
        "GatewayReceptionCount": 2,
        "RadioReceptionStatsJson": None, "ReceptionFieldQualityJson": None,
        "ReceptionShare": 1.0,
        "AverageRssi": -90,
        "MinRssi": -90,
        "MaxRssi": -90,
        "AverageSnr": 5,
        "MinSnr": 5,
        "MaxSnr": 5,
        "FirstReceivedAt": "2026-09-17 10:05:00",
        "LastReceivedAt": "2026-09-17 10:15:00",
        "ExtractedAt": "2026-09-17 11:00:00",
    }
    source = {**target, "RunId": "synthetic-run"}
    if scenario == "decreased_count":
        source["GatewayReceptionCount"] = 1
    elif scenario == "missing_gateway":
        source["GatewayId"] = "0000000000000004"
    elif scenario == "different_run":
        source["RunId"] = "another-run"
    else:
        for field in ("AverageRssi", "MinRssi", "MaxRssi", "AverageSnr", "MinSnr", "MaxSnr"):
            source[field] = None
    with closing(sqlite3.connect(":memory:")) as database:
        database.row_factory = sqlite3.Row
        for schema, row in (("observability", target), ("stg", source)):
            database.execute(f"ATTACH DATABASE ':memory:' AS {schema}")
            table = f"{schema}.factChirpStackDeviceGatewayMetrics"
            database.execute(f"CREATE TABLE {table} ({', '.join(row)})")
            database.execute(
                f"INSERT INTO {table} VALUES ({', '.join('?' for _ in row)})",
                tuple(row.values()),
            )
        database.execute("CREATE TABLE accepted (RunId, DevEui, TimeSpan)")
        database.execute(
            "INSERT INTO accepted VALUES (?, ?, ?)",
            (source["RunId"], source["DevEui"], source["TimeSpan"]),
        )
        selected = database.execute(source_select, ("synthetic-run",)).fetchall()
        if scenario == "different_run":
            assert selected == []
        elif scenario != "missing_radio":
            assert len(selected) == 1
            assert selected[0]["IsNonDecreasing"] == 0
        else:
            assert len(selected) == 1
            actual_rssi = target["AverageRssi"]
            if selected[0]["IsNonDecreasing"] == 1:
                actual_rssi = database.execute(
                    f"WITH source AS ({source_select}) "
                    f"SELECT {radio_update} "
                    "FROM source CROSS JOIN observability.factChirpStackDeviceGatewayMetrics AS target",
                    ("synthetic-run",),
                ).fetchone()[0]
            assert actual_rssi == target["AverageRssi"]


@pytest.mark.parametrize("count", (2, 3))
@pytest.mark.parametrize("signal", ("Rssi", "Snr"))
@pytest.mark.parametrize(
    "previous,incoming,expected",
    (
        pytest.param((-85, -90, -80), (None, None, None), (-85, -90, -80), id="null-triplet"),
        pytest.param((-85, -90, -80), (None, -80, -60), (-85, -90, -80), id="null-average"),
        pytest.param((-85, -90, -80), (-70, None, -60), (-85, -90, -80), id="null-minimum"),
        pytest.param((-85, -90, -80), (-70, -80, None), (-85, -90, -80), id="null-maximum"),
        pytest.param((-85, -90, -80), (0, 0, 0), (-85, -90, -80), id="zero-triplet"),
        pytest.param((-85, -90, -80), (0, -2, 2), (0, -2, 2), id="valid-zero-average"),
        pytest.param((-85, -90, -80), (2, 0, 4), (2, 0, 4), id="valid-zero-minimum"),
        pytest.param((-85, -90, -80), (-2, -4, 0), (-2, -4, 0), id="valid-zero-maximum"),
        pytest.param((-85, -90, -80), (-70, -80, -60), (-70, -80, -60), id="new-evidence"),
        pytest.param((-85, -90, -80), (-85, -90, -80), (-85, -90, -80), id="identical"),
        pytest.param((None, None, None), (-70, -80, -60), (-70, -80, -60), id="first-evidence"),
        pytest.param((None, None, None), (0, 0, 0), (0, 0, 0), id="first-zero-evidence"),
        pytest.param((None, None, None), (None, None, None), (None, None, None), id="still-unavailable"),
        pytest.param((None, -90, -80), (-70, -80, -60), (-70, -80, -60), id="complete-partial-evidence"),
    ),
)
def test_mqtt_replay_preserves_radio_triplets(count, signal, previous, incoming, expected):
    module = importlib.import_module("projectA.defs.resources.sql_server")
    statement = module.FACT_CHIRPSTACK_DEVICE_GATEWAY_METRICS_MERGE_SQL
    assignments = statement.split("UPDATE SET", 1)[1].split("WHEN NOT MATCHED", 1)[0]
    assignments = re.split(r",\s*target\.", assignments.strip().removeprefix("target."))
    projections = {
        field: expression
        for field, expression in (assignment.split(" = ", 1) for assignment in assignments)
        if field != "ModifiedAt"
    }
    target = {
        "TenantId": "00000000-0000-0000-0000-000000000003",
        "GatewayReceptionCount": 2,
        "RadioReceptionStatsJson": None, "ReceptionFieldQualityJson": None,
        "ReceptionShare": 1.0,
        "AverageRssi": -85, "MinRssi": -90, "MaxRssi": -80,
        "AverageSnr": 5, "MinSnr": 4, "MaxSnr": 6,
        "FirstReceivedAt": "2026-09-17 10:01:00",
        "LastReceivedAt": "2026-09-17 10:15:00",
        "ExtractedAt": "2026-09-17 11:00:00",
    }
    fields = [prefix + signal for prefix in ("Average", "Min", "Max")]
    target.update(zip(fields, previous))
    source = {
        **target,
        "GatewayReceptionCount": count,
        "ReceptionShare": 0.5,
        "AverageRssi": -75, "MinRssi": -80, "MaxRssi": -70,
        "AverageSnr": 8, "MinSnr": 7, "MaxSnr": 9,
        "LastReceivedAt": "2026-09-17 10:45:00",
        "ExtractedAt": "2026-09-17 11:30:00",
    }
    source.update(zip(fields, incoming))
    ddl = (
        REPOSITORY_ROOT
        / "sql" / "init" / "036_observability_factChirpStackDeviceGatewayMetrics.sql"
    ).read_text(encoding="utf-8")
    check = re.search(r"\bCHECK\s*\((.*?)\)\s*,", ddl, re.S).group(1)
    with closing(sqlite3.connect(":memory:")) as database:
        database.row_factory = sqlite3.Row
        ctes = []
        parameters = []
        for alias, row in (("target", target), ("source", source)):
            ctes.append(alias + " AS (SELECT " + ", ".join("? AS " + field for field in row) + ")")
            parameters.extend(row.values())
        updated = dict(database.execute(
            "WITH " + ", ".join(ctes)
            + " SELECT " + ", ".join(expression + " AS " + field for field, expression in projections.items())
            + " FROM target CROSS JOIN source",
            parameters,
        ).fetchone())
        assert tuple(updated[field] for field in fields) == expected
        assert {field: value for field, value in updated.items() if field not in fields} == {
            field: value for field, value in source.items() if field not in fields
        }
        valid = database.execute(
            "WITH result AS (SELECT " + ", ".join("? AS " + field for field in updated)
            + ") SELECT (" + check + ") FROM result",
            list(updated.values()),
        ).fetchone()[0]
        assert valid != 0


@pytest.mark.parametrize("lookback", (1, 6))
@pytest.mark.parametrize("mode", ("events", "empty", "504", "limit", "failure"))
def test_mqtt_dst_defers_then_recovers_both_utc_hours(lookback, mode):
    from dagster import AssetKey, AssetSpec, IOManager, IOManagerDefinition, RetryPolicy, materialize
    from pyspark.sql.types import _make_type_verifier

    module = importlib.import_module("projectA.defs.assets.observability_chirpstack_metrics")
    mqtt_module = importlib.import_module("projectA.defs.resources.mqtt_event_history")
    dev_eui = "0000000000000001"
    tenant_id = "00000000-0000-0000-0000-000000000003"
    scope = {"devices": {dev_eui: tenant_id}}
    source_rows = []
    for index, (hour, minute) in enumerate(((7, 0), (7, 15), (8, 0), (8, 45), (9, 0)), 1):
        event_time = datetime(2026, 11, 1, hour, minute, tzinfo=timezone.utc)
        source_rows.append({
            "id": f"00000000-0000-0000-0000-{index:012d}",
            "deveui": dev_eui,
            "tenant": mqtt_module.EVENT_API_TENANT,
            "fecha": event_time.astimezone(ZoneInfo("America/Mexico_City")).replace(tzinfo=None).isoformat(),
            "mensaje_raw": json.dumps({
                "time": event_time.isoformat(),
                "deduplicationId": f"00000000-0000-0000-0000-{index:012d}",
                "deviceInfo": {"devEui": dev_eui, "tenantId": tenant_id},
                "rxInfo": [{"gatewayId": "0000000000000002", "rssi": -80, "snr": 5}],
            }),
        })
    source_rows.append({**source_rows[1], "id": "00000000-0000-0000-0000-000000000006"})
    watermark = datetime(2026, 11, 1, 6, 30, tzinfo=timezone.utc)
    for hour in (7, 8, 9, 10):
        now = datetime(2026, 11, 1, hour, 30, tzinfo=timezone.utc)
        mqtt = module.MqttEventHistoryApiResource(
            url="https://mqtt.test/history", user="synthetic", password="synthetic",
            timeout_seconds=5, lookback_hours=lookback, limit=3 if mode == "limit" else 5000,
        )
        sql = Mock(spec=module.SqlServerResource)
        sql.get_watermark.return_value = watermark
        sql.write_mqtt_event_history_rejected_rows.return_value = (0, 0)
        sql.merge_chirpstack_mqtt_metrics.return_value = (0, 0)
        spark = Mock(spec=module.SparkResource)
        spark.write_chirpstack_device_event_metrics_stage.return_value = 0
        spark.write_chirpstack_device_gateway_metrics_stage.return_value = 0
        io_manager = Mock(spec=IOManager)
        io_manager.load_input.return_value = scope
        requests = []
        injected = False

        def respond(request, timeout):
            nonlocal injected
            query = parse_qs(urlparse(request.full_url).query)
            start = datetime.fromisoformat(query["fechaInicio"][0].replace("Z", "+00:00")).astimezone(
                ZoneInfo("America/Mexico_City")
            ).replace(tzinfo=None)
            end = datetime.fromisoformat(query["fechaFin"][0].replace("Z", "+00:00")).astimezone(
                ZoneInfo("America/Mexico_City")
            ).replace(tzinfo=None)
            assert start < end
            requests.append((start, end))
            if (
                not injected and start == datetime(2026, 11, 1, 1)
                and end == datetime(2026, 11, 1, 3)
                and (mode == "504" or mode == "failure" and hour == 9)
            ):
                injected = True
                raise HTTPError(request.full_url, 504 if mode == "504" else 401, "synthetic", {}, None)
            rows = [
                row for row in source_rows
                if start <= datetime.fromisoformat(row["fecha"]) < end
            ] if mode != "empty" else []
            response = io.BytesIO(json.dumps({
                "ok": True, "tenant": mqtt_module.EVENT_API_TENANT,
                "fecha_inicio": start.isoformat(), "fecha_fin": end.isoformat(),
                "total": len(rows), "data": rows[:mqtt.limit],
            }).encode())
            response.status = 200
            response.headers = Message()
            response.headers["Content-Type"] = "application/json"
            return response

        with (
            patch.object(module, "datetime", wraps=datetime) as clock,
            patch.object(mqtt_module, "urlopen", side_effect=respond),
            patch.object(mqtt_module, "sleep"),
            patch.object(
                type(module.observability_chirpstackmqttmetrics.op), "retry_policy",
                new_callable=PropertyMock, return_value=RetryPolicy(max_retries=0),
            ),
        ):
            clock.now.return_value = now
            result = materialize(
                [
                    AssetSpec(key=AssetKey(["projectA", "observability_chirpstackmetrics"])),
                    module.observability_chirpstackmqttmetrics,
                ],
                resources={
                    "mqtt_event_history_api": mqtt, "sqlserver": sql, "spark": spark,
                    "io_manager": IOManagerDefinition.hardcoded_io_manager(io_manager),
                },
                raise_on_error=False,
            )
        materializations = result.get_asset_materialization_events()
        if hour < 9:
            assert result.success
            assert materializations == []
            observations = result.get_asset_observation_events()
            assert len(observations) == 1
            metadata = observations[0].event_specific_data.asset_observation.metadata
            assert metadata["status"].value == "DEFERRED_DST"
            assert metadata["resume_after_utc"].value == "2026-11-01 09:00:00"
            assert requests == []
            assert [method[0] for method in sql.mock_calls] == ["get_watermark"]
            assert spark.mock_calls == []
            continue
        assert (datetime(2026, 11, 1, 1), datetime(2026, 11, 1, 3)) in requests or (
            hour == 10 and lookback == 1 and mode != "failure"
        )
        if mode == "failure" and hour == 9:
            assert not result.success
            assert materializations == []
            sql.merge_chirpstack_mqtt_metrics.assert_not_called()
            assert sql.mark_run_completed.call_args.kwargs["succeeded"] is False
            assert sql.mark_run_completed.call_args.kwargs["new_last_loaded_at"] == watermark.replace(tzinfo=None)
            continue
        assert result.success
        assert len(materializations) == 1
        frame_calls = spark.get_spark.return_value.createDataFrame.call_args_list
        assert len(frame_calls) == 2
        for frame_call in frame_calls:
            rows, schema = frame_call.args
            for row in rows:
                _make_type_verifier(schema)(row)
        device_rows = frame_calls[0].args[0]
        from_utc = watermark.replace(tzinfo=None, minute=0, second=0) - timedelta(hours=lookback - 1)
        expected = {}
        cursor = from_utc
        while cursor < now.replace(tzinfo=None):
            expected[cursor] = 0 if mode == "empty" else {7: 2, 8: 2, 9: 1}.get(cursor.hour, 0)
            cursor += timedelta(hours=1)
        assert {row["TimeSpan"]: row["CapturedUplinkCount"] for row in device_rows} == expected
        assert len(device_rows) == len(expected)
        assert mqtt.rejected_rows == []
        if mode in ("504", "limit") and (hour == 9 or lookback == 6):
            assert mqtt.windows_split > 0
        sql.mark_run_completed.assert_called_once_with(
            ingestion_name=module.MQTT_INGESTION_NAME,
            new_last_loaded_at=now.replace(tzinfo=None), succeeded=True, error_message=None,
        )
        watermark = now


@pytest.mark.parametrize(
    "watermark,now,deferred,expected_windows",
    (
        (datetime(2026, 11, 1, 7, 30), datetime(2026, 11, 1, 9), False, ((1, 2),)),
        (datetime(2026, 11, 1, 8), datetime(2026, 11, 1, 8, 59), True, ()),
        (datetime(2026, 11, 1, 8), datetime(2026, 11, 1, 9), False, ((1, 2),)),
        (None, datetime(2026, 11, 1, 8, 30), True, ()),
        (datetime(2026, 3, 8, 8), datetime(2026, 3, 8, 10), False, ((1, 3), (3, 4))),
        (datetime(2026, 11, 2, 8), datetime(2026, 11, 2, 10), False, ((1, 2), (2, 3))),
    ),
)
def test_mqtt_dst_window_boundaries(watermark, now, deferred, expected_windows):
    from dagster import build_asset_context

    module = importlib.import_module("projectA.defs.assets.observability_chirpstack_metrics")
    mqtt = module.MqttEventHistoryApiResource(
        url="https://mqtt.test/history", user="synthetic", password="synthetic",
        timeout_seconds=5, lookback_hours=1,
    )
    sql = Mock(spec=module.SqlServerResource)
    sql.get_watermark.return_value = watermark
    sql.write_mqtt_event_history_rejected_rows.return_value = (0, 0)
    sql.merge_chirpstack_mqtt_metrics.return_value = (0, 0)
    spark = Mock(spec=module.SparkResource)
    spark.write_chirpstack_device_event_metrics_stage.return_value = 0
    spark.write_chirpstack_device_gateway_metrics_stage.return_value = 0
    with (
        build_asset_context(resources={"mqtt_event_history_api": mqtt, "sqlserver": sql, "spark": spark}) as context,
        patch.object(module, "datetime", wraps=datetime) as clock,
        patch.object(mqtt, "_request_window", return_value=[]) as request,
    ):
        clock.now.return_value = now.replace(tzinfo=timezone.utc)
        results = list(module.observability_chirpstackmqttmetrics(
            context,
            {"devices": {"0000000000000001": "00000000-0000-0000-0000-000000000003"},
             "original_last_loaded_at": None},
        ))
    assert len(results) == (0 if deferred else 1)
    assert [(c.args[0].hour, c.args[1].hour) for c in request.call_args_list] == list(expected_windows)
    if deferred:
        sql.mark_run_started.assert_not_called()
        sql.mark_run_completed.assert_not_called()
    else:
        sql.mark_run_completed.assert_called_once_with(
            ingestion_name=module.MQTT_INGESTION_NAME,
            new_last_loaded_at=now, succeeded=True, error_message=None,
        )


if __name__ == "__main__":
    unittest.main()
