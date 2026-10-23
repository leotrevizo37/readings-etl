import json
import os
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from dagster import resource


class ChirpStackApiResource:
    def __init__(
        self,
        base_url: str,
        token: str,
        timeout_seconds: int,
        page_size: int,
    ) -> None:
        parsed_url = urlparse(base_url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            raise ValueError("CHIRPSTACK_API_URL must be an absolute HTTP or HTTPS URL")
        if not token:
            raise ValueError("CHIRPSTACK_API_TOKEN is required")
        if timeout_seconds <= 0:
            raise ValueError("CHIRPSTACK_API_TIMEOUT_SECONDS must be greater than zero")
        if page_size <= 0:
            raise ValueError("CHIRPSTACK_API_PAGE_SIZE must be greater than zero")
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout_seconds = timeout_seconds
        self.page_size = page_size

    def get_device(self, dev_eui: str) -> dict[str, Any]:
        return self._get(f"/api/devices/{dev_eui}")

    def get_application(self, application_id: str) -> dict[str, Any]:
        return self._get(f"/api/applications/{application_id}")

    def get_device_profile(self, device_profile_id: str) -> dict[str, Any]:
        return self._get(f"/api/device-profiles/{device_profile_id}")

    def get_device_link_metrics(
        self,
        dev_eui: str,
        start: datetime,
        end: datetime,
    ) -> dict[str, Any]:
        return self._get(
            f"/api/devices/{dev_eui}/link-metrics",
            self._metric_query(start, end),
        )

    def get_device_metrics(
        self,
        dev_eui: str,
        start: datetime,
        end: datetime,
    ) -> dict[str, Any]:
        return self._get(
            f"/api/devices/{dev_eui}/metrics",
            self._metric_query(start, end),
        )

    def list_gateways(self, tenant_id: str) -> list[dict[str, Any]]:
        gateways = []
        offset = 0
        total_count = None
        while total_count is None or len(gateways) < total_count:
            payload = self._get(
                "/api/gateways",
                {
                    "limit": self.page_size,
                    "offset": offset,
                    "tenantId": tenant_id,
                },
            )
            page_total_count = payload.get("totalCount")
            page = payload.get("result")
            if (
                isinstance(page_total_count, bool)
                or not isinstance(page_total_count, int)
                or page_total_count < 0
            ):
                raise RuntimeError("ChirpStack returned an invalid gateway totalCount")
            if not isinstance(page, list) or any(
                not isinstance(item, dict) for item in page
            ):
                raise RuntimeError("ChirpStack returned an invalid gateway result")
            if total_count is None:
                total_count = page_total_count
            elif total_count != page_total_count:
                raise RuntimeError("ChirpStack gateway totalCount changed during pagination")
            if not page and len(gateways) < total_count:
                raise RuntimeError("ChirpStack gateway pagination ended before totalCount")
            gateways.extend(page)
            if len(gateways) > total_count:
                raise RuntimeError("ChirpStack returned more gateways than totalCount")
            offset += len(page)
        return gateways

    def get_gateway(self, gateway_id: str) -> dict[str, Any]:
        return self._get(f"/api/gateways/{gateway_id}")

    def get_gateway_metrics(
        self,
        gateway_id: str,
        start: datetime,
        end: datetime,
    ) -> dict[str, Any]:
        return self._get(
            f"/api/gateways/{gateway_id}/metrics",
            self._metric_query(start, end),
        )

    def _metric_query(self, start: datetime, end: datetime) -> dict[str, str]:
        return {
            "start": self._rfc3339(start),
            "end": self._rfc3339(end),
            "aggregation": "HOUR",
        }

    def _get(
        self,
        path: str,
        query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        request_url = self.base_url + path
        if query:
            request_url += "?" + urlencode(query)
        request = Request(
            request_url,
            headers={
                "Accept": "application/json",
                "Grpc-Metadata-Authorization": "Bearer " + self.token,
            },
            method="GET",
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                payload = json.load(response)
        except HTTPError as ex:
            raise RuntimeError(
                f"ChirpStack request failed for {path} with HTTP {ex.code}"
            ) from ex
        except URLError as ex:
            raise RuntimeError(f"ChirpStack request failed for {path}") from ex
        except (json.JSONDecodeError, UnicodeDecodeError) as ex:
            raise RuntimeError(f"ChirpStack returned invalid JSON for {path}") from ex
        if not isinstance(payload, dict):
            raise TypeError(f"ChirpStack returned a non-object response for {path}")
        return payload

    @staticmethod
    def _rfc3339(value: datetime) -> str:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


@resource
def chirpstack_api_resource(_):
    base_url = os.getenv("CHIRPSTACK_API_URL", "").strip()
    token = os.getenv("CHIRPSTACK_API_TOKEN", "").strip()
    timeout_text = os.getenv("CHIRPSTACK_API_TIMEOUT_SECONDS", "").strip()
    page_size_text = os.getenv("CHIRPSTACK_API_PAGE_SIZE", "").strip()
    if not base_url:
        raise ValueError("CHIRPSTACK_API_URL is required")
    if not timeout_text:
        raise ValueError("CHIRPSTACK_API_TIMEOUT_SECONDS is required")
    if not page_size_text:
        raise ValueError("CHIRPSTACK_API_PAGE_SIZE is required")
    try:
        timeout_seconds = int(timeout_text)
    except ValueError as ex:
        raise ValueError("CHIRPSTACK_API_TIMEOUT_SECONDS must be an integer") from ex
    try:
        page_size = int(page_size_text)
    except ValueError as ex:
        raise ValueError("CHIRPSTACK_API_PAGE_SIZE must be an integer") from ex
    return ChirpStackApiResource(
        base_url=base_url,
        token=token,
        timeout_seconds=timeout_seconds,
        page_size=page_size,
    )
