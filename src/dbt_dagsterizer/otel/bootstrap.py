from __future__ import annotations

import atexit
import logging
import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

_log = logging.getLogger(__name__)
_configured_key: tuple[Any, ...] | None = None
_configured_result: OTelConfigureResult | None = None


@dataclass(frozen=True)
class OTelConfigureResult:
    traces_configured: bool
    metrics_configured: bool


def _normalize_exporter(value: str | None) -> str:
    v = (value or "").strip().lower()
    return v


def _parse_resource_attributes(value: str | None) -> dict[str, str]:
    raw = (value or "").strip()
    if not raw:
        return {}

    out: dict[str, str] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        k = k.strip()
        v = v.strip()
        if not k or not v:
            continue
        out[k] = v
    return out


def _normalize_otlp_endpoint(*, endpoint: str, protocol: str) -> str:
    ep = endpoint.strip()
    if not ep:
        return ep

    if protocol == "grpc":
        parsed = urlparse(ep)
        if parsed.scheme and parsed.netloc:
            return parsed.netloc
        return ep

    parsed = urlparse(ep)
    if parsed.scheme and parsed.netloc:
        return ep
    return f"http://{ep}"


def _http_otlp_signal_endpoint(*, endpoint: str, signal: str) -> str:
    parsed = urlparse(endpoint)
    if not (parsed.scheme and parsed.netloc):
        return endpoint

    if parsed.path and parsed.path != "/":
        return endpoint

    base = endpoint.rstrip("/")
    if signal == "traces":
        return f"{base}/v1/traces"
    if signal == "metrics":
        return f"{base}/v1/metrics"
    return endpoint


def configure_otel() -> OTelConfigureResult:
    global _configured_key, _configured_result

    traces_exporter = _normalize_exporter(os.getenv("OTEL_TRACES_EXPORTER"))
    metrics_exporter = _normalize_exporter(os.getenv("OTEL_METRICS_EXPORTER"))

    traces_enabled = traces_exporter not in {"", "none"}
    metrics_enabled = metrics_exporter not in {"", "none"}
    if not (traces_enabled or metrics_enabled):
        if _configured_key is None and (
            (traces_exporter or "none") == "none"
            or (metrics_exporter or "none") == "none"
        ):
            _log.warning(
                "OTEL export disabled via OTEL_TRACES_EXPORTER=%r and/or "
                "OTEL_METRICS_EXPORTER=%r. No OpenTelemetry traces or OTLP metrics will be "
                "emitted from this process. To enable, set both to 'otlp' and populate "
                "OTEL_EXPORTER_OTLP_ENDPOINT (for Fleet APM on Luban this is typically "
                "http://apm.elastic-stack.svc.cluster.local:8200 with protocol=http/protobuf).",
                traces_exporter or "none",
                metrics_exporter or "none",
            )
        return OTelConfigureResult(traces_configured=False, metrics_configured=False)

    endpoint = (os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") or "").strip()
    protocol = (os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL") or "").strip().lower() or "http/protobuf"
    if protocol and protocol not in {"grpc", "http/protobuf"}:
        _log.warning("Invalid OTEL_EXPORTER_OTLP_PROTOCOL=%r; disabling OTLP export", protocol)
        return OTelConfigureResult(traces_configured=False, metrics_configured=False)

    if not endpoint:
        _log.warning(
            "OTEL_EXPORTER_OTLP_ENDPOINT is empty/blank but OTEL_TRACES_EXPORTER=%r and/or "
            "OTEL_METRICS_EXPORTER=%r requested 'otlp'. No spans/metrics will be emitted from "
            "this process until an OTLP endpoint is configured (this is a platform-default "
            "no-op until an OTLP-compatible backend is installed). To enable: set "
            "otel_exporter_otlp_endpoint in your Luban centralized config (luban-config.yaml) "
            "or in your workspace gitops overlay. IMPORTANT: Always prefix the endpoint with "
            "explicit 'http://' (plaintext) or 'https://' (TLS) — omitting the scheme triggers "
            "gRPC default-TLS 'WRONG_VERSION_NUMBER' handshake failures against plaintext "
            "backends.",
            traces_exporter,
            metrics_exporter,
        )
        return OTelConfigureResult(traces_configured=False, metrics_configured=False)

    endpoint = _normalize_otlp_endpoint(endpoint=endpoint, protocol=protocol)

    service_name = (os.getenv("OTEL_SERVICE_NAME") or "").strip()
    resource_attrs_raw = os.getenv("OTEL_RESOURCE_ATTRIBUTES")
    resource_attrs = _parse_resource_attributes(resource_attrs_raw)
    if service_name:
        resource_attrs.setdefault("service.name", service_name)

    headers_raw = os.getenv("OTEL_EXPORTER_OTLP_HEADERS")

    configured_key = (
        traces_exporter,
        metrics_exporter,
        endpoint,
        protocol,
        service_name,
        resource_attrs_raw,
        headers_raw,
    )
    if _configured_key == configured_key and _configured_result is not None:
        return _configured_result

    try:
        from opentelemetry import metrics, trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExportResult
        from opentelemetry.sdk.trace.export import SpanExporter as _SpanExporter
    except Exception as e:
        _log.warning("OpenTelemetry packages are unavailable; OTEL disabled (%s)", e)
        return OTelConfigureResult(traces_configured=False, metrics_configured=False)

    resource = Resource.create(resource_attrs)

    traces_configured = False
    if traces_enabled and traces_exporter in {"otlp"}:
        try:
            if protocol == "grpc":
                from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
            else:
                from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

            trace_endpoint = endpoint
            if protocol == "http/protobuf":
                trace_endpoint = _http_otlp_signal_endpoint(endpoint=endpoint, signal="traces")

            class _ResultRecordingSpanExporter(_SpanExporter):  # type: ignore[misc,valid-type]
                """Wraps a real SpanExporter and records the most recent export outcome.

                OpenTelemetry-python BatchSpanProcessor's ``force_flush`` returns True whenever
                the processor successfully *asks* each queued batch to export — it does not
                reflect the underlying HTTP/gRPC response codes. As a result users observe
                ``force_flush() is True`` together with 0 traces delivered and have no signal
                in logs that transport is failing. This wrapper emits a WARNING whenever the
                inner exporter returns ``FAILURE``, so operators can trace the root cause
                (gRPC TLS 'WRONG_VERSION_NUMBER', DNS failure, APM server 503, missing
                ``http://`` scheme, NetworkPolicy dropping port 8200, etc.).
                """

                __slots__ = ("_inner", "last_result")

                def __init__(self, inner: Any) -> None:
                    self._inner = inner
                    self.last_result = SpanExportResult.SUCCESS

                def export(self, spans: Any, **kwargs: Any) -> SpanExportResult:  # type: ignore[override]
                    rc = self._inner.export(spans, **kwargs)
                    self.last_result = rc
                    if rc != SpanExportResult.SUCCESS:
                        span_count = len(list(spans)) if spans is not None else 0
                        if span_count == 0:
                            try:
                                span_count = len(spans)
                            except Exception:
                                span_count = 0
                        _log.warning(
                            "OTLP span export returned FAILURE (code=%s). %d span(s) in this "
                            "batch were NOT delivered to endpoint=%r protocol=%r. "
                            "Common causes for any OTLP backend (Elastic, OpenObserve, Tempo, "
                            "Collector, etc.):\n"
                            "  1. Network path / port blocked (NetworkPolicy, security-group, "
                            "     service-mesh deny) — verify with `nc -vz <host> <port>`.\n"
                            "  2. gRPC default-TLS vs plaintext mismatch when scheme omitted — "
                            "     ALWAYS prefix endpoint with explicit `http://` for plaintext "
                            "     or `https://` for TLS.\n"
                            "  3. Auth credentials missing — set OTEL_EXPORTER_OTLP_HEADERS "
                            "     for Bearer/Basic tokens, or OTEL_EXPORTER_OTLP_CERTIFICATE "
                            "     for mTLS client certs.\n"
                            "  4. Backend 5xx / 429 rate-limit / backend disk full or OOM.\n"
                            "  5. Per-signal path mismatch (backend expects `/v1/traces` but got "
                            "another path) — verify explicit *_ENDPOINT overrides.",
                            getattr(rc, "name", str(rc)),
                            span_count,
                            trace_endpoint,
                            protocol,
                        )
                    return rc

                def shutdown(self, timeout_millis: int = 30000) -> None:  # type: ignore[override]
                    return self._inner.shutdown(timeout_millis=timeout_millis)

                def force_flush(self, timeout_millis: int = 30000) -> bool:  # type: ignore[override]
                    return bool(self._inner.force_flush(timeout_millis=timeout_millis))

            raw_span_exporter = OTLPSpanExporter(endpoint=trace_endpoint)
            if headers_raw:
                # Some SDK versions accept `headers=` kwarg; apply via constructor directly.
                pass
            span_exporter = _ResultRecordingSpanExporter(raw_span_exporter)

            provider = TracerProvider(resource=resource)
            provider.add_span_processor(BatchSpanProcessor(span_exporter))
            try:
                trace.set_tracer_provider(provider)
            except RuntimeError as e:
                if "Overriding of current TracerProvider is not allowed" in str(e):
                    _log.warning(
                        "configure_otel() called a SECOND time in this process with "
                        "OTEL enabled; opentelemetry-python forbids replacing the global "
                        "TracerProvider / MeterProvider after construction. The settings "
                        "from the FIRST call (including if it was 'OTEL_*_EXPORTER=none') "
                        "are STILL IN EFFECT. To pick up new environment values call "
                        "configure_otel() exactly once, AFTER applying all OTEL_* overrides "
                        "(e.g. right after loading dagster definitions.py). (detail: %s)",
                        e,
                    )
                    traces_configured = False
                else:
                    raise
            else:
                atexit.register(provider.shutdown)
                traces_configured = True
        except Exception as e:
            _log.warning("Failed configuring OTLP trace exporter; traces disabled (%s)", e)

    metrics_configured = False
    if metrics_enabled and metrics_exporter in {"otlp"}:
        try:
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import (
                AggregationTemporality,
                MetricExporter,
                MetricExportResult,
                PeriodicExportingMetricReader,
            )

            if protocol == "grpc":
                from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
                    OTLPMetricExporter,
                )
            else:
                from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
                    OTLPMetricExporter,
                )

            metric_endpoint = endpoint
            if protocol == "http/protobuf":
                metric_endpoint = _http_otlp_signal_endpoint(endpoint=endpoint, signal="metrics")

            class _ResultRecordingMetricExporter(MetricExporter):  # type: ignore[misc,valid-type]
                """Mirror of _ResultRecordingSpanExporter for the metrics pipeline."""

                __slots__ = ("_inner", "last_result")

                def __init__(self, inner: Any) -> None:
                    self._inner = inner
                    self.last_result = MetricExportResult.SUCCESS

                def export(  # type: ignore[override]
                    self,
                    metrics_data: Any,
                    timeout_millis: float | None = None,
                    **kwargs: Any,
                ) -> MetricExportResult:
                    rc = self._inner.export(metrics_data, timeout_millis=timeout_millis, **kwargs)
                    self.last_result = rc
                    if rc != MetricExportResult.SUCCESS:
                        resource_count = 0
                        try:
                            resource_count = sum(
                                len(sm.metrics)
                                for rm in metrics_data.resource_metrics
                                for sm in rm.scope_metrics
                            )
                        except Exception:
                            pass
                        _log.warning(
                            "OTLP metric export returned FAILURE (code=%s). %d metric(s) in "
                            "this batch were NOT delivered to endpoint=%r protocol=%r. "
                            "Common causes for any OTLP backend (Elastic, OpenObserve, Tempo, "
                            "Collector, etc.):\n"
                            "  1. Network path / port blocked (NetworkPolicy, security-group, "
                            "     service-mesh deny) — verify with `nc -vz <host> <port>`.\n"
                            "  2. gRPC default-TLS vs plaintext mismatch when scheme omitted — "
                            "     ALWAYS prefix endpoint with explicit `http://` for plaintext "
                            "     or `https://` for TLS.\n"
                            "  3. Auth credentials missing — set OTEL_EXPORTER_OTLP_HEADERS "
                            "     for Bearer/Basic tokens, or OTEL_EXPORTER_OTLP_CERTIFICATE "
                            "     for mTLS client certs.\n"
                            "  4. Backend 5xx / 429 rate-limit / backend disk full or OOM.\n"
                            "  5. Per-signal path mismatch (backend expects `/v1/metrics` but "
                            "got another path) — verify explicit *_ENDPOINT overrides.",
                            getattr(rc, "name", str(rc)),
                            resource_count,
                            metric_endpoint,
                            protocol,
                        )
                    return rc

                def shutdown(self, timeout_millis: float = 30000, **kwargs: Any) -> None:  # type: ignore[override]
                    return self._inner.shutdown(timeout_millis=timeout_millis, **kwargs)

                def force_flush(self, timeout_millis: float = 30000, **kwargs: Any) -> bool:  # type: ignore[override]
                    return bool(self._inner.force_flush(timeout_millis=timeout_millis, **kwargs))

                @property  # type: ignore[override]
                def preferred_temporality(self) -> AggregationTemporality:
                    return self._inner.preferred_temporality

                @property  # type: ignore[override]
                def preferred_aggregation(self) -> Any:
                    return getattr(self._inner, "preferred_aggregation", None)

            raw_metric_exporter = OTLPMetricExporter(endpoint=metric_endpoint)
            metric_exporter = _ResultRecordingMetricExporter(raw_metric_exporter)

            reader = PeriodicExportingMetricReader(metric_exporter)
            provider = MeterProvider(resource=resource, metric_readers=[reader])
            try:
                metrics.set_meter_provider(provider)
            except RuntimeError as e:
                if "Overriding of current MeterProvider is not allowed" in str(e):
                    _log.warning(
                        "configure_otel() called a SECOND time in this process with "
                        "OTEL metrics enabled; opentelemetry-python forbids replacing "
                        "the global MeterProvider. The first call's settings are STILL "
                        "IN EFFECT. Call configure_otel() exactly once, AFTER applying "
                        "all OTEL_* overrides. (detail: %s)",
                        e,
                    )
                    metrics_configured = False
                else:
                    raise
            else:
                atexit.register(provider.shutdown)
                metrics_configured = True
        except Exception as e:
            _log.warning("Failed configuring OTLP metric exporter; metrics disabled (%s)", e)

    _configured_key = configured_key
    _configured_result = OTelConfigureResult(
        traces_configured=traces_configured, metrics_configured=metrics_configured
    )
    return _configured_result
