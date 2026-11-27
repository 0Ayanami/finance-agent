"""Export the same OpenTelemetry spans to local JSONL and optional OTLP/HTTP."""

import json
import logging
import logging.handlers
from pathlib import Path

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor, SpanExporter
from opentelemetry.sdk.trace.export import SpanExportResult


logger = logging.getLogger(__name__)


class JsonlSpanExporter(SpanExporter):
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.handler = logging.handlers.RotatingFileHandler(
            path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8", delay=True,
        )
        self.closed = False

    def export(self, spans):
        self.handler.acquire()
        try:
            if self.closed:
                return SpanExportResult.FAILURE
            for active in spans:
                parent = active.parent
                record = {
                    "trace_id": f"{active.context.trace_id:032x}",
                    "span_id": f"{active.context.span_id:016x}",
                    "parent_span_id": f"{parent.span_id:016x}" if parent else None,
                    "name": active.name,
                    "started_ns": active.start_time,
                    "duration_ms": round((active.end_time - active.start_time) / 1_000_000, 3),
                    "status": active.status.status_code.name,
                    "attributes": dict(active.attributes),
                }
                self.handler.handle(logging.LogRecord(
                    "agent.trace", logging.INFO, "", 0, json.dumps(record), (), None,
                ))
        finally:
            self.handler.release()
        return SpanExportResult.SUCCESS

    def shutdown(self):
        self.handler.acquire()
        try:
            self.closed = True
            self.handler.close()
        finally:
            self.handler.release()


def create_tracer_provider(config):
    provider = TracerProvider(
        resource=Resource({"service.name": "agent-harness"}),
        shutdown_on_exit=False,
    )
    if config.trace_jsonl:
        try:
            exporter = JsonlSpanExporter(config.data_dir / "traces" / "agent-traces.jsonl")
            provider.add_span_processor(SimpleSpanProcessor(exporter))
        except OSError:
            logger.warning("Local agent trace directory is unavailable")
    if config.trace_otlp_endpoint:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        provider.add_span_processor(BatchSpanProcessor(
            OTLPSpanExporter(endpoint=config.trace_otlp_endpoint, timeout=3, headers={}),
            max_queue_size=512, max_export_batch_size=128, schedule_delay_millis=500,
        ))
    return provider
