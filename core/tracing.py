"""Trace scopes, safe metadata, and explicit worker propagation."""

import re
from contextlib import contextmanager
from contextvars import ContextVar, copy_context

from opentelemetry import trace
from opentelemetry.trace import INVALID_SPAN, StatusCode


_tracer = ContextVar("agent_tracer", default=None)
_IDENTIFIER = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_./:-]{0,127}")


def identifier(value):
    if isinstance(value, str) and _IDENTIFIER.fullmatch(value):
        if not value.lower().startswith(("sk-", "bearer", "ghp_", "eyj")):
            return value
    return "unknown"


@contextmanager
def tracing_with(tracer):
    token = _tracer.set(tracer)
    try:
        yield
    finally:
        _tracer.reset(token)


def worker_target(function):
    """Capture the current trace and span before starting a new worker."""
    context = copy_context()

    def run(*args, **kwargs):
        return context.run(function, *args, **kwargs)

    return run


def fail(active, category):
    active.set_attribute("error.type", identifier(category))
    active.set_status(StatusCode.ERROR)


@contextmanager
def span(name, attributes=None):
    tracer = _tracer.get()
    if tracer is None:
        yield INVALID_SPAN
        return
    with tracer.start_as_current_span(
        name, attributes=attributes,
        record_exception=False, set_status_on_exception=False,
    ) as active:
        try:
            yield active
        except InterruptedError:
            active.set_attribute("outcome", "cancelled")
            raise
        except Exception as error:
            fail(active, type(error).__name__)
            raise


def trace_fields():
    context = trace.get_current_span().get_span_context()
    if not context.is_valid:
        return {}
    return {"trace_id": f"{context.trace_id:032x}", "span_id": f"{context.span_id:016x}"}
