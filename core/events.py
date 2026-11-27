"""Event port shared by CLI, browser and tests."""

from typing import Callable

from .tracing import trace_fields

EventSink = Callable[[str, dict], None]


def discard_event(kind: str, payload: dict) -> None:
    pass


def correlated(sink: EventSink) -> EventSink:
    def emit(kind, payload):
        sink(kind, {**payload, **trace_fields()})

    return emit
