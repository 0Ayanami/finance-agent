import random
import time
from ..core.models import RecoveryState
from ..core.messages import extract_text, tool_calls
from ..core.tracing import fail, identifier, span


class ModelGateway:
    extract_text = staticmethod(extract_text)
    has_tool_use = staticmethod(lambda content: bool(tool_calls(content)))

    def complete(self, *, state=None, on_text=None, purpose="agent", retry=True, **arguments):
        state = state or RecoveryState(self.config.model)
        stream_message = getattr(self.client.messages, "stream", None)
        streaming = on_text is not None and callable(stream_message)
        started = time.perf_counter()
        first_text = None
        with span("model.call", {
            "gen_ai.request.model": identifier(state.current_model),
            "model.streaming": streaming,
            "model.purpose": purpose,
        }) as call:
            def observe(text):
                nonlocal first_text
                if text and first_text is None:
                    first_text = round((time.perf_counter() - started) * 1000, 3)
                    call.set_attribute("model.first_text_ms", first_text)

            def request():
                if streaming:
                    def preview(text):
                        observe(text)
                        on_text(text)
                    return self._stream(stream_message, state.current_model, preview, arguments)
                response = self.client.messages.create(model=state.current_model, **arguments)
                if any(getattr(block, "type", None) == "text" and getattr(block, "text", "")
                       for block in getattr(response, "content", ())):
                    observe(True)
                return response

            try:
                response = self.with_retry(request, state, trace_call=call, retry=retry)
                call.set_attribute("gen_ai.response.model", identifier(
                    getattr(response, "model", None) or state.current_model,
                ))
                self._record_usage(call, response)
                return response
            finally:
                call.set_attribute("model.total_ms", round((time.perf_counter() - started) * 1000, 3))

    @staticmethod
    def _record_usage(active, response):
        usage = getattr(response, "usage", None)
        for name in ("input_tokens", "output_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"):
            count = usage.get(name) if isinstance(usage, dict) else getattr(usage, name, None)
            if type(count) is int and count >= 0:
                active.set_attribute(f"gen_ai.usage.{name}", count)

    @staticmethod
    def _stream(stream_message, model, on_text, arguments):
        # Reset the preview at the beginning of each retry.
        on_text("")
        blocks = {}
        with stream_message(model=model, **arguments) as stream:
            for event in stream:
                if event.type == "content_block_start" and event.content_block.type == "text":
                    blocks[event.index] = event.content_block.text
                    if event.content_block.text:
                        on_text("\n".join(blocks.values()))
                elif event.type == "content_block_delta" and event.delta.type == "text_delta":
                    blocks[event.index] = blocks.get(event.index, "") + event.delta.text
                    on_text("\n".join(blocks.values()))
                elif event.type == "message_stop":
                    return stream.get_final_message()
        raise RuntimeError("Model stream ended before the response was complete")

    def __init__(self, config, client):
        self.config = config
        self.client = client

    def retry_delay(self, attempt: int) -> float:
        base = min(self.config.retry_delay_ms * 2**attempt, 32000) / 1000
        return base + random.uniform(0, base * 0.25)

    def with_retry(self, fn, state: RecoveryState, *, trace_call=None, retry=True):
        max_attempts = self.config.max_retries if retry else 1
        for attempt in range(max_attempts):
            if trace_call is not None:
                trace_call.set_attribute("model.retry_count", attempt)
            with span("model.attempt", {
                "model.attempt": attempt + 1,
                "gen_ai.request.model": identifier(state.current_model),
            }) as active:
                try:
                    result = fn()
                except InterruptedError:
                    raise
                except Exception as error:
                    name, msg = type(error).__name__.lower(), str(error).lower()
                    rate_limit = "ratelimit" in name or "429" in msg
                    overloaded = "overloaded" in name or "529" in msg or "overloaded" in msg
                    if not retry or not (rate_limit or overloaded):
                        active.set_attribute("model.retry.result", "failed")
                        raise
                    fail(active, type(error).__name__)
                    active.set_attribute("model.retry.reason", "rate_limit" if rate_limit else "overloaded")
                    if attempt + 1 >= max_attempts:
                        active.set_attribute("model.retry.result", "exhausted")
                        break
                    if not rate_limit:
                        state.consecutive_529 += 1
                        if (state.consecutive_529 >= self.config.max_consecutive_overload
                                and self.config.fallback_model):
                            state.current_model = self.config.fallback_model
                            state.consecutive_529 = 0
                            active.set_attribute("model.retry.next_model", identifier(state.current_model))
                    delay = self.retry_delay(attempt)
                    active.set_attribute("model.retry.result", "retrying")
                    active.set_attribute("model.retry.delay_ms", round(delay * 1000, 3))
                else:
                    state.consecutive_529 = 0
                    active.set_attribute("model.retry.result", "recovered" if attempt else "success")
                    self._record_usage(active, result)
                    return result
            time.sleep(delay)
        raise RuntimeError(f"Max retries ({self.config.max_retries}) exceeded")

    def is_prompt_too_long_error(self, e: Exception) -> bool:
        msg = str(e).lower()
        return (
            "prompt" in msg
            and "long" in msg
            or "context_length_exceeded" in msg
            or "max_context_window" in msg
        )
