"""Shared tool execution pipeline for main agent, subagents and HTTP events."""

from types import SimpleNamespace

from ..core.tracing import fail, identifier, span


class ToolDispatcher:
    def __init__(self, hooks, emit, stop_event):
        self.hooks = hooks
        self.emit = emit
        self.stop_event = stop_event
        self.background = None

    @staticmethod
    def call_handler(handler, args: dict, name: str):
        if handler is None:
            return f"Unknown tool: {name}", "UnknownTool"
        try:
            output = str(handler(**args))
        except Exception as error:
            category = type(error).__name__
            return f"Error: {category}: {error}", category
        category = "ToolReportedError" if output.startswith("Error:") else None
        return output, category

    def execute(self, call: dict, handlers: dict, *, allow_background=True) -> str:
        block = SimpleNamespace(id=call["id"], name=call["name"], input=call.get("input", {}))
        metadata = {
            "tool_call_id": identifier(block.id),
            "tool.name": identifier(block.name) if block.name in handlers else "unknown",
        }
        with span("tool.execute", metadata) as tool:
            self.emit("tool_started", {"id": block.id, "name": block.name, "input": block.input})
            denied = None if self.stop_event.is_set() else self.hooks.trigger("PreToolUse", block)
            if self.stop_event.is_set():
                output = "Execution cancelled by user"
                tool.set_attribute("outcome", "cancelled")
            elif denied:
                output = str(denied)
                tool.set_attribute("outcome", "denied")
                fail(tool, "PermissionDenied")
            elif (
                allow_background
                and self.background
                and self.background.should_run_background(block.name, block.input)
            ):
                task_id = self.background.start_background_task(block, handlers)
                output = f"[Background task {task_id} started] Result will arrive as a task_notification."
                tool.set_attribute("outcome", "scheduled")
            else:
                with span("tool.run", metadata) as execution:
                    output, category = self.call_handler(handlers.get(block.name), block.input, block.name)
                    if category:
                        fail(execution, category)
                        fail(tool, category)
                    tool.set_attribute("outcome", "failed" if category else "completed")
                self.hooks.trigger("PostToolUse", block, output)
            self.emit("tool_finished", {"id": block.id, "name": block.name, "output": output})
            return output
