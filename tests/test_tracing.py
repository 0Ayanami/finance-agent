import json
from types import SimpleNamespace

from test_services import response, text, tool, wait_until
from test_web import HEADERS, web


def test_trace_connects_model_retries_subagent_approval_and_execution(web):
    browser, application, client, session_id = web
    session = application.get(session_id)
    session.runtime.gateway.retry_delay = lambda attempt: 0
    session.runtime.workspace._run_bash_process = lambda command, cwd=None: ("private-tool-output", 0)
    answer = response(text("private-model-answer"))
    answer.usage = SimpleNamespace(input_tokens=22, output_tokens=7)
    client.replies = [
        RuntimeError("429 private-provider-error"),
        response(tool("task", {"description": "private-subagent-task"}, "task-call"), stop_reason="tool_use"),
        response(tool("bash", {"command": "echo private-tool-arguments"}, "bash-call"), stop_reason="tool_use"),
        response(text("private-subagent-answer")),
        answer,
    ]
    posted = browser.post(
        f"/api/sessions/{session_id}/messages", headers=HEADERS,
        json={"message": "private-user-prompt"},
    )
    assert posted.status_code == 202
    wait_until(lambda: bool(session.approvals.snapshot()))
    approval = session.approvals.snapshot()[0]
    assert browser.post(
        f"/api/sessions/{session_id}/approvals/{approval['id']}",
        headers=HEADERS, json={"allow": True},
    ).status_code == 200
    session.worker.join(5)
    assert not session.worker.is_alive()

    output = (application.config.data_dir / "traces" / "agent-traces.jsonl").read_text(encoding="utf-8")
    assert "private-" not in output
    spans = [json.loads(line) for line in output.splitlines()]

    def find(name, parent=None, **attributes):
        return next(
            item for item in spans
            if item["name"] == name and item["parent_span_id"] == (parent and parent["span_id"])
            and all(item["attributes"].get(key) == value for key, value in attributes.items())
        )

    root = find("agent.run", outcome="completed")
    assert {item["trace_id"] for item in spans} == {root["trace_id"]}
    first_call = find("model.call", root, **{"model.retry_count": 1})
    assert first_call["attributes"]["gen_ai.request.model"] == "test-model"
    assert find("model.attempt", first_call, **{"model.retry.result": "retrying"})["status"] == "ERROR"
    find("model.attempt", first_call, **{"model.retry.result": "recovered"})
    final = find("model.call", root, **{"gen_ai.usage.output_tokens": 7})
    assert 0 <= final["attributes"]["model.first_text_ms"] <= final["attributes"]["model.total_ms"]

    task = find("tool.execute", root, tool_call_id="task-call")
    task_run = find("tool.run", task)
    subagent = find("agent.subagent", task_run)
    find("model.call", subagent, **{"model.purpose": "subagent"})
    shell = find("tool.execute", subagent, tool_call_id="bash-call")
    approval_span = find("tool.approval", shell, **{"approval.outcome": "approved"})
    actual = find("tool.run", shell)
    assert approval_span["started_ns"] < actual["started_ns"]
    assert all(0 <= item["duration_ms"] <= shell["duration_ms"] for item in (approval_span, actual))

    events = application.repository.events(session_id)
    for kind, active in (("run_started", root), ("approval_requested", approval_span), ("run_finished", root)):
        payload = next(item["payload"] for item in events if item["kind"] == kind)
        assert payload["trace_id"] == root["trace_id"]
        assert payload["span_id"] == active["span_id"]
