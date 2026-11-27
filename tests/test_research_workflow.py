import json
import threading
from dataclasses import replace
from datetime import date, datetime
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from fastapi.testclient import TestClient

from agent_harness.infrastructure.cron_store import CronStore
from agent_harness.infrastructure.research_sources import (
    DEFAULT_SOURCES,
    EASTMONEY_CALLBACK,
    EASTMONEY_LIST,
    EASTMONEY_PAGE_SIZE,
    EASTMONEY_PDF,
    ResearchSources,
)
from agent_harness.interfaces.web.app import create_app
from agent_harness.services.library_rag import LibraryRAG
from agent_harness.services.research_scope import research_scope
from test_pdf_layout import layout_fixture
from test_retrieval import embedding_config
from test_scheduled_tasks import HEADERS, _session, _tasks
from test_services import FakeClient, response, text, wait_until


@pytest.mark.parametrize("scheduled", [True, False], ids=["scheduled", "run-now"])
def test_research_task_indexes_while_summary_is_running(tmp_path, monkeypatch, scheduled):
    pdf_url = EASTMONEY_PDF.format("AP202611230002")
    broken_url = EASTMONEY_PDF.format("AP202611230001")
    copy_url = EASTMONEY_PDF.format("AP202611230003")
    pdf = layout_fixture()
    rows = [
        {
            "title": "Saturn broken",
            "infoCode": "AP202611230001",
            "publishDate": "2026-11-24 00:00:00",
        },
        {
            "title": "Saturn copy",
            "infoCode": "AP202611230003",
            "publishDate": "2026-11-22 00:00:00",
        },
    ]
    rows.extend(
        {"title": "Unrelated report", "infoCode": f"UNRELATED{index}"}
        for index in range(EASTMONEY_PAGE_SIZE - len(rows))
    )
    requests = []

    def fetch(self, url, referer=None):
        requests.append(url)
        if url.startswith(EASTMONEY_LIST + "?"):
            assert referer == "https://data.eastmoney.com/report/"
            query = parse_qs(urlsplit(url).query)
            category, page = query["qType"][0], query["pageNo"][0]
            if category == "0":
                return url, '{"data":[]}'
            if page == "1":
                return url, f"{EASTMONEY_CALLBACK}({json.dumps({'data': rows})});"
            return url, json.dumps(
                {
                    "data": [
                        {
                            "title": "Quarterly research",
                            "industryName": "Saturn",
                            "infoCode": "AP202611230002",
                            "publishDate": "2026-11-23 00:00:00",
                        }
                    ]
                }
            )
        return url, {
            pdf_url: pdf,
            broken_url: b"%PDF- broken",
            copy_url: pdf,
        }[url]

    monkeypatch.setattr(
        "agent_harness.infrastructure.research_sources.ResearchSources.fetch", fetch
    )
    started, finish = threading.Event(), threading.Event()

    class Model(FakeClient):
        def create(self, **arguments):
            assert arguments["thinking"] == {"type": "disabled"}
            system = arguments["system"]
            data = json.loads(arguments["messages"][0]["content"])
            if "逐段提取" in system:
                assert any("Useful body 2" in part for part in data["passages"])
                if arguments["max_tokens"] == 1200:
                    return response(stop_reason="max_tokens")
                assert arguments["max_tokens"] == 2400
                started.set()
                assert finish.wait(15)
                return response(text("R1 2026-11-23 Saturn 95"))
            assert "涨跌驱动" in system and data["facts"]
            return response(text("Saturn 数据为 95 [R1]。"))

    embedding_calls = []

    def embed(self, texts, *, kind):
        embedding_calls.append(kind)
        return [[1.0, 0.0, 0.0] for _ in texts]

    monkeypatch.setattr("agent_harness.infrastructure.embeddings.BailianEmbeddings.embed", embed)
    config = replace(embedding_config(tmp_path), model="deepseek-v4-pro")
    app = create_app(config, Model)
    with TestClient(app) as client:
        session_id = _session(client)
        research = {"target_industries": ["Saturn"], "report_mode": "market_digest"}
        run_path = _tasks(session_id).replace("/scheduled-tasks", "/research-runs")
        session = app.state.application.get(session_id)
        scheduler = session.runtime.scheduler
        if scheduled:
            created = client.post(
                _tasks(session_id),
                headers=HEADERS,
                json={**research, "cron_expr": "0 0 31 2 *", "recurring": False},
            )
            assert created.status_code == 201
            with scheduler.cron_lock:
                scheduler._enqueue_due_job(scheduler.scheduled_jobs[created.json()["id"]])
        else:
            assert client.post(run_path, json=research).status_code == 403
            assert (
                client.post(
                    run_path,
                    headers=HEADERS,
                    json={**research, "target_industries": []},
                ).status_code
                == 422
            )
            assert client.post(run_path, headers=HEADERS, json=research).status_code == 202
        try:
            assert started.wait(15)
            if not scheduled:
                assert scheduler.list_jobs() == []
                assert client.post(run_path, headers=HEADERS, json=research).status_code == 409
            library = app.state.library
            wait_until(lambda: library.list()["total"] == 1, timeout=15)
            record = library.list()["items"][0]
            wait_until(
                lambda: app.state.ingestion.status(record["id"])["status"] == "ready", timeout=15
            )
            wait_until(lambda: library.get(record["id"])["vector"]["status"] == "ready", timeout=15)
            metadata = record["research_metadata"]
            assert metadata["doc_id"] == record["id"]
            assert metadata["source"] == "https://data.eastmoney.com/report/"
            assert metadata["source_url"] == pdf_url
            assert metadata["local_path"].endswith("/source.pdf")
            assert datetime.fromisoformat(metadata["run_timestamp"]).utcoffset() is not None
            matches = app.state.retrieval.search(
                "Saturn", document_ids=[record["id"]], mode="keyword"
            )
            assert matches["hits"][0]["metadata"]["publish_date"] == "2026-11-23"
            assert matches["hits"][0]["metadata"]["chunk_index"] == 0
            assert research_scope(
                "昨天东方财富的 Saturn 行情",
                library.repository.research_records(),
                None,
                today=date(2026, 11, 24),
            ) == [record["id"]]
            assert (
                research_scope(
                    "昨天的 Saturn 行情",
                    library.repository.research_records(),
                    None,
                    today=date(2026, 11, 25),
                )
                == []
            )
            rag = LibraryRAG(app.state.retrieval, lambda *_: None, threading.Event())
            options = {"enabled": True, "mode": "vector"}
            assert rag.begin("2026-11-23 东方财富 Saturn", options)
            evidence = rag.answer_sources()
            assert evidence[0]["document_id"] == record["id"]
            assert evidence[0]["publish_date"] == "2026-11-23"
            assert not rag.begin("2026-11-22 东方财富 Saturn", options)
            assert "扩大筛选范围" in rag.unavailable_answer()
            assert embedding_calls.count("query") == 1
        finally:
            finish.set()
        wait_until(lambda: session.status == "idle", timeout=15)
        assert client.get(_tasks(session_id)).json()["items"] == []
        summary = app.state.application.snapshot(session_id)["history"][-1]["content"][0]["text"]
        assert "符合条件的资料不足 3 份" in summary
        assert "Saturn 数据为 95 [R1]" in summary and pdf_url in summary
        assert ("计划触发" if scheduled else "运行时间") in summary
        assert requests[-3:] == [broken_url, pdf_url, copy_url]
        assert len(requests) == 6
        events = app.state.application.repository.events(session_id)
        trace_id = next(
            event["payload"]["trace_id"] for event in events if event["kind"] == "run_started"
        )
        trace_file = config.data_dir / "traces" / "agent-traces.jsonl"
        attempts = [
            item["attributes"]
            for line in trace_file.read_text(encoding="utf-8").splitlines()
            if (item := json.loads(line))["trace_id"] == trace_id
            and item["name"] == "research.summary_request"
        ]
        assert [
            (
                attempt["research.max_tokens"],
                attempt["research.stop_reason"],
                attempt["research.has_text"],
            )
            for attempt in attempts
            if attempt["model.purpose"] == "research.extract_facts"
        ] == [(1200, "max_tokens", False), (2400, "end_turn", True)]


def test_eastmoney_falls_back_to_homepage_list_and_report_pdf(monkeypatch):
    source = DEFAULT_SOURCES[0]
    code = "AP202611230002"
    canonical = EASTMONEY_PDF.format(code)
    detail = f"{source}info/{code}.html"
    original = f"{canonical}?filename=original.pdf"
    rows = {
        "data": [
            {
                "title": "半导体研报",
                "publishDate": "2026-11-23 08:30:00",
                "infoCode": code,
            }
        ]
    }
    pdf = layout_fixture()

    def fetch(self, url, referer=None):
        if url.startswith(EASTMONEY_LIST + "?"):
            raise httpx.ConnectError("Provider list unavailable")
        return url, {
            source: f"<script>var initdata_stock= {json.dumps(rows, ensure_ascii=False)};</script>",
            canonical: "<html>PDF moved</html>",
            detail: f'<a href="{original}">查看PDF原文</a>',
            original: pdf,
        }[url]

    monkeypatch.setattr(ResearchSources, "fetch", fetch)
    with ResearchSources(1024 * 1024, lambda: None) as crawler:
        candidates, failed = crawler.discover([source], ["半导体"])
        assert failed == 0 and len(candidates) == 1
        assert crawler.download(*candidates[0]) == (original, pdf, "半导体研报", "2026-11-23")


def test_remote_cron_preserves_workflow_and_occurrence(tmp_path, monkeypatch):
    store = CronStore(tmp_path / "cron.sqlite3", "UTC")
    now = datetime.fromisoformat("2026-11-23T07:00:00+00:00")
    monkeypatch.setattr(store, "now", lambda: now)
    scope, session = "1" * 32, "2" * 32
    workflow = {
        "target_industries": ["Saturn"],
        "target_sources": [],
        "report_mode": "market_digest",
    }
    job = store.rpc(
        scope,
        "create",
        session,
        cron="0 7 * * *",
        prompt="Fixture",
        recurring=False,
        durable=True,
        workflow=workflow,
    )
    store.tick()
    delivered = store.rpc(scope, "claim", session)
    assert delivered["job_id"] == job["id"]
    assert delivered["workflow"] == workflow
    assert delivered["run_timestamp"] == now.isoformat()
    assert store.rpc(scope, "renew", session, token=delivered["token"])
    store.rpc(scope, "ack", session, token=delivered["token"])
    assert store.rpc(scope, "list", session) == []
