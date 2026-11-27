"""SQLite persistence is isolated from model execution and HTTP routes."""

import asyncio
import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from uuid import uuid4

from ..core.messages import json_value, repair_interrupted_turn
from .session_cache import HISTORY_PAGE_SIZE


SESSION_FLAGS = {"personalized": 1, "temporary": 0, "archived": 0}
EMPTY_TEXT_VERSION = {"id": 0, "created_at": ""}


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def _session_record(row):
    result = {**dict(row), **{key: bool(row[key]) for key in SESSION_FLAGS}}
    result.pop("revision", None)
    return result


def _user_text_event(row, payload):
    text = payload.get("text")
    return {**dict(row), "payload": {"text": text if isinstance(text, str) else ""}}


class SessionRepository:
    def __init__(self, path, *, cache=None):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.lock = threading.RLock()
        self.cache = cache
        with self.connection() as database:
            database.executescript("""
                CREATE TABLE IF NOT EXISTS sessions (
                    id TEXT PRIMARY KEY, title TEXT NOT NULL, history TEXT NOT NULL,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    personalized INTEGER NOT NULL DEFAULT 1,
                    temporary INTEGER NOT NULL DEFAULT 0,
                    archived INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL,
                    kind TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS event_session ON events(session_id, id);
                CREATE INDEX IF NOT EXISTS event_session_user ON events(session_id, id)
                    WHERE kind = 'user';
                CREATE TABLE IF NOT EXISTS session_cleanup (session_id TEXT PRIMARY KEY);
            """)
            database.execute("BEGIN IMMEDIATE")
            columns = {row["name"] for row in database.execute("PRAGMA table_info(sessions)")}
            for name, default in SESSION_FLAGS.items():
                if name not in columns:
                    database.execute(
                        f"ALTER TABLE sessions ADD COLUMN {name} INTEGER NOT NULL DEFAULT {default}"
                    )
            if "revision" not in columns:
                database.execute("ALTER TABLE sessions ADD COLUMN revision INTEGER NOT NULL DEFAULT 0")

    def start(self):
        if self.cache is not None:
            self.cache.start()

    def close(self):
        if self.cache is not None:
            self.cache.close()

    def notification_version(self, session_id):
        return self.cache.version(session_id) if self.cache is not None else 0

    def notify_draft(self, session_id):
        if self.cache is not None:
            self.cache.notify_local(session_id)

    async def wait_for_events(self, session_id, version, *, timeout=2.0):
        if self.cache is not None:
            await self.cache.wait(session_id, version, timeout=timeout)
        else:
            await asyncio.sleep(min(timeout, 0.3))

    def cache_status(self):
        return self.cache.status() if self.cache is not None else {
            "configured": False, "cache_available": False, "notifications_available": False,
        }

    def _changed(self, session_id, *, deleted=False):
        if self.cache is not None:
            if deleted:
                self.cache.discard(session_id)
            self.cache.publish(session_id)

    @contextmanager
    def connection(self):
        with self.lock:
            database = sqlite3.connect(self.path, timeout=30)
            database.row_factory = sqlite3.Row
            try:
                with database:
                    yield database
            finally:
                database.close()

    def create(self, *, personalized=True, temporary=False):
        session_id, now = uuid4().hex, timestamp()
        with self.connection() as database:
            database.execute(
                "INSERT INTO sessions(id,title,history,created_at,updated_at,personalized,temporary) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (session_id, "新对话", "[]", now, now, personalized, temporary),
            )
        return self.get(session_id)

    def get(self, session_id):
        # Model context (including tool_use/tool_result) is restored only from SQL.
        with self.connection() as database:
            row = database.execute("SELECT * FROM sessions WHERE id = ?", (session_id,)).fetchone()
        if row is None:
            raise KeyError(session_id)
        result = _session_record(row)
        result["history"] = json.loads(result["history"])
        repair_interrupted_turn(result["history"])
        return result

    def exists(self, session_id):
        with self.connection() as database:
            return (
                database.execute("SELECT 1 FROM sessions WHERE id = ?", (session_id,)).fetchone()
                is not None
            )

    def list(self, *, archived=False):
        with self.connection() as database:
            return [
                _session_record(row)
                for row in database.execute(
                    "SELECT id,title,created_at,updated_at,personalized,temporary,archived "
                    "FROM sessions WHERE archived = ? ORDER BY updated_at DESC",
                    (archived,),
                )
            ]

    def update(self, session_id, *, archived=None, personalized=None, temporary=None):
        with self.connection() as database:
            updated = database.execute(
                "UPDATE sessions SET archived=COALESCE(?, archived), "
                "personalized=COALESCE(?, personalized), temporary=COALESCE(?, temporary), "
                "revision=revision+1 "
                "WHERE id = ?",
                (archived, personalized, temporary, session_id),
            )
            if updated.rowcount != 1:
                raise KeyError(session_id)
            row = database.execute(
                "SELECT personalized,temporary,archived,revision FROM sessions WHERE id = ?",
                (session_id,),
            ).fetchone()
        self._changed(session_id)
        return _session_record(row)

    def delete(self, session_id):
        with self.connection() as database:
            deleted = database.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
            if deleted.rowcount != 1:
                raise KeyError(session_id)
            database.execute("DELETE FROM events WHERE session_id = ?", (session_id,))
            database.execute("INSERT INTO session_cleanup VALUES (?)", (session_id,))
        self._changed(session_id, deleted=True)

    def pending_cleanup(self):
        with self.connection() as database:
            return [row[0] for row in database.execute("SELECT session_id FROM session_cleanup")]

    def finish_cleanup(self, session_id):
        with self.connection() as database:
            database.execute("DELETE FROM session_cleanup WHERE session_id = ?", (session_id,))

    def save(self, session_id, title, history):
        serialized = json.dumps(json_value(history), ensure_ascii=False)
        with self.connection() as database:
            updated = database.execute(
                "UPDATE sessions SET title = ?, history = ?, updated_at = ?, revision=revision+1 "
                "WHERE id = ? AND archived = 0",
                (title, serialized, timestamp(), session_id),
            )
        if updated.rowcount:
            self._changed(session_id)

    def emit(self, session_id, kind, payload):
        payload = json_value(payload)
        created_at = timestamp()
        warm_text = kind == "user" and self.cache is not None and self.cache.can_access
        with self.connection() as database:
            inserted = database.execute(
                "INSERT INTO events(session_id, kind, payload, created_at) "
                "SELECT ?, ?, ?, ? FROM sessions WHERE id = ? AND archived = 0",
                (
                    session_id,
                    kind,
                    json.dumps(payload, ensure_ascii=False),
                    created_at,
                    session_id,
                ),
            )
            if inserted.rowcount and warm_text:
                previous = database.execute(
                    "SELECT id,created_at FROM events WHERE session_id=? AND id<? "
                    "AND kind='user' ORDER BY id DESC LIMIT 1",
                    (session_id, inserted.lastrowid),
                ).fetchone()
        if inserted.rowcount:
            self._changed(session_id)
            if warm_text and self.cache.can_access:
                version = {"id": inserted.lastrowid, "created_at": created_at}
                event = _user_text_event({**version, "session_id": session_id, "kind": kind}, payload)
                self._warm_user_text(session_id, dict(previous) if previous else EMPTY_TEXT_VERSION,
                                     version, event)

    def _warm_user_text(self, session_id, previous, version, event):
        # A valid cached prefix avoids rereading historical text from SQLite.
        texts = self.cache.get(session_id, previous) if previous["id"] else []
        if texts is not None:
            texts.append(event)
        elif self.cache.can_access:
            texts = self._recent_user_events(session_id, version["id"])
        if texts and texts[-1]["id"] == version["id"]:
            self.cache.put(session_id, version, texts)

    def _recent_user_events(self, session_id, through):
        with self.connection() as database:
            rows = database.execute(
                "SELECT * FROM events WHERE session_id=? AND kind='user' "
                "AND id<=? ORDER BY id DESC LIMIT ?", (session_id, through, HISTORY_PAGE_SIZE),
            ).fetchall()
        return [_user_text_event(row, json.loads(row["payload"])) for row in reversed(rows)]

    def history_page(self, session_id, *, before=None, through=None, part="all"):
        """Load a full page, or one part of the latest page at a fixed event ID."""
        use_cache = (before is None and part != "remaining"
                     and self.cache is not None and self.cache.can_access)
        selective = use_cache or part != "all"
        columns = "id,session_id,kind,created_at" if selective else "*"
        seek, params = "", (session_id,)
        if before is not None:
            seek, params = " AND id < ?", (*params, before)
        elif through is not None:
            seek, params = " AND id <= ?", (*params, through)
        with self.connection() as database:
            database.execute("BEGIN")
            if database.execute("SELECT 1 FROM sessions WHERE id=?", (session_id,)).fetchone() is None:
                raise KeyError(session_id)
            rows = database.execute(
                f"SELECT {columns} FROM events WHERE session_id=?{seek} ORDER BY id DESC LIMIT ?",
                (*params, HISTORY_PAGE_SIZE + 1),
            ).fetchall()
        page = list(reversed(rows[:HISTORY_PAGE_SIZE]))

        if selective:
            selected = page
            if part != "all":
                selected = [row for row in page if (row["kind"] == "user") == (part == "user")]
            result = self._hydrate_history(session_id, selected, use_cache=use_cache)
        else:
            result = [{**dict(row), "payload": json.loads(row["payload"])} for row in page]
        return {
            "events": result,
            "cursor": page[-1]["id"] if page else 0,
            "before": page[0]["id"] if len(rows) > HISTORY_PAGE_SIZE else None,
        }

    def _hydrate_history(self, session_id, rows, *, use_cache):
        latest = next((row for row in reversed(rows) if row["kind"] == "user"), None)
        version = {"id": latest["id"], "created_at": latest["created_at"]} if latest is not None else None
        cached = self.cache.get(session_id, version) if use_cache and version else None
        by_id = {event["id"]: event for event in cached or []}
        missing = []
        for row in rows:
            event = by_id.get(row["id"])
            if event is None or row["kind"] != "user" or event["created_at"] != row["created_at"]:
                by_id.pop(row["id"], None)
                missing.append(row["id"])
        if missing:
            placeholders = ",".join("?" for _ in missing)
            with self.connection() as database:
                for row in database.execute(
                    f"SELECT * FROM events WHERE session_id=? AND id IN ({placeholders})",
                    (session_id, *missing),
                ):
                    payload = json.loads(row["payload"])
                    by_id[row["id"]] = (
                        _user_text_event(row, payload) if row["kind"] == "user"
                        else {**dict(row), "payload": payload}
                    )
        result = [by_id[row["id"]] for row in rows if row["id"] in by_id]
        if use_cache and version and cached is None:
            texts = [event for event in result if event["kind"] == "user"]
            if texts and texts[-1]["id"] == version["id"]:
                self.cache.put(session_id, version, texts)
        return result

    def events(self, session_id, after=0, *, through=None):
        with self.connection() as database:
            rows = database.execute(
                "SELECT * FROM events WHERE session_id = ? AND id > ? AND (? IS NULL OR id <= ?) ORDER BY id LIMIT 200",
                (session_id, after, through, through),
            ).fetchall()
        return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]

    def event_page(self, session_id, after=0, *, through=None, snapshot=False):
        """Read existence, replay boundary and one page using one SQLite snapshot.

        A fixed replay boundary excludes later writes while allowing pagination
        beyond 200 rows. The SSE event stream reads SQL directly.
        """
        with self.connection() as database:
            database.execute("BEGIN")
            exists = database.execute(
                "SELECT 1 FROM sessions WHERE id = ?", (session_id,)
            ).fetchone() is not None
            if snapshot:
                through = database.execute(
                    "SELECT COALESCE(MAX(id), 0) FROM events WHERE session_id = ?", (session_id,)
                ).fetchone()[0]
            rows = database.execute(
                "SELECT * FROM events WHERE session_id = ? AND id > ? "
                "AND (? IS NULL OR id <= ?) ORDER BY id LIMIT 200",
                (session_id, after, through, through),
            ).fetchall() if exists else []
        return {
            "exists": exists,
            "through": through,
            "events": [{**dict(row), "payload": json.loads(row["payload"])} for row in rows],
        }

    def cursor(self, session_id):
        with self.connection() as database:
            return database.execute(
                "SELECT COALESCE(MAX(id), 0) FROM events WHERE session_id = ?",
                (session_id,),
            ).fetchone()[0]

    def has_transcript(self, session_id):
        with self.connection() as database:
            return database.execute(
                "SELECT 1 FROM events WHERE session_id=? "
                "AND kind IN ('user','assistant','scheduled') LIMIT 1", (session_id,),
            ).fetchone() is not None
