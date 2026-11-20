"""End-to-end tests for the release two-phase-commit drill.

The drill spans two *separate* PostgreSQL databases, so the suite needs two
connection strings:

* ``TEST_REGISTRY_URL``  - the release-order registration database
* ``TEST_QUOTA_URL``     - the release-quota database

Both must have ``max_prepared_transactions`` > 0 and the connecting role must
be allowed to create tables.  Point them at throwaway databases, e.g. two
databases on one local PostgreSQL instance::

    TEST_REGISTRY_URL=postgresql://u@localhost/registry_test \
    TEST_QUOTA_URL=postgresql://u@localhost/quota_test \
    pytest tests/test_release_2pc.py -q
"""

from __future__ import annotations

import importlib
import os
import sys

import psycopg
import pytest
from fastapi.testclient import TestClient

REGISTRY_URL = os.environ.get("TEST_REGISTRY_URL")
QUOTA_URL = os.environ.get("TEST_QUOTA_URL")
pytestmark = pytest.mark.skipif(
    not (REGISTRY_URL and QUOTA_URL),
    reason="TEST_REGISTRY_URL / TEST_QUOTA_URL not set; 2pc tests skipped",
)


def _execute(url: str, sql: str) -> None:
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute(sql)


def _rollback_all_prepared(url: str) -> None:
    with psycopg.connect(url, autocommit=True) as conn:
        for (gid,) in conn.execute("SELECT gid FROM pg_prepared_xacts").fetchall():
            conn.execute(
                psycopg.sql.SQL("ROLLBACK PREPARED {}").format(
                    psycopg.sql.Literal(gid)
                )
            )


@pytest.fixture()
def web(monkeypatch):
    """Fresh app + store per test, mimicking an application restart."""
    assert REGISTRY_URL and QUOTA_URL
    monkeypatch.setenv("REGISTRY_DATABASE_URL", REGISTRY_URL)
    monkeypatch.setenv("QUOTA_DATABASE_URL", QUOTA_URL)
    monkeypatch.setenv("INITIAL_QUOTA", "5")
    # The row-lock drill is not exercised here; point it at the registry DB so
    # application startup still succeeds.
    monkeypatch.setenv("DATABASE_URL", REGISTRY_URL)
    for name in list(sys.modules):
        if name.startswith("app."):
            del sys.modules[name]
    import app.config
    import app.db
    import app.release_config
    import app.release_db
    importlib.reload(app.config)
    importlib.reload(app.db)
    importlib.reload(app.release_config)
    importlib.reload(app.release_db)
    import app.release_drill
    importlib.reload(app.release_drill)
    import app.release_web
    importlib.reload(app.release_web)
    import app.main
    importlib.reload(app.main)

    client = TestClient(app.main.app)
    with client:
        _rollback_all_prepared(REGISTRY_URL)
        _rollback_all_prepared(QUOTA_URL)
        _execute(REGISTRY_URL, "TRUNCATE release_orders, tx_decisions, order_intents")
        _execute(QUOTA_URL, "TRUNCATE release_quota_ledger")
        _execute(
            QUOTA_URL,
            "UPDATE release_quota SET total = 5, remaining = 5, updated_at = now() "
            "WHERE id = 1",
        )
        app.release_drill.store._flows.clear()
        yield client, app.release_drill.store


def _api(client, order_no: str) -> dict:
    return client.get(f"/release/api/orders/{order_no}").json()["order"]


def _remaining() -> int:
    with psycopg.connect(QUOTA_URL, autocommit=True) as conn:
        return conn.execute(
            "SELECT remaining FROM release_quota WHERE id = 1"
        ).fetchone()[0]


def _prepared(url: str) -> set[str]:
    with psycopg.connect(url, autocommit=True) as conn:
        return {
            r[0]
            for r in conn.execute("SELECT gid FROM pg_prepared_xacts").fetchall()
        }


# ---------------------------------------------------------------------------
# happy path / idempotency / conflict
# ---------------------------------------------------------------------------


def test_happy_path_commits_both_databases(web):
    client, _ = web
    res = client.post(
        "/release/submit",
        data={"order_no": "REL-1", "version": "v1", "pause": "none"},
        follow_redirects=False,
    )
    assert res.status_code == 303
    order = _api(client, "REL-1")
    assert order["registry_state"] == "committed"
    assert order["quota_state"] == "committed"
    assert order["remaining_quota"] == 4
    assert order["decision"] == "commit"
    assert _prepared(REGISTRY_URL) == set() and _prepared(QUOTA_URL) == set()
    with psycopg.connect(REGISTRY_URL, autocommit=True) as conn:
        assert conn.execute(
            "SELECT version FROM release_orders WHERE order_no='REL-1'"
        ).fetchone()[0] == "v1"
    with psycopg.connect(QUOTA_URL, autocommit=True) as conn:
        assert conn.execute(
            "SELECT 1 FROM release_quota_ledger WHERE order_no='REL-1'"
        ).fetchone() is not None


def test_same_order_same_version_is_idempotent(web):
    client, _ = web
    client.post(
        "/release/submit",
        data={"order_no": "REL-1", "version": "v1", "pause": "none"},
    )
    again = client.post(
        "/release/api/submit",
        data={"order_no": "REL-1", "version": "v1", "pause": "none"},
    )
    assert again.status_code == 200
    with psycopg.connect(REGISTRY_URL, autocommit=True) as conn:
        assert conn.execute("SELECT count(*) FROM release_orders").fetchone()[0] == 1
    with psycopg.connect(QUOTA_URL, autocommit=True) as conn:
        assert conn.execute("SELECT count(*) FROM release_quota_ledger").fetchone()[0] == 1
    assert _remaining() == 4


def test_same_order_different_version_conflicts(web):
    client, _ = web
    client.post(
        "/release/submit",
        data={"order_no": "REL-1", "version": "v1", "pause": "none"},
    )
    conflict = client.post(
        "/release/api/submit",
        data={"order_no": "REL-1", "version": "v2", "pause": "none"},
    )
    assert conflict.status_code == 409
    assert conflict.json()["kind"] == "OrderConflict"
    assert _remaining() == 4


# ---------------------------------------------------------------------------
# pause points and restart recovery
# ---------------------------------------------------------------------------


def test_pause_a_then_in_process_continue_completes(web):
    client, store = web
    client.post(
        "/release/submit",
        data={
            "order_no": "REL-2",
            "version": "v2",
            "pause": "after_registry_prepared",
        },
    )
    order = _api(client, "REL-2")
    assert order["registry_state"] == "prepared"
    assert order["quota_state"] == "none"
    assert order["decision"] is None
    assert _remaining() == 5

    res = client.post("/release/orders/REL-2/continue", follow_redirects=False)
    assert res.status_code == 303
    order = _api(client, "REL-2")
    assert order["registry_state"] == "committed"
    assert order["quota_state"] == "committed"
    assert _remaining() == 4


def test_restart_at_pause_a_aborts_without_commit_decision(web):
    client, store = web
    client.post(
        "/release/submit",
        data={
            "order_no": "REL-3",
            "version": "v3",
            "pause": "after_registry_prepared",
        },
    )
    # Simulate a web restart: the in-memory flow map is dropped while the
    # prepared transaction stays in PostgreSQL.
    store._flows.clear()

    res = client.post("/release/orders/REL-3/continue", follow_redirects=False)
    assert res.status_code == 303
    order = _api(client, "REL-3")
    assert order["registry_state"] == "none"
    assert order["quota_state"] == "none"
    assert order["decision"] == "abort"
    assert _remaining() == 5
    assert _prepared(REGISTRY_URL) == set() and _prepared(QUOTA_URL) == set()
    with psycopg.connect(REGISTRY_URL, autocommit=True) as conn:
        assert conn.execute(
            "SELECT count(*) FROM release_orders WHERE order_no='REL-3'"
        ).fetchone()[0] == 0

    page = client.get("/release/orders/REL-3").text
    assert "已撤销" in page


def test_pause_b_in_process_continue_then_restart_resume(web):
    client, store = web
    client.post(
        "/release/submit",
        data={
            "order_no": "REL-PB",
            "version": "vp",
            "pause": "after_registry_committed",
        },
    )
    order = _api(client, "REL-PB")
    assert order["registry_state"] == "committed"
    assert order["quota_state"] == "prepared"
    # In-process continue finishes the quota participant.
    client.post("/release/orders/REL-PB/continue")
    order = _api(client, "REL-PB")
    assert order["quota_state"] == "committed"
    assert _remaining() == 4


def test_restart_at_pause_b_commits_remaining_participant(web):
    client, store = web
    client.post(
        "/release/submit",
        data={
            "order_no": "REL-4",
            "version": "v4",
            "pause": "after_registry_committed",
        },
    )
    order = _api(client, "REL-4")
    assert order["registry_state"] == "committed"
    assert order["quota_state"] == "prepared"
    assert order["decision"] == "commit"
    store._flows.clear()  # restart

    res = client.post("/release/orders/REL-4/continue", follow_redirects=False)
    assert res.status_code == 303
    order = _api(client, "REL-4")
    assert order["registry_state"] == "committed"
    assert order["quota_state"] == "committed"
    assert _remaining() == 4
    page = client.get("/release/orders/REL-4").text
    assert "发布成功" in page


def test_bootstrap_does_not_block_on_prepared_quota_lock(web):
    """A prepared quota transaction holds the quota row lock; restarting the
    web process must not hang while seeding/validating the schema."""
    client, store = web
    client.post(
        "/release/submit",
        data={
            "order_no": "REL-5",
            "version": "v5",
            "pause": "after_registry_committed",
        },
    )
    assert _prepared(QUOTA_URL)  # the row lock is held by the prepared txn
    # Must return promptly instead of blocking on the row lock.
    store.ensure_schema()
    # Recover so the fixture leaves nothing locked.
    client.post("/release/orders/REL-5/continue")


# ---------------------------------------------------------------------------
# quota shortage / aborted-order reuse / unknown order
# ---------------------------------------------------------------------------


def test_insufficient_quota_neither_side_takes_effect(web):
    client, _ = web
    with psycopg.connect(QUOTA_URL, autocommit=True) as conn:
        conn.execute("UPDATE release_quota SET remaining = 0 WHERE id = 1")
    order = client.post(
        "/release/api/submit",
        data={"order_no": "REL-6", "version": "v6", "pause": "none"},
    ).json()["order"]
    assert order["registry_state"] == "none"
    assert order["quota_state"] == "none"
    assert order["decision"] == "abort"
    assert order["decision_reason"] == "发布额度不足"
    assert _remaining() == 0
    assert _prepared(REGISTRY_URL) == set() and _prepared(QUOTA_URL) == set()
    with psycopg.connect(REGISTRY_URL, autocommit=True) as conn:
        assert conn.execute(
            "SELECT count(*) FROM release_orders WHERE order_no='REL-6'"
        ).fetchone()[0] == 0


def test_aborted_order_number_can_be_reused(web):
    client, _ = web
    with psycopg.connect(QUOTA_URL, autocommit=True) as conn:
        conn.execute("UPDATE release_quota SET remaining = 0 WHERE id = 1")
    client.post(
        "/release/api/submit",
        data={"order_no": "REL-7", "version": "v7a", "pause": "none"},
    )
    with psycopg.connect(QUOTA_URL, autocommit=True) as conn:
        conn.execute(
            "UPDATE release_quota SET remaining = 1, total = 6 WHERE id = 1"
        )
    order = client.post(
        "/release/api/submit",
        data={"order_no": "REL-7", "version": "v7b", "pause": "none"},
    ).json()["order"]
    assert order["registry_state"] == "committed"
    assert order["quota_state"] == "committed"
    with psycopg.connect(REGISTRY_URL, autocommit=True) as conn:
        row = conn.execute(
            "SELECT version, (SELECT decision FROM tx_decisions WHERE order_no='REL-7') "
            "FROM release_orders WHERE order_no='REL-7'"
        ).fetchone()
    assert row == ("v7b", "commit")


def test_unknown_order_continue_is_rejected(web):
    client, _ = web
    res = client.post("/release/orders/NOPE/continue", follow_redirects=False)
    assert res.status_code == 400
