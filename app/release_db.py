"""Database access for the release two-phase-commit drill.

Two independent PostgreSQL databases take part:

* ``registry`` — the release-order registration database.  It owns the
  ``release_orders`` table (committed registrations) and the durable
  ``tx_decisions`` log, which records the commit/abort decision *outside* the
  XA transaction so it survives both prepared transactions and restarts.
* ``quota`` — the release-quota database.  It owns the single-row
  ``release_quota`` account that every successful release debits by one.

Each 2PC participant uses one deterministic global transaction id (GID)
derived from the order number, so a recovered process can always find the
prepared transaction belonging to an order without any process-local state.
"""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row

from .release_config import release_settings

# Participant labels used in GIDs, prepared-transaction lookups and the UI.
REGISTRY = "registry"
QUOTA = "quota"

# States reported per participant, read from the databases themselves.
STATE_NONE = "none"          # no registration / no deduction visible
STATE_PREPARED = "prepared"  # a prepared transaction is holding the change
STATE_COMMITTED = "committed"  # the change is durably committed

# Durable decision log values.
DECISION_COMMIT = "commit"
DECISION_ABORT = "abort"


def _connect(dsn: str, *, application_name: str, autocommit: bool) -> psycopg.Connection[Any]:
    return psycopg.connect(
        dsn,
        connect_timeout=release_settings.connect_timeout,
        autocommit=autocommit,
        row_factory=dict_row,
        application_name=application_name,
    )


def connect_registry(*, autocommit: bool = False) -> psycopg.Connection[Any]:
    return _connect(
        release_settings.registry_url,
        application_name=f"{release_settings.application_name}:registry",
        autocommit=autocommit,
    )


def connect_quota(*, autocommit: bool = False) -> psycopg.Connection[Any]:
    return _connect(
        release_settings.quota_url,
        application_name=f"{release_settings.application_name}:quota",
        autocommit=autocommit,
    )


def gid_for(order_no: str) -> str:
    """Deterministic GID shared by both participants for one order.

    PostgreSQL prepared-transaction identifiers are limited to 200 chars and
    must be unique within a database; the order number is unique by design.
    """
    return f"xa_rel_{order_no}"[:200]


def ensure_schemas() -> None:
    """Create every table and seed the quota account (idempotent).

    Bootstrap connections set a short ``lock_timeout`` and seed data only
    after an MVCC existence check: while recovering a paused drill, a prepared
    transaction may still hold a row lock on ``release_quota``, and a plain
    ``INSERT ... ON CONFLICT`` would block on it forever at startup.
    """
    with connect_registry() as conn:
        conn.execute("SET lock_timeout = '3000'")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS release_orders (
                order_no   text PRIMARY KEY,
                version    text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        # The decision log is plain DDL/DML in its own transactions, never
        # part of a PREPARE TRANSACTION, so a row here is durable regardless
        # of what the prepared transactions do.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tx_decisions (
                order_no     text PRIMARY KEY,
                gid          text NOT NULL,
                decision     text NOT NULL CHECK (decision IN ('commit', 'abort')),
                reason       text,
                decided_at   timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        # In-flight versions, written outside the XA transaction so the
        # version of a paused (prepared-only) order survives a web restart and
        # conflict checks still work. Rows are deleted once the 2PC finishes.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS order_intents (
                order_no    text PRIMARY KEY,
                gid         text NOT NULL,
                version     text NOT NULL,
                created_at  timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        conn.commit()

    with connect_quota() as conn:
        conn.execute("SET lock_timeout = '3000'")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS release_quota (
                id         integer PRIMARY KEY,
                total      integer NOT NULL,
                remaining  integer NOT NULL CHECK (remaining >= 0),
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        # Per-order deduction ledger: its visible rows give an unambiguous,
        # order-keyed answer to "has the quota participant committed for this
        # order?" without relying on timestamps.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS release_quota_ledger (
                order_no   text PRIMARY KEY,
                amount     integer NOT NULL CHECK (amount = 1),
                created_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        # READ COMMITTED SELECT never blocks on a row lock held by a prepared
        # transaction; only insert when the account is genuinely missing.
        account = conn.execute(
            "SELECT 1 FROM release_quota WHERE id = 1"
        ).fetchone()
        if account is None:
            conn.execute(
                """
                INSERT INTO release_quota (id, total, remaining)
                VALUES (1, %s, %s)
                """,
                (release_settings.initial_quota, release_settings.initial_quota),
            )
        conn.commit()
