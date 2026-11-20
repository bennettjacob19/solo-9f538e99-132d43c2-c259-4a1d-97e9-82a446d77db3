"""Two-phase-commit orchestration for the release drill.

A release registration must atomically (a) register the release order in the
*registry* database and (b) debit one unit of release quota in the separate
*quota* database.  The drill drives PostgreSQL's native two-phase commit
(``PREPARE TRANSACTION`` / ``COMMIT PREPARED`` / ``ROLLBACK PREPARED``) across
the two databases.

Design points
-------------
* Both participants use the same deterministic GID (``gid_for(order_no)``), so
  after a web restart the prepared transactions are discoverable from the
  order number alone — nothing transactional lives in web memory.
* The durable *decision* is recorded in ``tx_decisions`` (registry database)
  in its own auto-commit transaction, written **before** any participant is
  committed.  On restart: a prepared transaction without a decision row is
  aborted; with one, the recorded decision is driven to completion.
* Every status shown on the page is read live from the databases
  (``pg_prepared_xacts`` plus the visible table rows), never assumed.
* An in-memory flow map only exists to keep a deliberately paused flow alive
  within one web process; a restart drops it and the durable recovery rules
  take over instead.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

import psycopg
from psycopg import sql

from .release_db import (
    DECISION_ABORT,
    DECISION_COMMIT,
    QUOTA,
    REGISTRY,
    STATE_COMMITTED,
    STATE_NONE,
    STATE_PREPARED,
    connect_quota,
    connect_registry,
    ensure_schemas,
    gid_for,
)

logger = logging.getLogger("releasedrill")


class ReleaseError(Exception):
    """Base class for release-drill control errors."""

    status_code = 400


class OrderConflict(ReleaseError):
    """Same order number submitted again with a different version."""

    status_code = 409


class OrderBusy(ReleaseError):
    """Another request is already driving this order."""

    status_code = 409


# Pause points requested for a drill run.
PAUSE_AFTER_REGISTRY_PREPARED = "after_registry_prepared"
PAUSE_AFTER_REGISTRY_COMMITTED = "after_registry_committed"
PAUSE_NONE = "none"
VALID_PAUSES = {
    PAUSE_AFTER_REGISTRY_PREPARED,
    PAUSE_AFTER_REGISTRY_COMMITTED,
    PAUSE_NONE,
}


def _prepared_exists(conn: psycopg.Connection[Any], gid: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM pg_prepared_xacts WHERE gid = %s", (gid,)
    ).fetchone()
    return row is not None


def _prepared_versions(conn: psycopg.Connection[Any]) -> dict[str, str]:
    """Map order number -> version for orders currently in flight.

    The version is persisted in ``order_intents`` outside the XA transaction,
    so a restarted web process can still enforce one-registration and detect
    version conflicts for prepared-only (paused) orders.
    """
    rows = conn.execute(
        "SELECT order_no, version FROM order_intents"
    ).fetchall()
    return {row["order_no"]: row["version"] for row in rows}


def _save_intent(
    order_no: str, gid: str, version: str
) -> None:
    """Persist the in-flight version in its own auto-commit transaction."""
    with connect_registry(autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO order_intents (order_no, gid, version)
            VALUES (%s, %s, %s)
            ON CONFLICT (order_no) DO UPDATE
                SET gid = EXCLUDED.gid,
                    version = EXCLUDED.version,
                    created_at = now()
            """,
            (order_no, gid, version),
        )


def _delete_intent(order_no: str) -> None:
    with connect_registry(autocommit=True) as conn:
        conn.execute("DELETE FROM order_intents WHERE order_no = %s", (order_no,))


# PREPARE/COMMIT/ROLLBACK TRANSACTION do not accept bind parameters; the GID
# is generated server-side from the order number and embedded as a literal.
def _prepare_transaction(conn: psycopg.Connection[Any], gid: str) -> None:
    conn.execute(
        sql.SQL("PREPARE TRANSACTION {}").format(sql.Literal(gid))
    )


def _commit_prepared_sql(conn: psycopg.Connection[Any], gid: str) -> None:
    conn.execute(
        sql.SQL("COMMIT PREPARED {}").format(sql.Literal(gid))
    )


def _rollback_prepared_sql(conn: psycopg.Connection[Any], gid: str) -> None:
    conn.execute(
        sql.SQL("ROLLBACK PREPARED {}").format(sql.Literal(gid))
    )


def _registry_committed(conn: psycopg.Connection[Any], order_no: str) -> dict[str, Any] | None:
    return conn.execute(
        "SELECT order_no, version, created_at FROM release_orders WHERE order_no = %s",
        (order_no,),
    ).fetchone()


def _get_decision(conn: psycopg.Connection[Any], order_no: str) -> dict[str, Any] | None:
    return conn.execute(
        "SELECT order_no, gid, decision, reason, decided_at "
        "FROM tx_decisions WHERE order_no = %s",
        (order_no,),
    ).fetchone()


def _record_decision(
    order_no: str, gid: str, decision: str, reason: str | None
) -> None:
    """Persist the commit/abort decision in its own auto-commit transaction.

    An auto-commit connection guarantees the row is durable independently of
    any XA participant, which is exactly what restart recovery relies on.
    """
    with connect_registry(autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO tx_decisions (order_no, gid, decision, reason)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (order_no) DO UPDATE
                SET gid = EXCLUDED.gid,
                    decision = EXCLUDED.decision,
                    reason = EXCLUDED.reason,
                    decided_at = now()
            """,
            (order_no, gid, decision, reason),
        )


def _remaining_quota(conn: psycopg.Connection[Any]) -> dict[str, int]:
    row = conn.execute(
        "SELECT total, remaining FROM release_quota WHERE id = 1"
    ).fetchone()
    return {"total": row["total"], "remaining": row["remaining"]}


def _quota_deduction_committed(
    conn: psycopg.Connection[Any], order_no: str
) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM release_quota_ledger WHERE order_no = %s",
            (order_no,),
        ).fetchone()
        is not None
    )


def _prepared_holds_quota(conn: psycopg.Connection[Any], gid: str) -> bool:
    """Whether a prepared quota transaction is currently holding (but not yet
    committing) the one-unit deduction for this order."""
    row = conn.execute(
        """
        SELECT count(*) AS n
        FROM pg_prepared_xacts x
        JOIN pg_locks l ON l.transactionid = x.transaction
        WHERE x.gid = %s AND l.relation = 'release_quota'::regclass
        """,
        (gid,),
    ).fetchone()
    return (row["n"] or 0) > 0


class _Flow:
    """Process-local bookkeeping of one deliberately paused drill flow."""

    def __init__(self, order_no: str, version: str, pause: str) -> None:
        self.order_no = order_no
        self.version = version
        self.pause = pause
        self.hint = ""


class ReleaseStore:
    """Thread-safe orchestrator.  One instance per web process."""

    def __init__(self) -> None:
        self._global = threading.RLock()
        self._order_locks: dict[str, threading.Lock] = dict()
        self._flows: dict[str, _Flow] = {}

    # -- helpers ---------------------------------------------------------
    def _lock_for(self, order_no: str) -> threading.Lock:
        with self._global:
            lock = self._order_locks.get(order_no)
            if lock is None:
                lock = threading.Lock()
                self._order_locks[order_no] = lock
            return lock

    def _get_flow(self, order_no: str) -> _Flow | None:
        with self._global:
            return self._flows.get(order_no)

    def _put_flow(self, flow: _Flow) -> None:
        with self._global:
            self._flows[flow.order_no] = flow

    def _drop_flow(self, order_no: str) -> None:
        with self._global:
            self._flows.pop(order_no, None)

    def ensure_schema(self) -> None:
        ensure_schemas()

    # -- state observation ----------------------------------------------
    def exists(self, order_no: str) -> bool:
        """True if the order has any durable footprint or a live flow."""
        gid = gid_for(order_no)
        with connect_registry(autocommit=True) as reg, connect_quota(
            autocommit=True
        ) as quota:
            if _registry_committed(reg, order_no) is not None:
                return True
            if _get_decision(reg, order_no) is not None:
                return True
            if order_no in _prepared_versions(reg):
                return True
            if _prepared_exists(reg, gid) or _prepared_exists(quota, gid):
                return True
        return self._get_flow(order_no) is not None

    def get_order(self, order_no: str) -> dict[str, Any]:
        """Reconstruct the full order view from the live databases."""
        gid = gid_for(order_no)
        with connect_registry(autocommit=True) as reg, connect_quota(
            autocommit=True
        ) as quota:
            order_row = _registry_committed(reg, order_no)
            reg_prepared = _prepared_exists(reg, gid)
            decision = _get_decision(reg, order_no)
            quota_prepared = _prepared_exists(quota, gid)
            quota_deducted = _quota_deduction_committed(quota, order_no)
            quota_view = _remaining_quota(quota)
            quota_holds = _prepared_holds_quota(quota, gid)
            prepared_versions = _prepared_versions(reg)

        if order_row is not None:
            registry_state = STATE_COMMITTED
        elif reg_prepared:
            registry_state = STATE_PREPARED
        else:
            registry_state = STATE_NONE

        if quota_deducted:
            quota_state = STATE_COMMITTED
        elif quota_prepared:
            quota_state = STATE_PREPARED
        else:
            quota_state = STATE_NONE

        flow = self._get_flow(order_no)
        fallback_version = (
            flow.version if flow else prepared_versions.get(order_no)
        )
        return {
            "order_no": order_no,
            "gid": gid,
            "version": order_row["version"] if order_row else fallback_version,
            "created_at": order_row["created_at"].isoformat(sep=" ") if order_row else None,
            "registry_state": registry_state,
            "quota_state": quota_state,
            "decision": decision["decision"] if decision else None,
            "decision_reason": decision["reason"] if decision else None,
            "decided_at": decision["decided_at"].isoformat(sep=" ") if decision else None,
            "quota_total": quota_view["total"],
            "remaining_quota": quota_view["remaining"],
            "quota_prepared_holds_deduction": quota_holds,
            "pause": flow.pause if flow else None,
            "hint": flow.hint if flow else "",
            "in_memory_flow": flow is not None,
        }

    # -- new submission --------------------------------------------------
    def submit(
        self, order_no: str, version: str, pause: str
    ) -> dict[str, Any]:
        order_no = (order_no or "").strip()
        version = (version or "").strip()
        if not order_no:
            raise ReleaseError("发布单号不能为空。")
        if not version:
            raise ReleaseError("制品版本不能为空。")
        if pause not in VALID_PAUSES:
            raise ReleaseError(f"未知的暂停点：{pause!r}")
        gid = gid_for(order_no)

        lock = self._lock_for(order_no)
        if not lock.acquire(blocking=False):
            raise OrderBusy("该发布单正在另一个请求中处理，请稍后刷新查看。")
        try:
            # Reject conflicting versions against durable history, the
            # XA-independent in-flight intents (so this still works after a
            # restart), or a still-open in-memory flow.
            with connect_registry(autocommit=True) as reg:
                existing = _registry_committed(reg, order_no)
                existing_prepared = _prepared_exists(reg, gid)
                existing_decision = _get_decision(reg, order_no)
                prepared_versions = _prepared_versions(reg)
            flow = self._get_flow(order_no)
            prepared_version = prepared_versions.get(order_no)
            known_version = (
                existing["version"]
                if existing
                else (
                    prepared_version
                    if prepared_version is not None
                    else flow.version if flow else None
                )
            )
            if known_version is not None and known_version != version:
                raise OrderConflict(
                    f"发布单号 {order_no} 已登记版本 {known_version}，"
                    f"与本次提交的版本 {version} 冲突；每单至多登记一次。"
                )
            # A previous attempt that ended in abort never registered the
            # order nor deducted quota: the order number may be reused.
            prior_abort = (
                existing_decision is not None
                and existing_decision["decision"] == DECISION_ABORT
                and existing is None
                and not existing_prepared
            )
            if prior_abort:
                # Keep the abort history for audit but allow a fresh 2PC run.
                pass
            elif existing is not None or existing_prepared or existing_decision:
                # Idempotent reuse: drive/return the existing order instead of
                # registering or deducting a second time.
                return self._resume_locked(order_no)

            return self._run_new_flow(order_no, version, pause, gid)
        finally:
            lock.release()

    def _run_new_flow(
        self, order_no: str, version: str, pause: str, gid: str
    ) -> dict[str, Any]:
        # ---- Phase 1a: prepare the registry participant -----------------
        try:
            with connect_registry() as reg:
                reg.execute(
                    "INSERT INTO release_orders (order_no, version) VALUES (%s, %s)",
                    (order_no, version),
                )
                _prepare_transaction(reg, gid)
        except psycopg.Error as exc:
            raise ReleaseError(
                f"登记库预备失败，两边均未生效：{type(exc).__name__}: {exc}"
            ) from exc

        # Durable, XA-independent record of the in-flight version.
        _save_intent(order_no, gid, version)

        flow = _Flow(order_no, version, pause)
        self._put_flow(flow)

        # Deliberate stop: registry prepared, quota never touched.
        if pause == PAUSE_AFTER_REGISTRY_PREPARED:
            flow.hint = (
                "已停在暂停点 A：登记库事务已 PREPARE，额度库尚未开始预备。"
                "重启 Web 后因无持久化提交决定，继续处理将撤销该预备事务。"
            )
            return self.get_order(order_no)

        return self._prepare_quota_and_decide(order_no, gid, flow)

    def _prepare_quota_and_decide(
        self, order_no: str, gid: str, flow: _Flow
    ) -> dict[str, Any]:
        # ---- Phase 1b: prepare the quota participant --------------------
        quota_abort_reason: str | None = None
        try:
            with connect_quota() as quota:
                row = quota.execute(
                    "SELECT remaining FROM release_quota WHERE id = 1 FOR UPDATE"
                ).fetchone()
                if row is None or row["remaining"] < 1:
                    quota_abort_reason = "发布额度不足"
                    quota.rollback()
                else:
                    quota.execute(
                        """
                        UPDATE release_quota
                        SET remaining = remaining - 1, updated_at = now()
                        WHERE id = 1
                        """,
                    )
                    quota.execute(
                        "INSERT INTO release_quota_ledger (order_no, amount) "
                        "VALUES (%s, 1)",
                        (order_no,),
                    )
                    _prepare_transaction(quota, gid)
        except psycopg.Error as exc:
            quota_abort_reason = f"额度库预备异常：{type(exc).__name__}: {exc}"

        if quota_abort_reason is not None:
            # Abort path: durable abort decision first, then roll back the
            # registry participant. Nothing becomes effective on either side.
            _record_decision(order_no, gid, DECISION_ABORT, quota_abort_reason)
            self._rollback_prepared(REGISTRY, gid)
            _delete_intent(order_no)
            self._drop_flow(order_no)
            return self.get_order(order_no)

        # Both participants prepared: the commit decision is now durable.
        _record_decision(order_no, gid, DECISION_COMMIT, None)

        # ---- Phase 2a: commit the registry participant first ------------
        self._commit_prepared(REGISTRY, gid)

        # Deliberate stop: registry committed, quota still prepared.
        if flow.pause == PAUSE_AFTER_REGISTRY_COMMITTED:
            flow.hint = (
                "已停在暂停点 B：登记库已 COMMIT PREPARED，额度库仍处于 PREPARED。"
                "重启 Web 后存在持久化提交决定，继续处理将补提交额度库。"
            )
            return self.get_order(order_no)

        # ---- Phase 2b: commit the quota participant ---------------------
        self._commit_prepared(QUOTA, gid)
        _delete_intent(order_no)
        self._drop_flow(order_no)
        return self.get_order(order_no)

    # -- resume / recovery ----------------------------------------------
    def continue_order(self, order_no: str) -> dict[str, Any]:
        order_no = (order_no or "").strip()
        if not order_no:
            raise ReleaseError("发布单号不能为空。")
        lock = self._lock_for(order_no)
        if not lock.acquire(blocking=False):
            raise OrderBusy("该发布单正在另一个请求中处理，请稍后刷新查看。")
        try:
            return self._resume_locked(order_no)
        finally:
            lock.release()

    def _resume_locked(self, order_no: str) -> dict[str, Any]:
        gid = gid_for(order_no)
        with connect_registry(autocommit=True) as reg, connect_quota(
            autocommit=True
        ) as quota:
            order_row = _registry_committed(reg, order_no)
            reg_prepared = _prepared_exists(reg, gid)
            decision = _get_decision(reg, order_no)
            quota_prepared = _prepared_exists(quota, gid)
            intents = _prepared_versions(reg)
            flow = self._get_flow(order_no)
        has_intent = order_no in intents

        # Nothing durable and nothing live: unknown order.
        if (
            order_row is None
            and not reg_prepared
            and decision is None
            and flow is None
            and not has_intent
        ):
            raise ReleaseError(
                f"未找到发布单 {order_no}，没有可继续处理的事务。"
            )

        # Terminal states: nothing left to prepare or commit (idempotent
        # re-submission / repeat "continue" of the same order).
        if not reg_prepared and not quota_prepared:
            view = self.get_order(order_no)
            if order_row is not None:
                view["resume_message"] = "该发布单两库均已提交，沿用原单，未重复登记或扣减。"
            else:
                view["resume_message"] = "该单此前已撤销，预备事务均不存在，两边均未生效。"
            _delete_intent(order_no)
            self._drop_flow(order_no)
            return view

        # No durable decision yet.
        if decision is None:
            if flow is not None and reg_prepared and not quota_prepared:
                # Same web process, stopped at pause A: continue the 2PC flow
                # exactly where it stopped (prepare quota, then decide).
                self._prepare_quota_and_decide(order_no, gid, flow)
                view = self.get_order(order_no)
                view["resume_message"] = "已从暂停点 A 继续：完成额度库预备并按两阶段提交推进。"
                return view
            # Restart (in-memory flow lost), or any prepared participant
            # without a persisted commit decision: record a durable abort
            # decision first, then undo every prepared transaction.
            _record_decision(
                order_no,
                gid,
                DECISION_ABORT,
                "重启恢复时无持久化提交决定，撤销预备事务",
            )
            if reg_prepared:
                self._rollback_prepared(REGISTRY, gid)
            if quota_prepared:
                self._rollback_prepared(QUOTA, gid)
            _delete_intent(order_no)
            self._drop_flow(order_no)
            view = self.get_order(order_no)
            view["resume_message"] = (
                "未发现持久化的提交决定，已撤销该单全部预备事务，两边均未生效。"
            )
            return view

        if decision["decision"] == DECISION_ABORT:
            if reg_prepared:
                self._rollback_prepared(REGISTRY, gid)
            if quota_prepared:
                self._rollback_prepared(QUOTA, gid)
            _delete_intent(order_no)
            self._drop_flow(order_no)
            view = self.get_order(order_no)
            view["resume_message"] = (
                f"提交决定为撤销（{decision['reason'] or '无原因'}），"
                "预备事务已全部撤销，两边均未生效。"
            )
            return view

        # A persisted commit decision -> drive every participant to completion.
        if reg_prepared:
            self._commit_prepared(REGISTRY, gid)
        if quota_prepared:
            self._commit_prepared(QUOTA, gid)
        _delete_intent(order_no)
        self._drop_flow(order_no)
        view = self.get_order(order_no)
        if view["registry_state"] == STATE_COMMITTED and view["quota_state"] == STATE_COMMITTED:
            view["resume_message"] = "提交决定已落实，登记库与额度库均已提交。"
        else:
            view["resume_message"] = "继续处理完成，请按库中实况核对两库状态。"
        return view

    # -- listing ---------------------------------------------------------
    def list_orders(self, limit: int = 20) -> dict[str, Any]:
        """Recent orders plus the current quota, all read from the databases."""
        with connect_registry(autocommit=True) as reg, connect_quota(
            autocommit=True
        ) as quota:
            rows = reg.execute(
                """
                SELECT o.order_no, o.version, o.created_at,
                       d.decision, d.reason, d.decided_at
                FROM release_orders o
                LEFT JOIN tx_decisions d ON d.order_no = o.order_no
                ORDER BY o.created_at DESC, o.order_no DESC
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
            # Prepared-only orders never made it into release_orders; surface
            # them from the decision log, the in-flight intents and the live
            # prepared transactions.
            prepared_orders = reg.execute(
                """
                SELECT x.gid, x.prepared AS prepared_at
                FROM pg_prepared_xacts x
                WHERE x.gid LIKE 'xa_rel_%%'
                ORDER BY x.prepared DESC
                """
            ).fetchall()
            intents = reg.execute(
                "SELECT order_no FROM order_intents ORDER BY created_at DESC"
            ).fetchall()
            decisions = reg.execute(
                """
                SELECT order_no, gid, decision, reason, decided_at
                FROM tx_decisions
                ORDER BY decided_at DESC
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
            quota_view = _remaining_quota(quota)
            quota_prepared = quota.execute(
                "SELECT gid FROM pg_prepared_xacts WHERE gid LIKE 'xa_rel_%%'"
            ).fetchall()

        known = {row["order_no"] for row in rows}
        order_nos = [row["order_no"] for row in rows]
        # Include abort-only / prepared-only order numbers not in release_orders.
        for row in decisions:
            if row["order_no"] not in known:
                known.add(row["order_no"])
                order_nos.append(row["order_no"])
        for row in intents:
            if row["order_no"] not in known:
                known.add(row["order_no"])
                order_nos.append(row["order_no"])
        for row in prepared_orders:
            no = row["gid"][len("xa_rel_"):]
            if no not in known:
                known.add(no)
                order_nos.append(no)

        items = [self.get_order(no) for no in order_nos[:limit]]
        return {
            "orders": items,
            "quota_total": quota_view["total"],
            "remaining_quota": quota_view["remaining"],
            "quota_prepared_count": len(quota_prepared),
            "registry_prepared_count": len(prepared_orders),
        }

    # -- low-level participant operations --------------------------------
    def _commit_prepared(self, participant: str, gid: str) -> None:
        connector = connect_registry if participant == REGISTRY else connect_quota
        try:
            with connector(autocommit=True) as conn:
                _commit_prepared_sql(conn, gid)
            logger.info("committed prepared txn %s on %s", gid, participant)
        except psycopg.errors.UndefinedObject:
            # Already committed/rolled back by a previous attempt: idempotent.
            logger.info("prepared txn %s absent on %s (already finished)", gid, participant)

    def _rollback_prepared(self, participant: str, gid: str) -> None:
        connector = connect_registry if participant == REGISTRY else connect_quota
        try:
            with connector(autocommit=True) as conn:
                _rollback_prepared_sql(conn, gid)
            logger.info("rolled back prepared txn %s on %s", gid, participant)
        except psycopg.errors.UndefinedObject:
            logger.info("prepared txn %s absent on %s (already finished)", gid, participant)


# Module-level singleton used by the web layer.
store = ReleaseStore()
