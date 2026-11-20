"""Configuration for the release two-phase-commit drill."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:  # pragma: no cover - startup misconfiguration
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc


@dataclass(frozen=True)
class ReleaseSettings:
    # The two participants live in two separate PostgreSQL databases.
    registry_url: str
    quota_url: str
    application_name: str
    # Initial quota seeded into a brand-new quota database.
    initial_quota: int
    # Serialises 2PC orchestration inside the single web worker. PostgreSQL
    # itself remains the source of truth for every prepared transaction.
    lock_timeout_seconds: int
    connect_timeout: int

    @classmethod
    def from_env(cls) -> "ReleaseSettings":
        return cls(
            registry_url=os.environ.get(
                "REGISTRY_DATABASE_URL",
                "postgresql://drill:drill@localhost:5432/drill",
            ),
            quota_url=os.environ.get(
                "QUOTA_DATABASE_URL",
                "postgresql://drill:drill@localhost:5432/drill",
            ),
            application_name=os.environ.get("RELEASE_APP_NAME", "releasedrill"),
            initial_quota=_int_env("INITIAL_QUOTA", 5),
            lock_timeout_seconds=_int_env("RELEASE_LOCK_TIMEOUT", 30),
            connect_timeout=_int_env("RELEASE_DB_CONNECT_TIMEOUT", 5),
        )


release_settings = ReleaseSettings.from_env()
