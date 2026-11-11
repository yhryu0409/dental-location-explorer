"""Typed, environment-only runtime configuration.

This module intentionally does not read ``.env`` files. Deployments may inject
variables through their own secret manager; local developers can use a shell or
an explicit dotenv loader outside this package. Keeping that boundary here makes
it harder to copy secrets into manifests, logs, or release artifacts by accident.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional


PACKAGE_ROOT = Path(__file__).resolve().parent
_SOURCE_PROJECT_ROOT = PACKAGE_ROOT.parents[1]
PROJECT_ROOT = (
    _SOURCE_PROJECT_ROOT
    if (_SOURCE_PROJECT_ROOT / "pyproject.toml").is_file()
    else Path.cwd().resolve()
)


class ConfigurationError(ValueError):
    """Raised when a runtime setting is missing or malformed."""


def _read_int(values: Mapping[str, str], name: str, default: int) -> int:
    raw = values.get(name, "").strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc
    if parsed < 1:
        raise ConfigurationError(f"{name} must be greater than zero")
    return parsed


def _read_float(values: Mapping[str, str], name: str, default: float) -> float:
    raw = values.get(name, "").strip()
    if not raw:
        return default
    try:
        parsed = float(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be a number") from exc
    if parsed <= 0:
        raise ConfigurationError(f"{name} must be greater than zero")
    return parsed


@dataclass(frozen=True)
class Settings:
    """Non-secret settings shared by collectors and pipeline commands."""

    data_root: Path
    environment: str = "development"
    log_level: str = "INFO"
    http_timeout_seconds: float = 30.0
    max_retries: int = 4
    retry_base_seconds: float = 1.0
    rate_limit_per_second: float = 5.0
    release_retention: int = 5

    @classmethod
    def from_env(
        cls,
        environ: Optional[Mapping[str, str]] = None,
        *,
        default_data_root: Optional[Path] = None,
    ) -> "Settings":
        """Build validated settings from a supplied mapping or ``os.environ``.

        ``default_data_root`` is injectable so tests and one-off runs never need
        to mutate the process environment. Relative paths are resolved from the
        current working directory.
        """

        values = os.environ if environ is None else environ
        fallback_root = default_data_root or (PROJECT_ROOT / "var")
        configured_root = values.get("DENTAL_DATA_ROOT", "").strip()
        data_root = Path(configured_root).expanduser() if configured_root else fallback_root

        environment = values.get("DENTAL_ENV", "development").strip() or "development"
        log_level = values.get("DENTAL_LOG_LEVEL", "INFO").strip().upper() or "INFO"
        allowed_levels = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
        if log_level not in allowed_levels:
            allowed = ", ".join(sorted(allowed_levels))
            raise ConfigurationError(f"DENTAL_LOG_LEVEL must be one of: {allowed}")

        return cls(
            data_root=data_root.resolve(),
            environment=environment,
            log_level=log_level,
            http_timeout_seconds=_read_float(
                values, "DENTAL_HTTP_TIMEOUT_SECONDS", 30.0
            ),
            max_retries=_read_int(values, "DENTAL_MAX_RETRIES", 4),
            retry_base_seconds=_read_float(
                values, "DENTAL_RETRY_BASE_SECONDS", 1.0
            ),
            rate_limit_per_second=_read_float(
                values, "DENTAL_RATE_LIMIT_PER_SECOND", 5.0
            ),
            release_retention=_read_int(values, "DENTAL_RELEASE_RETENTION", 5),
        )

    def ensure_data_directories(self) -> None:
        """Create the runtime root; layer-specific code owns its subdirectories."""

        self.data_root.mkdir(parents=True, exist_ok=True)


def require_secret(name: str, environ: Optional[Mapping[str, str]] = None) -> str:
    """Read a named secret without persisting it in ``Settings``.

    Connectors call this only when the corresponding source is used. The value
    must never be included in request manifests, exception text, or logs.
    """

    values = os.environ if environ is None else environ
    value = values.get(name, "").strip()
    if not value:
        raise ConfigurationError(f"required environment variable is missing: {name}")
    return value
