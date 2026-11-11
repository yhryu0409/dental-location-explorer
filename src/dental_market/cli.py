"""Command-line interface for M1 collection, rebuild, and release inspection."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

from .config import ConfigurationError, Settings
from .connectors import ConnectorError
from .contracts import ContractViolation, RawIntegrityError, ReleaseValidationError
from .gate import run_gate
from .pipeline import DentalMarketPipeline, PipelineError, run_sample
from .security import redact_sensitive
from .transforms import TransformContractError


class CliUsageError(ValueError):
    """Raised instead of printing an unstructured argparse error."""


class JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise CliUsageError(message)


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = JsonArgumentParser(prog="dental-market")
    parser.add_argument(
        "--data-root",
        type=Path,
        help="runtime data root; overrides DENTAL_DATA_ROOT",
    )
    subparsers = parser.add_subparsers(
        dest="command", required=True, parser_class=JsonArgumentParser
    )

    sample = subparsers.add_parser(
        "sample-run", help="collect the synthetic M1 fixture and publish a mart"
    )
    sample.add_argument("--fixture", type=Path)
    sample.add_argument("--as-of-date")
    sample.add_argument("--page-size", type=_positive_int, default=2)
    sample.add_argument("--run-id")

    rebuild = subparsers.add_parser(
        "rebuild",
        help="recreate staging/core/mart from one immutable raw page collection",
    )
    rebuild.add_argument("--source", required=True)
    rebuild.add_argument("--dataset", required=True)
    rebuild.add_argument("--collection-hash", required=True)
    rebuild.add_argument("--as-of-date", required=True)
    rebuild.add_argument("--run-id")
    rebuild.add_argument("--publish", action="store_true")

    subparsers.add_parser(
        "current-release", help="show and verify the last-good release"
    )

    gate = subparsers.add_parser(
        "m1-gate", help="run all Gate 1 checks in a fresh runtime directory"
    )
    gate.add_argument(
        "--runs-root",
        type=Path,
        help="parent directory for the fresh Gate 1 runtime",
    )
    return parser


def _settings(data_root: Optional[Path]) -> Settings:
    values = dict(os.environ)
    if data_root is not None:
        values["DENTAL_DATA_ROOT"] = str(data_root)
    return Settings.from_env(values)


def _print(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def main(argv: Optional[Sequence[str]] = None) -> int:
    try:
        args = _parser().parse_args(argv)
        settings = _settings(args.data_root)
        if args.command == "sample-run":
            kwargs: Dict[str, Any] = {
                "as_of_date": args.as_of_date,
                "page_size": args.page_size,
                "run_id": args.run_id,
            }
            if args.fixture is not None:
                kwargs["fixture_path"] = args.fixture
            result = run_sample(settings, **kwargs)
            _print(result.to_dict())
            return 0

        pipeline = DentalMarketPipeline(settings)
        if args.command == "rebuild":
            result = pipeline.rebuild_from_raw(
                args.source,
                args.dataset,
                args.collection_hash,
                as_of_date=args.as_of_date,
                run_id=args.run_id,
                publish=args.publish,
            )
            _print(result.to_dict())
            return 0
        if args.command == "current-release":
            pointer = pipeline.release_manager.current()
            if pointer is None:
                _print({"status": "missing", "current_release": None})
                return 3
            _print(
                {
                    "status": "verified",
                    "current_release": pointer.to_dict(),
                    "manifest": pipeline.release_manager.current_manifest(),
                }
            )
            return 0
        if args.command == "m1-gate":
            result = run_gate(settings, base_root=args.runs_root)
            _print(result.to_dict())
            return 0 if result.passed else 2
        raise AssertionError("unhandled command")
    except (
        ConfigurationError,
        ConnectorError,
        ContractViolation,
        CliUsageError,
        json.JSONDecodeError,
        OSError,
        PipelineError,
        RawIntegrityError,
        ReleaseValidationError,
        TransformContractError,
        UnicodeError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error_type": type(exc).__name__,
                    "error": redact_sensitive(exc),
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2 if isinstance(exc, CliUsageError) else 1


if __name__ == "__main__":
    raise SystemExit(main())
