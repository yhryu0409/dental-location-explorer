"""Deterministic connector used by the M1 sample pipeline and unit tests."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from typing import Any, Mapping, Optional, Sequence, Union

from .base import (
    BaseConnector,
    ConnectorError,
    Page,
    RawResponse,
    Request,
    RetryAfter,
)


FixtureRow = Mapping[str, Any]


class FixtureConnector(BaseConnector):
    """Serve in-memory rows using stable offset cursors and snapshot watermarks.

    ``transient_failures`` maps a cursor (``None`` means the first page) to the
    number of retryable 503 failures to emit before returning that page.  It
    makes failure/retry behavior reproducible without a network or credential.
    """

    parser_id = "jsonl-records"
    parser_version = "1"

    def __init__(
        self,
        rows_by_dataset: Union[
            Mapping[str, Sequence[FixtureRow]], Sequence[FixtureRow]
        ],
        page_size: Optional[int] = None,
        *,
        dataset: str = "sample",
        source: str = "fixture",
        default_page_size: Optional[int] = None,
        transient_failures: Optional[Mapping[Optional[str], int]] = None,
        retry_after: Optional[RetryAfter] = None,
    ) -> None:
        if not source.strip():
            raise ValueError("source must be non-empty")
        if page_size is not None and default_page_size is not None:
            raise ValueError("use either page_size or default_page_size, not both")
        effective_page_size = (
            page_size
            if page_size is not None
            else (default_page_size if default_page_size is not None else 100)
        )
        if effective_page_size <= 0:
            raise ValueError("page_size must be positive")
        self.source = source
        self.default_page_size = effective_page_size
        if isinstance(rows_by_dataset, Mapping):
            self._rows_by_dataset = {
                name: tuple(rows) for name, rows in rows_by_dataset.items()
            }
        else:
            if not dataset.strip():
                raise ValueError("dataset must be non-empty")
            self._rows_by_dataset = {dataset: tuple(rows_by_dataset)}
        self._remaining_failures = defaultdict(int)
        if transient_failures:
            self._remaining_failures.update(transient_failures)
        self.retry_after = retry_after
        self.requested_cursors = []

    def fetch_raw_page(self, request: Request) -> RawResponse:
        if request.dataset not in self._rows_by_dataset:
            raise ConnectorError(
                "unknown fixture dataset: {}".format(request.dataset),
                status_code=404,
                retryable=False,
            )

        self.requested_cursors.append(request.cursor)
        if self._remaining_failures[request.cursor] > 0:
            self._remaining_failures[request.cursor] -= 1
            raise ConnectorError(
                "planned fixture failure",
                status_code=503,
                retry_after=self.retry_after,
            )

        rows = self._rows_by_dataset[request.dataset]
        snapshot_watermark = self._snapshot_watermark(rows)
        if request.cursor is None and request.watermark == snapshot_watermark:
            return RawResponse(
                body=b"",
                content_type="application/x-ndjson; charset=utf-8",
                status_code=304,
                response_headers={"etag": snapshot_watermark},
                source_as_of=snapshot_watermark,
            )

        offset = self._parse_cursor(request.cursor, len(rows))
        page_size = request.page_size or self.default_page_size
        upper = min(offset + page_size, len(rows))
        page_rows = rows[offset:upper]
        raw_body = self._raw_page_bytes(page_rows)
        return RawResponse(
            body=raw_body,
            content_type="application/x-ndjson; charset=utf-8",
            response_headers={"etag": snapshot_watermark},
            source_as_of=snapshot_watermark,
        )

    def decode_page(self, request: Request, response: RawResponse) -> Page:
        """Decode pagination only; record parsing belongs to the registry."""

        if response.status_code == 304:
            return Page(watermark=response.source_as_of)
        rows = self._rows_by_dataset[request.dataset]
        offset = self._parse_cursor(request.cursor, len(rows))
        page_size = request.page_size or self.default_page_size
        upper = min(offset + page_size, len(rows))
        next_cursor = str(upper) if upper < len(rows) else None
        return Page(
            next_cursor=next_cursor,
            watermark=response.source_as_of if next_cursor is None else None,
        )

    @staticmethod
    def _raw_page_bytes(rows: Sequence[FixtureRow]) -> bytes:
        serialized = [dict(row) for row in rows]
        if not serialized:
            return b""
        return (
            "\n".join(
                json.dumps(
                    row,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                for row in serialized
            )
            + "\n"
        ).encode("utf-8")

    @staticmethod
    def _parse_cursor(cursor: Optional[str], row_count: int) -> int:
        if cursor is None:
            return 0
        try:
            offset = int(cursor)
        except ValueError as error:
            raise ConnectorError(
                "invalid fixture cursor: {!r}".format(cursor), retryable=False
            ) from error
        if offset < 0 or offset > row_count:
            raise ConnectorError(
                "fixture cursor is outside the dataset", retryable=False
            )
        return offset

    @staticmethod
    def _snapshot_watermark(rows: Sequence[FixtureRow]) -> str:
        serializable = [dict(row) for row in rows]
        canonical = json.dumps(
            serializable,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return "sha256:{}".format(hashlib.sha256(canonical).hexdigest())
