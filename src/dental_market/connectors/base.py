"""Reusable connector contracts and collection controls.

The module deliberately has no dependency on a specific HTTP client.  A source
connector implements raw-byte fetch plus pagination-only decoding; retries,
rate limiting, pagination, and durable watermarks are handled by
:class:`ConnectorRunner`. Record parsing happens later through the versioned
parser registry.
"""

from __future__ import annotations

import abc
import email.utils
import hashlib
import json
import re
import threading
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import (
    Any,
    Callable,
    Iterator,
    Mapping,
    Optional,
    Protocol,
    Tuple,
    Union,
)


RetryAfter = Union[str, int, float]


class _FrozenParams(dict):
    """JSON-compatible mapping that cannot be changed after construction."""

    @staticmethod
    def _immutable(*_args: Any, **_kwargs: Any) -> None:
        raise TypeError("request params are immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable
    __ior__ = _immutable


def freeze_json_mapping(params: Mapping[str, Any]) -> Mapping[str, Any]:
    """Detach JSON values and recursively expose an immutable mapping.

    Request parameters are part of the durable collection-scope fingerprint.
    A shallow frozen dataclass is insufficient because both the outer mapping
    and nested values can otherwise be mutated after hashing.  A JSON
    round-trip also enforces the same portable value domain used by manifests.
    """

    if not isinstance(params, Mapping):
        raise TypeError("request params must be a mapping")
    try:
        encoded = json.dumps(
            dict(params),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        detached = json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "request params must contain JSON-compatible values"
        ) from exc
    if not isinstance(detached, dict):  # defensive; ``dict(params)`` is an object
        raise ValueError("request params must encode as a JSON object")

    def freeze(value: Any) -> Any:
        if isinstance(value, dict):
            return _FrozenParams(
                {key: freeze(nested) for key, nested in value.items()}
            )
        if isinstance(value, list):
            return tuple(freeze(nested) for nested in value)
        return value

    return freeze(detached)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


@dataclass(frozen=True)
class Request:
    """One logical page request sent to a connector."""

    dataset: str
    params: Mapping[str, Any] = field(default_factory=dict)
    cursor: Optional[str] = None
    watermark: Optional[str] = None
    page_size: Optional[int] = None
    snapshot_token: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.dataset.strip():
            raise ValueError("dataset must be non-empty")
        if self.cursor is not None and not self.cursor.strip():
            raise ValueError("cursor must be non-empty when provided")
        if self.watermark is not None and not self.watermark.strip():
            raise ValueError("watermark must be non-empty when provided")
        if self.snapshot_token is not None and not self.snapshot_token.strip():
            raise ValueError("snapshot_token must be non-empty when provided")
        if self.page_size is not None and self.page_size <= 0:
            raise ValueError("page_size must be positive")
        object.__setattr__(self, "params", freeze_json_mapping(self.params))


@dataclass(frozen=True)
class RawResponse:
    """Exact response body plus a deliberately small, non-secret envelope.

    The body is never normalized here.  Source-specific parsing happens only
    after these bytes have been written to immutable raw storage.  Response
    headers are metadata, not a request dump, and sensitive header names are
    rejected so credentials cannot enter manifests accidentally.
    """

    body: bytes
    content_type: str = "application/octet-stream"
    status_code: int = 200
    response_headers: Mapping[str, str] = field(default_factory=dict)
    retrieved_at: str = field(default_factory=_utc_now_iso)
    source_as_of: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.body, bytes):
            raise TypeError("raw response body must be bytes")
        if not self.content_type.strip():
            raise ValueError("raw response content_type must be non-empty")
        if self.status_code < 100 or self.status_code > 599:
            raise ValueError("raw response status_code must be a valid HTTP status")
        if not self.retrieved_at.strip():
            raise ValueError("raw response retrieved_at must be non-empty")
        if self.source_as_of is not None and not self.source_as_of.strip():
            raise ValueError("raw response source_as_of must be non-empty")
        normalized = {}
        for name, value in self.response_headers.items():
            lowered = str(name).strip().lower()
            if lowered in {
                "authorization",
                "proxy-authorization",
                "cookie",
                "set-cookie",
                "x-api-key",
                "api-key",
            }:
                raise ValueError("sensitive response headers cannot be captured")
            normalized[lowered] = str(value)
        object.__setattr__(self, "response_headers", normalized)


@dataclass(frozen=True)
class Page:
    """Pagination metadata decoded only after exact response capture.

    ``next_cursor`` is opaque to the runner. ``watermark`` should describe the
    newest completely represented source state and is committed only after the
    whole pagination chain succeeds. Record parsing deliberately does not
    belong here; versioned raw parsers are the single record truth.
    """

    next_cursor: Optional[str] = None
    watermark: Optional[str] = None
    source_as_of: Optional[str] = None
    snapshot_fingerprint: Optional[str] = None
    record_count: Optional[int] = None
    total_count: Optional[int] = None

    def __post_init__(self) -> None:
        if self.next_cursor is not None and not self.next_cursor.strip():
            raise ValueError("next_cursor must be non-empty when provided")
        if self.watermark is not None and not self.watermark.strip():
            raise ValueError("watermark must be non-empty when provided")
        if self.source_as_of is not None and not self.source_as_of.strip():
            raise ValueError("source_as_of must be non-empty when provided")
        if self.snapshot_fingerprint is not None and (
            not isinstance(self.snapshot_fingerprint, str)
            or re.fullmatch(r"[0-9a-f]{64}", self.snapshot_fingerprint) is None
        ):
            raise ValueError(
                "snapshot_fingerprint must be a lowercase SHA-256 digest"
            )
        for field_name, value in (
            ("record_count", self.record_count),
            ("total_count", self.total_count),
        ):
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                raise ValueError(field_name + " must be a non-negative integer")
        if (
            self.record_count is not None
            and self.total_count is not None
            and self.record_count > self.total_count
        ):
            raise ValueError("record_count cannot exceed total_count")


class Connector(Protocol):
    """Structural interface accepted by :class:`ConnectorRunner`."""

    source: str
    parser_id: str
    parser_version: str

    def fetch_raw_page(self, request: Request) -> RawResponse:
        ...

    def decode_page(self, request: Request, response: RawResponse) -> Page:
        ...


class BaseConnector(abc.ABC):
    """Convenience base class for concrete source connectors."""

    source: str
    parser_id = "unavailable"
    parser_version = "0"

    @abc.abstractmethod
    def fetch_raw_page(self, request: Request) -> RawResponse:
        """Fetch exact response bytes without parsing the response body."""

    @abc.abstractmethod
    def decode_page(self, request: Request, response: RawResponse) -> Page:
        """Decode pagination metadata after the response has been persisted."""


class WatermarkStore(Protocol):
    """External persistence contract used to resume incremental collection."""

    def get_watermark(self, source_id: str, dataset_id: str) -> Optional[str]:
        ...

    def set_watermark(
        self, source_id: str, dataset_id: str, value: str
    ) -> None:
        ...


class InMemoryWatermarkStore:
    """Small reference implementation useful for tests and local samples."""

    def __init__(self) -> None:
        self._values: dict[Tuple[str, str], str] = {}

    def get_watermark(self, source_id: str, dataset_id: str) -> Optional[str]:
        return self._values.get((source_id, dataset_id))

    def set_watermark(self, source_id: str, dataset_id: str, value: str) -> None:
        self._values[(source_id, dataset_id)] = value

    # Compatibility aliases for callers written during the M1 prototype.
    def read_watermark(self, source: str, dataset: str) -> Optional[str]:
        return self.get_watermark(source, dataset)

    def write_watermark(self, source: str, dataset: str, watermark: str) -> None:
        self.set_watermark(source, dataset, watermark)


class ConnectorError(RuntimeError):
    """Normalized source error with enough metadata for retry decisions."""

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        retry_after: Optional[RetryAfter] = None,
        retryable: Optional[bool] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after
        self.retryable = retryable


class PaginationLoopError(ConnectorError):
    """Raised when a source repeats a cursor instead of advancing."""


@dataclass(frozen=True)
class RetryPolicy:
    """Deterministic exponential-backoff policy."""

    max_attempts: int = 4
    base_delay_seconds: float = 0.5
    max_delay_seconds: float = 30.0
    retryable_status_codes: Tuple[int, ...] = (
        408,
        425,
        429,
        500,
        502,
        503,
        504,
    )

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay_seconds < 0:
            raise ValueError("base_delay_seconds must not be negative")
        if self.max_delay_seconds < 0:
            raise ValueError("max_delay_seconds must not be negative")

    def should_retry(self, error: ConnectorError, attempts_made: int) -> bool:
        if attempts_made >= self.max_attempts:
            return False
        if error.retryable is not None:
            return error.retryable
        return error.status_code in self.retryable_status_codes

    def delay_seconds(
        self,
        failures_seen: int,
        retry_after: Optional[RetryAfter] = None,
        *,
        now: Optional[datetime] = None,
    ) -> float:
        """Return the delay before retrying after ``failures_seen`` failures.

        A server supplied ``Retry-After`` is treated as a lower bound.  Unlike
        local exponential backoff it is not clipped by ``max_delay_seconds``.
        """

        if failures_seen < 1:
            raise ValueError("failures_seen must be at least 1")
        exponential = min(
            self.max_delay_seconds,
            self.base_delay_seconds * (2 ** (failures_seen - 1)),
        )
        server_delay = parse_retry_after(retry_after, now=now)
        return max(exponential, server_delay or 0.0)


def parse_retry_after(
    value: Optional[RetryAfter], *, now: Optional[datetime] = None
) -> Optional[float]:
    """Parse a Retry-After seconds value or RFC 7231 HTTP date."""

    if value is None:
        return None
    if isinstance(value, (int, float)):
        return max(0.0, float(value))

    stripped = value.strip()
    if not stripped:
        return None
    try:
        return max(0.0, float(stripped))
    except ValueError:
        pass

    try:
        retry_at = email.utils.parsedate_to_datetime(stripped)
    except (TypeError, ValueError, OverflowError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    reference = now or datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    return max(0.0, (retry_at - reference).total_seconds())


class RateLimiter:
    """Thread-safe minimum-interval limiter with injectable time functions."""

    def __init__(
        self,
        requests_per_second: Optional[float] = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if requests_per_second is not None and requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        self._interval = (
            0.0 if requests_per_second is None else 1.0 / requests_per_second
        )
        self._clock = clock
        self._sleeper = sleeper
        self._next_allowed_at: Optional[float] = None
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = self._clock()
            if self._next_allowed_at is not None and now < self._next_allowed_at:
                self._sleeper(self._next_allowed_at - now)
                now = self._clock()
            self._next_allowed_at = max(
                now, self._next_allowed_at if self._next_allowed_at is not None else now
            ) + self._interval


@dataclass(frozen=True)
class CapturedPage:
    """One successfully fetched page and the cursor used to request it."""

    page_index: int
    request_cursor: Optional[str]
    raw_response: RawResponse


@dataclass(frozen=True)
class CollectionResult:
    source: str
    dataset: str
    pages_fetched: int
    starting_watermark: Optional[str]
    ending_watermark: Optional[str]
    raw_pages: Tuple[CapturedPage, ...] = ()


class ConnectorRunner:
    """Apply operational controls around a source-specific connector."""

    def __init__(
        self,
        connector: Connector,
        *,
        watermark_store: Optional[WatermarkStore] = None,
        retry_policy: Optional[RetryPolicy] = None,
        rate_limiter: Optional[RateLimiter] = None,
        sleeper: Callable[[float], None] = time.sleep,
        page_observer: Optional[Callable[[CapturedPage], None]] = None,
        retain_raw_pages: bool = True,
    ) -> None:
        if not connector.source.strip():
            raise ValueError("connector.source must be non-empty")
        self.connector = connector
        self.watermark_store = watermark_store
        self.retry_policy = retry_policy or RetryPolicy()
        self.rate_limiter = rate_limiter or RateLimiter()
        self.sleeper = sleeper
        if not retain_raw_pages and page_observer is None:
            raise ValueError(
                "a page_observer is required when raw pages are not retained"
            )
        self.page_observer = page_observer
        self.retain_raw_pages = retain_raw_pages

    def collect(self, request: Request) -> CollectionResult:
        return self.collect_from(request)

    def collect_from(
        self,
        request: Request,
        *,
        initial_page_index: int = 0,
        decoded_page_observer: Optional[
            Callable[[CapturedPage, Page], None]
        ] = None,
    ) -> CollectionResult:
        """Collect a page chain, optionally continuing an audited attempt.

        ``initial_page_index`` only controls the durable page numbering exposed
        to observers.  The caller remains responsible for proving that any
        earlier pages belong to the same stable snapshot before starting from
        a non-empty request cursor.  The decoded callback runs strictly after
        the raw-page callback, preserving the raw-before-parse invariant.
        """

        self._validate_initial_page_index(initial_page_index)
        starting_watermark = self._resolve_watermark(request)
        pages_fetched = 0
        ending_watermark = starting_watermark
        raw_pages = []

        for page, captured_page in self._iter_pages_with_raw(
            request,
            watermark=starting_watermark,
            initial_page_index=initial_page_index,
            decoded_page_observer=decoded_page_observer,
        ):
            pages_fetched += 1
            if self.retain_raw_pages:
                raw_pages.append(captured_page)
            if page.watermark is not None:
                ending_watermark = page.watermark

        return CollectionResult(
            source=self.connector.source,
            dataset=request.dataset,
            pages_fetched=pages_fetched,
            starting_watermark=starting_watermark,
            ending_watermark=ending_watermark,
            raw_pages=tuple(raw_pages),
        )

    def iter_pages(
        self,
        request: Request,
        watermark: Optional[str] = None,
        *,
        initial_page_index: int = 0,
        decoded_page_observer: Optional[
            Callable[[CapturedPage, Page], None]
        ] = None,
    ) -> Iterator[Page]:
        """Yield a fully controlled pagination stream.

        The external watermark is updated after, and only after, the generator
        reaches the natural end of the page chain. Callers that stop consuming
        early therefore leave the previous resume point intact.
        """

        self._validate_initial_page_index(initial_page_index)
        for page, _ in self._iter_pages_with_raw(
            request,
            watermark=watermark,
            initial_page_index=initial_page_index,
            decoded_page_observer=decoded_page_observer,
        ):
            yield page

    def _iter_pages_with_raw(
        self,
        request: Request,
        watermark: Optional[str] = None,
        *,
        initial_page_index: int = 0,
        decoded_page_observer: Optional[
            Callable[[CapturedPage, Page], None]
        ] = None,
    ) -> Iterator[Tuple[Page, CapturedPage]]:
        starting_watermark = (
            watermark if watermark is not None else self._resolve_watermark(request)
        )
        ending_watermark = starting_watermark
        cursor = request.cursor
        seen_cursors = set()
        if cursor is not None:
            seen_cursors.add(cursor)
        page_index = initial_page_index

        while True:
            page_request = replace(
                request, cursor=cursor, watermark=starting_watermark
            )
            raw_response = self._fetch_raw_with_retry(page_request)
            captured_page = CapturedPage(
                page_index=page_index,
                request_cursor=cursor,
                raw_response=raw_response,
            )
            if self.page_observer is not None:
                self.page_observer(captured_page)
            if not self._is_success_status(raw_response.status_code):
                raise ConnectorError(
                    "connector returned non-success HTTP status %d"
                    % raw_response.status_code,
                    status_code=raw_response.status_code,
                    retryable=False,
                )
            page = self.connector.decode_page(page_request, raw_response)
            if decoded_page_observer is not None:
                decoded_page_observer(captured_page, page)
            if page.watermark is not None:
                ending_watermark = page.watermark
            yield page, captured_page

            if page.next_cursor is None:
                break
            if page.next_cursor in seen_cursors:
                cursor_digest = hashlib.sha256(
                    page.next_cursor.encode("utf-8")
                ).hexdigest()
                raise PaginationLoopError(
                    "connector repeated pagination cursor "
                    "fingerprint=%s length=%d"
                    % (cursor_digest, len(page.next_cursor)),
                    retryable=False,
                )
            seen_cursors.add(page.next_cursor)
            cursor = page.next_cursor
            page_index += 1

        if (
            self.watermark_store is not None
            and ending_watermark is not None
            and ending_watermark != starting_watermark
        ):
            self.watermark_store.set_watermark(
                self.connector.source, request.dataset, ending_watermark
            )

    def _resolve_watermark(self, request: Request) -> Optional[str]:
        if request.watermark is not None:
            return request.watermark
        if self.watermark_store is None:
            return None
        return self.watermark_store.get_watermark(
            self.connector.source, request.dataset
        )

    def _fetch_raw_with_retry(self, request: Request) -> RawResponse:
        attempts_made = 0
        while True:
            attempts_made += 1
            self.rate_limiter.wait()
            try:
                response = self.connector.fetch_raw_page(request)
                if self._is_success_status(response.status_code):
                    return response
                error = ConnectorError(
                    "connector returned non-success HTTP status %d"
                    % response.status_code,
                    status_code=response.status_code,
                )
                if not self.retry_policy.should_retry(error, attempts_made):
                    # Return the final response so the caller can persist its
                    # exact bytes before failing closed.
                    return response
                delay = self.retry_policy.delay_seconds(
                    failures_seen=attempts_made,
                    retry_after=response.response_headers.get("retry-after"),
                )
                if delay > 0:
                    self.sleeper(delay)
                continue
            except ConnectorError as error:
                if not self.retry_policy.should_retry(error, attempts_made):
                    raise
                delay = self.retry_policy.delay_seconds(
                    failures_seen=attempts_made,
                    retry_after=error.retry_after,
                )
                if delay > 0:
                    self.sleeper(delay)

    @staticmethod
    def _is_success_status(status_code: int) -> bool:
        return 200 <= status_code < 300 or status_code == 304

    @staticmethod
    def _validate_initial_page_index(value: int) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("initial_page_index must be a non-negative integer")


def collect(
    connector: Connector,
    request: Request,
    *,
    watermark_store: Optional[WatermarkStore] = None,
    retry_policy: Optional[RetryPolicy] = None,
    rate_limiter: Optional[RateLimiter] = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> CollectionResult:
    """One-call convenience wrapper around :class:`ConnectorRunner`."""

    return ConnectorRunner(
        connector,
        watermark_store=watermark_store,
        retry_policy=retry_policy,
        rate_limiter=rate_limiter,
        sleeper=sleeper,
    ).collect(request)
