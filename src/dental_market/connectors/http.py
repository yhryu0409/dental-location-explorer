"""Small, dependency-free HTTPS GET transport for official data connectors.

The transport deliberately stops at exact response capture.  Source-specific
connectors remain responsible for decoding pagination or source error
envelopes, and :class:`~dental_market.connectors.base.ConnectorRunner` remains
responsible for persisting those bytes before any such decoding happens.

Credentials are accepted separately from semantic query parameters.  They are
inserted only while constructing the outbound request and are never retained
on the transport, included in its representation, or copied into errors.
"""

from __future__ import annotations

import http.client
import re
import socket
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Optional, Protocol, Sequence, Tuple

from .. import __version__
from ..security import SensitiveDataError, reject_sensitive_data
from .base import ConnectorError, RawResponse


# Keep this deny-by-default list aligned with the durable raw collection
# envelope.  Unknown headers are intentionally discarded; response bodies are
# the exact source of truth.
SAFE_RESPONSE_HEADERS = frozenset(
    {
        "cache-control",
        "content-encoding",
        "content-language",
        "content-length",
        "content-range",
        "content-type",
        "date",
        "etag",
        "expires",
        "last-modified",
        "retry-after",
        "vary",
        "x-correlation-id",
        "x-page",
        "x-page-count",
        "x-per-page",
        "x-rate-limit-limit",
        "x-rate-limit-remaining",
        "x-rate-limit-reset",
        "x-ratelimit-limit",
        "x-ratelimit-remaining",
        "x-ratelimit-reset",
        "x-request-id",
        "x-total-count",
    }
)

_QUERY_NAME = re.compile(r"^(?:[A-Za-z][A-Za-z0-9_.-]*|_type)$")
_LOCALDATA_CONDITION_NAME = re.compile(
    # Exact operator allowlist from the current LOCALDATA replacement APIs.
    # LIKE is used for institution-type selection and LT closes an incremental
    # DAT_UPDT_PNT window.  Arbitrary bracket syntax remains forbidden.
    r"^cond\[([A-Z][A-Z0-9_]*)::(EQ|GTE|LIKE|LT)\]$"
)
_DEFAULT_ACCEPT = "application/json, application/xml;q=0.9, */*;q=0.1"
_DEFAULT_USER_AGENT = "dental-market-analysis/%s" % __version__


class _Openable(Protocol):
    def open(
        self,
        fullurl: urllib.request.Request,
        data: Optional[bytes] = None,
        timeout: float = socket._GLOBAL_DEFAULT_TIMEOUT,
    ) -> Any:
        ...


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Turn redirects into ordinary HTTP responses instead of following them."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Mapping[str, str],
        newurl: str,
    ) -> None:
        del req, fp, code, msg, headers, newurl
        return None


@dataclass(frozen=True)
class SecretQueryParam:
    """One transient credential query parameter with a redacted representation."""

    name: str
    value: str = field(repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or _QUERY_NAME.fullmatch(self.name) is None:
            raise ValueError("secret query parameter name is invalid")
        if not isinstance(self.value, str) or not self.value:
            raise ValueError("secret query parameter value must be non-empty")


class StdlibHttpGetTransport:
    """Fetch exact bytes from one allowlisted HTTPS host and set of paths.

    ``allowed_paths`` uses exact matches, not prefixes.  This prevents a source
    connector from turning an approved API origin into a generic network
    client.  Endpoint URLs must not carry a query string: semantic parameters
    and transient credential parameters are supplied separately to ``get``.
    """

    def __init__(
        self,
        allowed_host: str,
        allowed_paths: Sequence[str],
        *,
        timeout_seconds: float = 30.0,
        max_body_bytes: int = 10 * 1024 * 1024,
        opener: Optional[_Openable] = None,
    ) -> None:
        normalized_host = self._normalize_allowed_host(allowed_host)
        normalized_paths = self._normalize_allowed_paths(allowed_paths)
        if isinstance(timeout_seconds, bool) or not isinstance(
            timeout_seconds, (int, float)
        ):
            raise ValueError("timeout_seconds must be a positive number")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be a positive number")
        if isinstance(max_body_bytes, bool) or not isinstance(max_body_bytes, int):
            raise ValueError("max_body_bytes must be a positive integer")
        if max_body_bytes < 1:
            raise ValueError("max_body_bytes must be a positive integer")

        self.allowed_host = normalized_host
        self.allowed_paths = normalized_paths
        self.timeout_seconds = float(timeout_seconds)
        self.max_body_bytes = max_body_bytes
        self._opener = opener or urllib.request.build_opener(_NoRedirectHandler())

    def get(
        self,
        endpoint: str,
        *,
        params: Optional[Mapping[str, object]] = None,
        secret_params: Iterable[SecretQueryParam] = (),
    ) -> RawResponse:
        """Perform one bounded HTTPS GET and return an unparsed response.

        An HTTP status response, including 4xx/5xx represented by
        :class:`urllib.error.HTTPError`, is returned as ``RawResponse`` so the
        runner can persist its exact body before failing or parsing it.  A
        transport failure has no response body and becomes a normalized
        ``ConnectorError`` without the URL or underlying exception text.
        """

        base_url = self._validate_endpoint(endpoint)
        public_query = self._public_query_pairs(params or {})
        transient_secrets = self._secret_query_pairs(secret_params, public_query)
        secret_values = tuple(value for _, value in transient_secrets)

        request: Optional[urllib.request.Request] = None
        request_build_failed = False
        try:
            # This is intentionally the only point where secret values enter a
            # URL.  Construction errors are normalized after leaving this
            # block because Unicode exceptions retain their original string.
            query = urllib.parse.urlencode(
                public_query + transient_secrets, doseq=False
            )
            outbound_url = base_url + ("?" + query if query else "")
            request = urllib.request.Request(
                outbound_url,
                headers={
                    "Accept": _DEFAULT_ACCEPT,
                    "User-Agent": _DEFAULT_USER_AGENT,
                },
                method="GET",
            )
        except (TypeError, ValueError, UnicodeError):
            request_build_failed = True
        if request_build_failed or request is None:
            raise ConnectorError(
                "HTTPS request could not be constructed safely",
                retryable=False,
            ) from None

        deferred_error: Optional[Tuple[str, Optional[int], bool]] = None
        response: Any = None
        try:
            response = self._opener.open(request, timeout=self.timeout_seconds)
        except urllib.error.HTTPError as exc:
            try:
                captured = self._response_from_stream(
                    exc, int(exc.code), secret_values=secret_values
                )
            except ConnectorError as capture_error:
                deferred_error = (
                    str(capture_error),
                    capture_error.status_code,
                    bool(capture_error.retryable),
                )
                captured = None
            except Exception:
                deferred_error = (
                    "HTTPS response could not be captured safely",
                    None,
                    False,
                )
                captured = None
            finally:
                self._close_quietly(exc)
            if captured is not None:
                return captured
        except (
            urllib.error.URLError,
            TimeoutError,
            socket.timeout,
            ssl.SSLError,
            OSError,
            ValueError,
            http.client.HTTPException,
        ):
            deferred_error = (
                "HTTPS request failed before a response was received",
                None,
                True,
            )

        # Raise only after leaving the urllib exception handler.  Otherwise the
        # original HTTPError/URLError (which retains the credential-bearing
        # request URL) would remain reachable through ``__context__``.
        if deferred_error is not None:
            message, status_code, retryable = deferred_error
            raise ConnectorError(
                message,
                status_code=status_code,
                retryable=retryable,
            ) from None

        captured = None
        capture_error: Optional[Tuple[str, Optional[int], bool]] = None
        try:
            status_code = getattr(response, "status", None)
            if status_code is None:
                status_code = response.getcode()
            captured = self._response_from_stream(
                response, int(status_code), secret_values=secret_values
            )
        except ConnectorError as exc:
            capture_error = (
                str(exc),
                exc.status_code,
                bool(exc.retryable),
            )
        except (TypeError, ValueError, OSError):
            capture_error = (
                "HTTPS response could not be captured safely",
                None,
                False,
            )
        finally:
            self._close_quietly(response)
        if capture_error is not None:
            message, status_code, retryable = capture_error
            raise ConnectorError(
                message,
                status_code=status_code,
                retryable=retryable,
            ) from None
        if captured is None:  # defensive; every capture path returns or errors
            raise ConnectorError(
                "HTTPS response could not be captured safely",
                retryable=False,
            )
        return captured

    @staticmethod
    def _normalize_allowed_host(allowed_host: str) -> str:
        if not isinstance(allowed_host, str) or not allowed_host.strip():
            raise ValueError("allowed_host must be non-empty")
        candidate = allowed_host.strip().lower().rstrip(".")
        parsed = urllib.parse.urlsplit("//" + candidate)
        if (
            parsed.hostname != candidate
            or parsed.username is not None
            or parsed.password is not None
            or parsed.port is not None
            or parsed.path
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("allowed_host must be a bare DNS host")
        return candidate

    @staticmethod
    def _normalize_allowed_paths(allowed_paths: Sequence[str]) -> Tuple[str, ...]:
        if isinstance(allowed_paths, (str, bytes)) or not allowed_paths:
            raise ValueError("allowed_paths must contain at least one exact path")
        normalized = []
        for path in allowed_paths:
            if (
                not isinstance(path, str)
                or not path.startswith("/")
                or "?" in path
                or "#" in path
                or "\\" in path
                or "//" in path
                or any(part in {".", ".."} for part in path.split("/"))
            ):
                raise ValueError("allowed_paths entries must be exact absolute paths")
            normalized.append(path)
        if len(set(normalized)) != len(normalized):
            raise ValueError("allowed_paths entries must be unique")
        return tuple(sorted(normalized))

    def _validate_endpoint(self, endpoint: str) -> str:
        if not isinstance(endpoint, str) or not endpoint:
            raise ValueError("endpoint must be a non-empty HTTPS URL")
        parsed = urllib.parse.urlsplit(endpoint)
        try:
            port = parsed.port
        except ValueError:
            raise ValueError("endpoint contains an invalid port") from None
        host = parsed.hostname.lower().rstrip(".") if parsed.hostname else None
        if (
            parsed.scheme.lower() != "https"
            or host != self.allowed_host
            or port not in (None, 443)
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in self.allowed_paths
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("endpoint is outside the HTTPS host/path allowlist")
        # Rebuild from validated, non-secret components so caller spelling does
        # not accidentally carry userinfo, a query, or a fragment forward.
        return urllib.parse.urlunsplit(("https", self.allowed_host, parsed.path, "", ""))

    @staticmethod
    def _public_query_pairs(params: Mapping[str, object]) -> list[Tuple[str, str]]:
        if not isinstance(params, Mapping):
            raise TypeError("params must be a mapping")
        try:
            reject_sensitive_data(params, location="HTTP public query params")
        except SensitiveDataError:
            raise SensitiveDataError(
                "credentials must be supplied through secret_params"
            ) from None

        pairs = []
        for name, value in sorted(params.items(), key=lambda item: str(item[0])):
            if not isinstance(name, str) or not StdlibHttpGetTransport._is_query_name(
                name
            ):
                raise ValueError("public query parameter name is invalid")
            if value is None or isinstance(value, (Mapping, list, tuple, set, bytes)):
                raise ValueError("public query parameters must be scalar values")
            if isinstance(value, bool):
                encoded_value = "true" if value else "false"
            elif isinstance(value, (str, int, float)):
                encoded_value = str(value)
            else:
                raise ValueError("public query parameters must be scalar values")
            reject_sensitive_data(
                encoded_value, location="HTTP public query parameter value"
            )
            pairs.append((name, encoded_value))
        return pairs

    @staticmethod
    def _is_query_name(name: str) -> bool:
        if _QUERY_NAME.fullmatch(name) is not None:
            return True
        condition = _LOCALDATA_CONDITION_NAME.fullmatch(name)
        if condition is None:
            return False
        # LOCALDATA's official condition grammar needs brackets and ``::``.
        # Only the explicitly contracted operators and an uppercase field
        # identifier are accepted; a credential-shaped field is still banned.
        try:
            reject_sensitive_data(
                {condition.group(1): "condition"},
                location="LOCALDATA condition field",
            )
        except SensitiveDataError:
            return False
        return True

    @staticmethod
    def _secret_query_pairs(
        secret_params: Iterable[SecretQueryParam],
        public_query: Sequence[Tuple[str, str]],
    ) -> list[Tuple[str, str]]:
        public_names = {name for name, _ in public_query}
        seen = set()
        pairs = []
        for item in secret_params:
            if not isinstance(item, SecretQueryParam):
                raise TypeError("secret_params must contain SecretQueryParam values")
            if item.name in public_names or item.name in seen:
                raise ValueError("query parameter names must be unique")
            seen.add(item.name)
            pairs.append((item.name, item.value))
        return pairs

    def _response_from_stream(
        self,
        stream: Any,
        status_code: int,
        *,
        secret_values: Sequence[str],
    ) -> RawResponse:
        body = self._read_bounded(stream)
        if self._contains_credential_echo(body, secret_values):
            raise ConnectorError(
                "HTTPS response echoed credential material; raw capture refused",
                retryable=False,
            )
        headers = self._safe_headers(
            getattr(stream, "headers", None), secret_values=secret_values
        )
        content_type = headers.get("content-type", "application/octet-stream")
        return RawResponse(
            body=body,
            content_type=content_type,
            status_code=status_code,
            response_headers=headers,
        )

    def _read_bounded(self, stream: Any) -> bytes:
        chunks = []
        captured = 0
        while captured <= self.max_body_bytes:
            remaining = self.max_body_bytes + 1 - captured
            chunk = stream.read(remaining)
            if not chunk:
                break
            if not isinstance(chunk, bytes):
                raise ConnectorError(
                    "HTTPS response body was not bytes", retryable=False
                )
            chunks.append(chunk)
            captured += len(chunk)
        body = b"".join(chunks)
        if len(body) > self.max_body_bytes:
            raise ConnectorError(
                "HTTPS response body exceeds the configured byte limit",
                retryable=False,
            )
        return body

    @staticmethod
    def _contains_credential_echo(body: bytes, secret_values: Sequence[str]) -> bool:
        for value in secret_values:
            candidates = {
                value.encode("utf-8"),
                urllib.parse.quote(value, safe="").encode("ascii"),
                urllib.parse.quote_plus(value, safe="").encode("ascii"),
            }
            if any(candidate and candidate in body for candidate in candidates):
                return True
        return False

    @staticmethod
    def _close_quietly(stream: Any) -> None:
        try:
            stream.close()
        except Exception:
            # Capture has already succeeded or produced a normalized error.
            # A close failure must not replace it with an exception that might
            # retain the outbound request URL.
            pass

    @staticmethod
    def _safe_headers(
        headers: Any, *, secret_values: Sequence[str]
    ) -> dict[str, str]:
        if headers is None:
            return {}
        try:
            entries = headers.items()
        except AttributeError:
            raise ConnectorError(
                "HTTPS response headers could not be captured safely",
                retryable=False,
            ) from None
        safe = {}
        for raw_name, raw_value in entries:
            name = str(raw_name).strip().lower()
            if name not in SAFE_RESPONSE_HEADERS:
                continue
            value = str(raw_value).strip()
            if StdlibHttpGetTransport._contains_credential_echo(
                value.encode("utf-8"), secret_values
            ):
                raise ConnectorError(
                    "HTTPS response header echoed credential material",
                    retryable=False,
                )
            try:
                reject_sensitive_data(value, location="HTTP response header")
            except SensitiveDataError:
                raise ConnectorError(
                    "HTTPS response header failed the safety policy",
                    retryable=False,
                ) from None
            safe[name] = value
        return dict(sorted(safe.items()))


__all__ = [
    "SAFE_RESPONSE_HEADERS",
    "SecretQueryParam",
    "StdlibHttpGetTransport",
]
