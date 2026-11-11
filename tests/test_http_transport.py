from __future__ import annotations

import io
import urllib.error
import urllib.parse
import urllib.request
from email.message import Message
from typing import Optional

import pytest

from dental_market.connectors.base import ConnectorError
from dental_market.connectors.http import (
    SAFE_RESPONSE_HEADERS,
    SecretQueryParam,
    StdlibHttpGetTransport,
    _NoRedirectHandler,
)
from dental_market.security import SensitiveDataError


HOST = "apis.data.go.kr"
PATH = "/B551182/hospInfoServicev2/getHospBasisList"
ENDPOINT = "https://" + HOST + PATH
SECRET = "TOPSECRET+/=value"


def _headers(**values: str) -> Message:
    headers = Message()
    for name, value in values.items():
        headers.add_header(name.replace("_", "-"), value)
    return headers


class FakeResponse:
    def __init__(
        self,
        body: bytes,
        *,
        status: int = 200,
        headers: Optional[Message] = None,
        short_reads: bool = False,
    ) -> None:
        self.status = status
        self.headers = headers or Message()
        self._body = io.BytesIO(body)
        self._short_reads = short_reads
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        if self._short_reads and size > 1:
            size = min(size, 2)
        return self._body.read(size)

    def getcode(self) -> int:
        return self.status

    def close(self) -> None:
        self.closed = True


class FakeOpener:
    def __init__(self, result: object) -> None:
        self.result = result
        self.requests = []
        self.timeouts = []

    def open(self, request, data=None, timeout=None):
        assert data is None
        self.requests.append(request)
        self.timeouts.append(timeout)
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


def _transport(opener: FakeOpener, **kwargs) -> StdlibHttpGetTransport:
    return StdlibHttpGetTransport(
        HOST,
        [PATH],
        opener=opener,
        timeout_seconds=kwargs.pop("timeout_seconds", 2.5),
        max_body_bytes=kwargs.pop("max_body_bytes", 1024),
        **kwargs,
    )


def test_exact_body_secret_last_moment_and_redacted_representations() -> None:
    body = b"\x00raw\xffresponse\n"
    response = FakeResponse(
        body,
        headers=_headers(
            content_type="application/json; charset=utf-8",
            etag='"snapshot-1"',
            server="must-not-persist",
            set_cookie="session=must-not-persist",
        ),
        short_reads=True,
    )
    opener = FakeOpener(response)
    transport = _transport(opener)
    secret = SecretQueryParam("serviceKey", SECRET)

    raw = transport.get(
        ENDPOINT,
        params={"pageNo": 1, "numOfRows": 10, "json": True},
        secret_params=[secret],
    )

    assert raw.body == body
    assert raw.status_code == 200
    assert raw.content_type == "application/json; charset=utf-8"
    assert raw.response_headers == {
        "content-type": "application/json; charset=utf-8",
        "etag": '"snapshot-1"',
    }
    assert response.closed is True
    assert opener.timeouts == [2.5]
    assert opener.requests[0].get_header("User-agent") == (
        "dental-market-analysis/0.1.0"
    )

    outbound = opener.requests[0].full_url
    parsed_query = urllib.parse.parse_qs(urllib.parse.urlsplit(outbound).query)
    assert parsed_query == {
        "json": ["true"],
        "numOfRows": ["10"],
        "pageNo": ["1"],
        "serviceKey": [SECRET],
    }
    assert SECRET not in repr(secret)
    assert SECRET not in repr(transport)
    assert SECRET not in repr(raw)


def test_http_error_is_returned_as_exact_raw_response_without_error_text() -> None:
    body = b"<?xml version='1.0'?><error>unauthorized</error>\x00"
    secret_url = ENDPOINT + "?serviceKey=" + urllib.parse.quote(SECRET)
    error = urllib.error.HTTPError(
        secret_url,
        401,
        "unauthorized " + SECRET,
        _headers(
            content_type="application/xml",
            retry_after="3",
            x_request_id="request-1",
            x_powered_by="discard-me",
        ),
        io.BytesIO(body),
    )
    opener = FakeOpener(error)

    raw = _transport(opener).get(
        ENDPOINT, secret_params=[SecretQueryParam("serviceKey", SECRET)]
    )

    assert raw.body == body
    assert raw.status_code == 401
    assert raw.content_type == "application/xml"
    assert raw.response_headers == {
        "content-type": "application/xml",
        "retry-after": "3",
        "x-request-id": "request-1",
    }
    assert SECRET not in repr(raw)


def test_transport_failure_is_normalized_without_url_reason_or_secret() -> None:
    failure = urllib.error.URLError(
        "serviceKey=%s failed at %s" % (SECRET, ENDPOINT)
    )
    opener = FakeOpener(failure)

    with pytest.raises(ConnectorError) as caught:
        _transport(opener).get(
            ENDPOINT, secret_params=[SecretQueryParam("serviceKey", SECRET)]
        )

    assert caught.value.retryable is True
    assert SECRET not in str(caught.value)
    assert ENDPOINT not in str(caught.value)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def test_secret_encoding_failure_does_not_leave_secret_in_exception_context() -> None:
    malformed_secret = "private-\udcff-value"
    opener = FakeOpener(FakeResponse(b"unused"))

    with pytest.raises(ConnectorError, match="constructed safely") as caught:
        _transport(opener).get(
            ENDPOINT,
            secret_params=[SecretQueryParam("serviceKey", malformed_secret)],
        )

    assert "private" not in str(caught.value)
    assert caught.value.__context__ is None
    assert opener.requests == []


def test_response_body_limit_fails_without_returning_partial_bytes() -> None:
    response = FakeResponse(b"123456", short_reads=True)
    opener = FakeOpener(response)

    with pytest.raises(ConnectorError, match="byte limit") as caught:
        _transport(opener, max_body_bytes=5).get(
            ENDPOINT, secret_params=[SecretQueryParam("serviceKey", SECRET)]
        )

    assert SECRET not in str(caught.value)
    assert response.closed is True


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://apis.data.go.kr" + PATH,
        "https://example.com" + PATH,
        "https://apis.data.go.kr/other",
        ENDPOINT + "?pageNo=1",
        ENDPOINT + "#fragment",
        "https://user@apis.data.go.kr" + PATH,
        "https://apis.data.go.kr:444" + PATH,
    ],
)
def test_endpoint_must_match_exact_https_host_and_path(endpoint: str) -> None:
    opener = FakeOpener(FakeResponse(b"ok"))

    with pytest.raises(ValueError, match="allowlist"):
        _transport(opener).get(endpoint)

    assert opener.requests == []


@pytest.mark.parametrize(
    "host,paths",
    [
        ("https://apis.data.go.kr", [PATH]),
        ("apis.data.go.kr:443", [PATH]),
        (HOST, []),
        (HOST, PATH),
        (HOST, ["relative"]),
        (HOST, ["/allowed/../other"]),
        (HOST, [PATH, PATH]),
    ],
)
def test_transport_allowlist_configuration_is_strict(host, paths) -> None:
    with pytest.raises(ValueError):
        StdlibHttpGetTransport(host, paths)


@pytest.mark.parametrize(
    "credential_name", ["serviceKey", "confmKey", "consumer_key"]
)
def test_public_params_cannot_carry_credentials(credential_name: str) -> None:
    opener = FakeOpener(FakeResponse(b"ok"))

    with pytest.raises(SensitiveDataError, match="secret_params") as caught:
        _transport(opener).get(ENDPOINT, params={credential_name: SECRET})

    assert SECRET not in str(caught.value)
    assert opener.requests == []


def test_localdata_condition_query_names_allow_only_contracted_grammar() -> None:
    opener = FakeOpener(FakeResponse(b"ok"))

    _transport(opener).get(
        ENDPOINT,
        params={
            "cond[DAT_UPDT_PNT::GTE]": "20260901000000",
            "cond[DAT_UPDT_PNT::LT]": "20260902000000",
            "cond[MDLCR_INST_BTP_NM::LIKE]": "치과",
            "cond[OPN_ATMY_GRP_CD::EQ]": "21",
        },
    )

    parsed_query = urllib.parse.parse_qs(
        urllib.parse.urlsplit(opener.requests[0].full_url).query
    )
    assert parsed_query == {
        "cond[DAT_UPDT_PNT::GTE]": ["20260901000000"],
        "cond[DAT_UPDT_PNT::LT]": ["20260902000000"],
        "cond[MDLCR_INST_BTP_NM::LIKE]": ["치과"],
        "cond[OPN_ATMY_GRP_CD::EQ]": ["21"],
    }


def test_official_hira_type_query_name_is_allowed() -> None:
    opener = FakeOpener(FakeResponse(b"ok"))

    raw = _transport(opener).get(ENDPOINT, params={"_type": "json"})

    assert raw.status_code == 200
    query = urllib.parse.parse_qs(
        urllib.parse.urlsplit(opener.requests[0].full_url).query
    )
    assert query == {"_type": ["json"]}


@pytest.mark.parametrize(
    "name",
    [
        "cond[FIELD::DROP]",
        "cond[field::EQ]",
        "cond[FIELD::EQ]extra",
        "cond[FIELD::EQ][OTHER::EQ]",
        "cond[FIELD::EQ]\r\nInjected",
        "cond[SERVICE_KEY::EQ]",
    ],
)
def test_localdata_condition_query_names_reject_other_bracket_syntax(name: str) -> None:
    opener = FakeOpener(FakeResponse(b"ok"))

    with pytest.raises(ValueError):
        _transport(opener).get(ENDPOINT, params={name: "value"})

    assert opener.requests == []


def test_duplicate_public_and_secret_query_names_are_rejected() -> None:
    opener = FakeOpener(FakeResponse(b"ok"))

    with pytest.raises(ValueError, match="unique"):
        _transport(opener).get(
            ENDPOINT,
            params={"pageNo": 1},
            secret_params=[SecretQueryParam("pageNo", SECRET)],
        )

    assert opener.requests == []


def test_only_durable_allowlisted_response_headers_are_kept() -> None:
    header_values = {
        name.replace("-", "_"): "safe-value" for name in SAFE_RESPONSE_HEADERS
    }
    header_values["authorization"] = "Bearer must-be-dropped"
    header_values["unknown"] = "must-be-dropped"
    response = FakeResponse(b"ok", headers=_headers(**header_values))

    raw = _transport(FakeOpener(response)).get(ENDPOINT)

    assert set(raw.response_headers) == SAFE_RESPONSE_HEADERS
    assert "authorization" not in raw.response_headers
    assert "unknown" not in raw.response_headers


def test_sensitive_value_in_an_allowlisted_header_fails_closed() -> None:
    response = FakeResponse(
        b"ok", headers=_headers(x_request_id="token=must-not-persist")
    )

    with pytest.raises(ConnectorError, match="safety policy") as caught:
        _transport(FakeOpener(response)).get(ENDPOINT)

    assert "must-not-persist" not in str(caught.value)


def test_exact_credential_in_allowlisted_header_is_refused_without_exposure() -> None:
    response = FakeResponse(b"ok", headers=_headers(x_request_id=SECRET))

    with pytest.raises(ConnectorError, match="credential material") as caught:
        _transport(FakeOpener(response)).get(
            ENDPOINT, secret_params=[SecretQueryParam("serviceKey", SECRET)]
        )

    assert SECRET not in str(caught.value)
    assert caught.value.__context__ is None


@pytest.mark.parametrize("encoded", [False, True])
def test_credential_echo_in_response_body_is_refused_without_exposure(
    encoded: bool,
) -> None:
    echoed = urllib.parse.quote_plus(SECRET).encode("ascii") if encoded else SECRET.encode()
    secret_url = ENDPOINT + "?serviceKey=" + urllib.parse.quote(SECRET)
    error = urllib.error.HTTPError(
        secret_url,
        401,
        "unauthorized " + SECRET,
        _headers(content_type="application/xml"),
        io.BytesIO(b"<error>" + echoed + b"</error>"),
    )

    with pytest.raises(ConnectorError, match="credential material") as caught:
        _transport(FakeOpener(error)).get(
            ENDPOINT, secret_params=[SecretQueryParam("serviceKey", SECRET)]
        )

    assert SECRET not in str(caught.value)
    assert caught.value.__context__ is None


def test_default_redirect_handler_never_builds_a_followup_request() -> None:
    handler = _NoRedirectHandler()
    request = urllib.request.Request(ENDPOINT)

    redirected = handler.redirect_request(
        request,
        io.BytesIO(b"redirect"),
        302,
        "Found",
        {},
        "https://attacker.invalid/steal",
    )

    assert redirected is None
