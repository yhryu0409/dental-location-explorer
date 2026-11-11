import pytest

from dental_market.security import (
    SensitiveDataError,
    redact_sensitive,
    reject_sensitive_data,
)


def test_redacts_common_secret_shapes() -> None:
    message = (
        "Authorization: Bearer abc123 serviceKey=query-secret&x=1 "
        "api_key%3Dencoded-secret "
        "{\"access_token\":\"json-secret\"}"
    )

    redacted = redact_sensitive(message)

    for secret in ("abc123", "query-secret", "encoded-secret", "json-secret"):
        assert secret not in redacted
    assert "[REDACTED]" in redacted


def test_redacts_json_basic_authorization_value() -> None:
    redacted = redact_sensitive(
        'headers={"Authorization":"Basic dXNlcjpwYXNz"}'
    )
    assert "dXNlcjpwYXNz" not in redacted
    assert '"Authorization":"[REDACTED]"' in redacted


def test_redacts_arbitrary_authorization_scheme() -> None:
    redacted = redact_sensitive("Authorization = ApiKey TOPSECRET")
    assert "TOPSECRET" not in redacted
    assert redacted == "Authorization=[REDACTED]"


def test_redacts_multipart_authorization_and_json_api_key_headers() -> None:
    message = (
        'Authorization: AWS4-HMAC-SHA256 Credential=ABC,Signature=TOPSECRET\n'
        'Digest Authorization=response="RESPSECRET"\n'
        '{"X-Api-Key":"JSONHEADERSECRET"}'
    )
    redacted = redact_sensitive(message)
    for secret in ("TOPSECRET", "RESPSECRET", "JSONHEADERSECRET"):
        assert secret not in redacted
    assert redacted.count("[REDACTED]") == 3


def test_redacts_prefixed_env_and_oauth_secret_names() -> None:
    message = (
        "HIRA_API_KEY=HSECRET SGIS_CONSUMER_SECRET=SSECRET "
        "client_secret=CSECRET refresh_token=RSECRET "
        "confmKey=JSECRET consumer_key=SGISID"
    )
    redacted = redact_sensitive(message)
    for secret in (
        "HSECRET",
        "SSECRET",
        "CSECRET",
        "RSECRET",
        "JSECRET",
        "SGISID",
    ):
        assert secret not in redacted
    assert redacted.count("[REDACTED]") == 6


def test_nested_durable_metadata_with_sensitive_keys_is_rejected() -> None:
    with pytest.raises(SensitiveDataError):
        reject_sensitive_data(
            {"nested": [{"HIRA_API_KEY": "TOPSECRET"}]},
            location="test metadata",
        )


@pytest.mark.parametrize("name", ["confmKey", "consumer_key"])
def test_provider_specific_credential_fields_are_rejected(name: str) -> None:
    with pytest.raises(SensitiveDataError, match="sensitive field"):
        reject_sensitive_data({name: "TOPSECRET"}, location="test metadata")


def test_credential_pattern_embedded_in_a_metadata_key_is_rejected() -> None:
    with pytest.raises(SensitiveDataError, match="sensitive field"):
        reject_sensitive_data(
            {"note-client_secret=KEYLEAK": "safe-looking-value"},
            location="test metadata",
        )
