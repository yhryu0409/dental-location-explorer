import json

from dental_market import cli
from dental_market.connectors import ConnectorError


def test_invalid_page_size_returns_structured_usage_error(capsys) -> None:
    exit_code = cli.main(["sample-run", "--page-size", "0"])

    assert exit_code == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["status"] == "failed"
    assert payload["error_type"] == "CliUsageError"
    assert "greater than zero" in payload["error"]


def test_connector_error_is_structured_and_redacts_common_secret_fields(
    monkeypatch, capsys, tmp_path
) -> None:
    def fail_sample(*_args, **_kwargs):
        raise ConnectorError(
            "upstream failed serviceKey=do-not-print&dataset=clinics"
        )

    monkeypatch.setattr(cli, "run_sample", fail_sample)
    exit_code = cli.main(
        ["--data-root", str(tmp_path / "runtime"), "sample-run"]
    )

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["error_type"] == "ConnectorError"
    assert "do-not-print" not in payload["error"]
    assert "[REDACTED]" in payload["error"]


def test_authorization_bearer_value_is_fully_redacted(
    monkeypatch, capsys, tmp_path
) -> None:
    def fail_sample(*_args, **_kwargs):
        raise ConnectorError("Authorization: Bearer TOPSECRET123 request failed")

    monkeypatch.setattr(cli, "run_sample", fail_sample)
    exit_code = cli.main(
        ["--data-root", str(tmp_path / "runtime"), "sample-run"]
    )

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().err)
    assert "TOPSECRET123" not in payload["error"]
    assert payload["error"].startswith("Authorization=[REDACTED]")
