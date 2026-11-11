from pathlib import Path

import pytest

from dental_market.config import ConfigurationError, Settings, require_secret


def test_settings_use_explicit_default_root_without_secrets(tmp_path: Path) -> None:
    settings = Settings.from_env({}, default_data_root=tmp_path / "runtime")

    assert settings.data_root == (tmp_path / "runtime").resolve()
    assert settings.environment == "development"
    assert settings.max_retries == 4


def test_environment_overrides_are_validated(tmp_path: Path) -> None:
    settings = Settings.from_env(
        {
            "DENTAL_DATA_ROOT": str(tmp_path / "external-data"),
            "DENTAL_ENV": "test",
            "DENTAL_LOG_LEVEL": "debug",
            "DENTAL_MAX_RETRIES": "2",
            "DENTAL_RATE_LIMIT_PER_SECOND": "20",
        }
    )

    assert settings.data_root == (tmp_path / "external-data").resolve()
    assert settings.environment == "test"
    assert settings.log_level == "DEBUG"
    assert settings.max_retries == 2
    assert settings.rate_limit_per_second == 20.0


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("DENTAL_MAX_RETRIES", "0"),
        ("DENTAL_HTTP_TIMEOUT_SECONDS", "not-a-number"),
        ("DENTAL_LOG_LEVEL", "verbose"),
    ],
)
def test_invalid_environment_values_fail_closed(
    name: str, value: str, tmp_path: Path
) -> None:
    with pytest.raises(ConfigurationError):
        Settings.from_env(
            {name: value}, default_data_root=tmp_path / "runtime"
        )


def test_secret_is_loaded_only_when_requested() -> None:
    assert require_secret("EXAMPLE_API_KEY", {"EXAMPLE_API_KEY": "secret"}) == "secret"
    with pytest.raises(ConfigurationError, match="EXAMPLE_API_KEY"):
        require_secret("EXAMPLE_API_KEY", {})
