import os

from _pytest.monkeypatch import MonkeyPatch

from dags.helpers.suspend_aws_env import SuspendAwsEnvVar


def test_aws_key_id_not_in_environ(monkeypatch: MonkeyPatch):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)

    with SuspendAwsEnvVar():
        assert "AWS_ACCESS_KEY_ID" not in os.environ

    assert "AWS_ACCESS_KEY_ID" not in os.environ


def test_aws_secret_key_not_in_environ(monkeypatch: MonkeyPatch):
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

    with SuspendAwsEnvVar():
        assert "AWS_SECRET_ACCESS_KEY" not in os.environ

    assert "AWS_SECRET_ACCESS_KEY" not in os.environ


def test_environ_has_aws_access_key(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "abc")

    with SuspendAwsEnvVar():
        assert "AWS_ACCESS_KEY_ID" not in os.environ

    assert os.environ["AWS_ACCESS_KEY_ID"] == "abc"


def test_environ_has_aws_secret_key(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "def")

    with SuspendAwsEnvVar():
        assert "AWS_SECRET_ACCESS_KEY" not in os.environ

    assert os.environ["AWS_SECRET_ACCESS_KEY"] == "def"
