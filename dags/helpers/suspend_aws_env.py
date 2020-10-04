import os
from traceback import TracebackException
from types import TracebackType


class SuspendAwsEnvVar:
    def __init__(
        self,
        aws_access_key_id_env_var: str = "AWS_ACCESS_KEY_ID",
        aws_secret_access_key_env_var: str = "AWS_SECRET_ACCESS_KEY",
    ) -> None:
        self.aws_access_key_id_env_var = aws_access_key_id_env_var
        self.aws_secret_access_key_env_var = aws_secret_access_key_env_var

    def __enter__(self) -> None:
        try:
            self.aws_access_key_id = os.environ[self.aws_access_key_id_env_var]
            del os.environ[self.aws_access_key_id_env_var]
        except KeyError:
            pass

        try:
            self.aws_secret_access_key = os.environ[self.aws_secret_access_key_env_var]
            del os.environ[self.aws_secret_access_key_env_var]
        except KeyError:
            pass

    def __exit__(
        self, exc_type: Exception, exc_val: TracebackException, exc_tb: TracebackType
    ) -> None:
        if hasattr(self, "aws_access_key_id"):
            os.environ[self.aws_access_key_id_env_var] = self.aws_access_key_id

        if hasattr(self, "aws_secret_access_key"):
            os.environ[self.aws_secret_access_key_env_var] = self.aws_secret_access_key
