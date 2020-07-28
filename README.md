### Installing Airflow locally

First, ensure that `virtualenv` is installed and enter a 
virtual environment:

```bash
pip install virtualenv
virtualenv venv
source venv/bin/activate
```

And then install Airflow and its dependencies:

```bash
pip install -r requirements.txt
```

You should then have all the librairies required to run Airflow
jobs in your local development environment.

### Testing a PythonOperator task

When creating a DAG file, you may want to test Python code in a
`PythonOperator`. This can be problematic because the Airflow
production deployment injects credentials for external services
at runtime through `Hook`s.

One avenue to run these tasks locally is to call the PythonOperator's
`callable` from the script's `main` block, and patch Airflow `Hook`s
to provide credentials from environment variables:

This is an example from the `dags/talkdesk_import.py` script:

```python
if __name__ == "__main__":
    import os
    from snowflake.sqlalchemy import URL
    from unittest.mock import patch, MagicMock

    mock = MagicMock()
    mock.login = os.environ.get("CLIENT_ID")
    mock.password = os.environ.get("CLIENT_SECRET")
    mock.host = os.environ.get("TOKEN_ENDPOINT")
    mock.extra_dejson = {
        "token_endpoint": os.environ.get("TOKEN_ENDPOINT"),
    }

    account = os.environ.get("SNOWFLAKE_ACCOUNT", "thinkingcapital.ca-central-1.aws")
    database = os.environ.get("SNOWFLAKE_DATABASE", "TCLEGACY")
    role = os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN")
    user = os.environ.get("SNOWFLAKE_USER")

    url = (
        URL(account=account, database=database, role=role, user=user)
        if user
        else URL(account=account, database=database, role=role)
    )

    with patch(
        "dags.<your_dag>.SnowflakeHook.get_sqlalchemy_engine",
        return_value=create_engine(
            url, connect_args={"authenticator": "externalbrowser",},
        ),
    ) as mock_engine:
        your_callable(args)
```

This will replace the return value of `SnowflakeHook.get_sqlalchemy_engine`
with the provided `return_value`.