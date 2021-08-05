# Airflow On Kubernetes

This is our Airflow project where we orchestrate various data workflows and pipelines,
connecting multiple sources systems to our data warehouse (Snowflake).

## Getting Started

### Install packages

First, ensure that `virtualenv` is installed and enter a virtual environment:

```bash
pip install virtualenv
virtualenv venv
source venv/bin/activate
```

And then install Airflow and its dependencies:

```bash
pip install -r requirements.txt --use-deprecated legacy-resolver
```

Note that the `--use-deprecated legacy-resolver` argument is used to avoid installation errors as the new 
pip resolver (released 20.3) is not compatible with the current version of Apache Airflow used
([reference](https://airflow.apache.org/docs/apache-airflow/1.10.15/installation.html)).

You should then have all the libraries required to run Airflow jobs in your local development environment. 
If the airflow command is not getting recognized, add ```PATH=$PATH:~/.local/bin``` to your zshrc or bashrc file and 
reload it.

### Meta database setup

When you install Airflow for the first time, you will be set up with sqlite as the meta database and the 
SequentialExecutor. You can use this setup, but you will need to switch to a different database and an executor
if you want to run parallel tasks.

#### Install PostgreSQL

```bash
brew install postgresql
postgres -V
```

#### Create database and user

```bash
psql postgres
CREATE database airflow_db;
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow;
```

#### Update airflow configuration (i.e., airflow.cfg)

```buildoutcfg
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost:5432/airflow_db
executor = LocalExecutor
```

### Run the webserver and scheduler

#### Set dags folder (in airflow.cfg)

```buildoutcfg
dags_folder = /Users/xzhang/PycharmProjects/airflow-on-kubernetes/dags
```

#### Initialize database

```bash
airflow initdb
```

#### Run webserver and scheduler (in separate terminals)

```bash
airflow webserver
```

```bash
airflow scheduler
```

## Testing a PythonOperator task

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

### Importing Salesforce data

Here is an example of changes need to be made in `sfdc_bulk_load.py` for testing the DAG:

#### Connect to source
You need to add the `salesforce_conn` to your local airflow. To connect with Salesforce sandbox,
you'll also need to add `domain='test'` to your `Salesforce()` call.

#### Connect to destination
1. If you already have the `snowflake_conn` in your local airflow, you can keep using the connection.
2. If you don't have the `snowflake_conn`, you can change

```python
engine_ = SnowflakeHook(snowflake_conn).get_sqlalchemy_engine()
```

to

```python
engine_ = create_engine(
    URL(
        account="thinkingcapital.ca-central-1.aws",
        user="{your_username_usually_email_in_snowflake}",
        password="{your_password}",
        database="{destination_db}",
        warehouse="{destination_wh}",
        role="{your_role}",
    ),
    connect_args={
        "authenticator": "externalbrowser",
    },
)
```

(`database="{destination_db}"` is optional here because this DAG is using
`f'use database {database}'` in every query, database in `URL()` won't affect the destination DB)

For local tests, if you don't want to load data into production DBs,
you will need to change the `database` and `schema` in `op_kwargs` to the destination DB you want. For example:
```python
op_kwargs={
    "snowflake_conn": "",
    "salesforce_conn": "",
    "database": "{your_branch_db}"
    "schema": "{schema_in_your_db}",
}
```

### Variables and Pools:

Before testing, you should check if there are any airflow _Variables_ or _Pools_ being used in the DAG,
if so, you may need to add them to your local.


## Appendix

### Perform safety checks on packages 

Run the following to see if there are any security vulnerabilities in the packages installed.

```bash
safety check
```

You should see a table that lists out any security vulnerabilities. You can try to address the
issues by updating your packages. Sometimes this might not be possible due to dependency conflicts.
In this case, please update the CI/CD pipeline to ignore specific security vulnerabilities.

### Visualize package dependency tree

Run the following to see how the installed packages depend on each other.

```bash
pipdeptree
```

### Local setup tips

#### Airflow configuration file

The airflow configuration (i.e., airflow.cfg) file is usually located in `~/airflow` directory. This file contains 
important configurations for your local environment.

- Set `load_default_connections = False` to not load default connections to your local Airflow environment.

- Set `load_examples = False` to not load the example DAGs in your local Airflow environment.

#### Slack alerts

Instead of sending alerts to the production `#slack_data_alerts` channel, use the `#slack_data_alerts_test` channel.
This is achieved by using a different webhook_token in the Extra section when you add the `slack_data_alerts` 
connection. You can see the webhooks defined [here](https://api.slack.com/apps/A024EU67G58/incoming-webhooks?).

```text
{
    "webhook_token": "T0J256FV1/B0254NKRMG8/vhd4GRaEGIRzut5gF52Mqilh"
}
```

#### Adding variables from json

Instead of adding each Variable from production individually, you can download the `airflow-variables.json` file from
`tc-data-airflow-production/local_setup/airflow-variables.json` in the AWS DataOps S3 bucket ([link](https://s3.console.aws.amazon.com/s3/object/tc-data-airflow-production?region=ca-central-1&prefix=local_setup/airflow-variables.json)).

#### Testing tasks

When possible try to use the following command to test each task locally.

```python
airflow tasks test <dag_id> <task_id> <execution_date>
```

### Airflow Style Guide

The following are some guidelines to reference when writing your DAGs. Not every DAGs follows this guideline currently.

---

#### Explicitly pass DAG reference, following Python's design principle `Explicit is better than implicit`.

```python
dag = DAG(...)
t1 = DummyOperator(task_id="task1", dag=dag)
t2 = DummyOperator(task_id="task2", dag=dag)
```

vs.

```python
with DAG(...) as dag:
    t1 = DummyOperator(task_id="task1")
    t2 = DummyOperator(task_id="task2")
```

---

#### Be explicit in the context variable you are passing.

```python
def _print_exec_date(execution_date, **_):
    print(execution_date)
```

vs.

```python
def _print_exec_date(execution_date, **context):
    print(execution_date)
```

---

#### Readability counts, hence it is better to not mix directions in a single statement. Also, use downstream direction whenever possible since it is the natural way of reading from left to right for most people.

```python
task1 >> [task2, task3] >> task4
[task5, task6] >> task4
```

vs.

```python
task1 >> [task2, task3] >> task4  [task5, task6]
```