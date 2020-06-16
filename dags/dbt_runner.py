with DAG(
    "dbt_extras",
    max_active_runs=1,
    schedule_interval=None,
    start_date=pendulum.datetime(
        2020, 4, 21, tzinfo=pendulum.timezone("America/Toronto")
    ),
) as dag:
    dag << BashOperator(
        task_id="dbt_refresh",
        pool="snowflake_pool",
        execution_timeout=datetime.timedelta(hours=4),
        retry_delay=datetime.timedelta(hours=1),
        retries=3,
        priority_weight=sobject.get("weight", 0),
        bash_command="git clone https://${gl_user}:${gl_token}@gitlab.com/tc-data/curated-data-warehouse.git"
        "&& cd curated-data-warehouse"
        "&& dbt seed --profiles-dir . --target prod"
        "&& dbt run --profiles-dir . --target prod"
        "&& dbt snapshot --profiles-dir . --target prod",
        env={
            "SNOWFLAKE_ACCOUNT": dbt_hook.host,
            "SNOWFLAKE_USERNAME": dbt_hook.login,
            "SNOWFLAKE_PASSWORD": dbt_hook.password,
            "SNOWFLAKE_DATABASE": "ANALYTICS_PRODUCTION",
            "SNOWFLAKE_SCHEMA": "DBT_ARIO",
            "SNOWFLAKE_ROLE": "DBT_PRODUCTION",
            "SNOWFLAKE_WAREHOUSE": "ETL",
            "ZETATANGO_ENV": "PRODUCTION",
            "gl_user": gl_user,
            "gl_token": gl_token,
        },
    )
