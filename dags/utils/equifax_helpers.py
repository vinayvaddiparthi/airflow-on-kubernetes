from datetime import datetime, timedelta


def get_import_month(ds_nodash: str) -> str:
    """
    Returns the previous month based on the Airflow execution date (ds).
    For example, if ds_nodash = '20210902' then the return value = '202108'.
    """
    return (
        datetime.strptime(str(ds_nodash), "%Y%m%d").replace(day=1) - timedelta(days=1)
    ).strftime("%Y%m")
