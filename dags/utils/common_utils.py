from datetime import datetime, timezone


def get_utc_timestamp():
    
    utc_time_now = (
        datetime.now(timezone.utc)
        .replace(tzinfo=None)
        .isoformat(sep=" ", timespec="milliseconds")
    )  # get utc time, strip timezone offset and format it
    
    return utc_time_now