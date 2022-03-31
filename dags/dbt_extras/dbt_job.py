import enum


class DbtJobRunStatus(enum.IntEnum):
    """
    This is a list of supported DBT actions that will be used when calling DBT jobs
    """

    QUEUED = 1
    STARTING = 2
    RUNNING = 3
    SUCCESS = 10
    ERROR = 20
    CANCELLED = 30