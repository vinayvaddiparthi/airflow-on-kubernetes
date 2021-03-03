import enum


class DbtAction(enum.Enum):
    """
    This is a list of supported DBT actions that will be used when calling DbtOperator
    """

    run = 1
    snapshot = 2
    compile = 3
    debug = 4
    seed = 5
    docs = 6
    test = 7
    deps = 0
