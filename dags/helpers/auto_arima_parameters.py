import secrets
import attr


@attr.s(kw_only=True)
class AutoArimaParameters:
    start_p: int = attr.ib(default=0)
    max_p: int = attr.ib(default=5)
    start_q: int = attr.ib(default=0)
    max_q: int = attr.ib(default=5)
    max_d: int = attr.ib(default=2)
    start_P: int = attr.ib(default=0)
    max_P: int = attr.ib(default=2)
    start_Q: int = attr.ib(default=0)
    max_Q: int = attr.ib(default=2)
    max_D: int = attr.ib(default=1)
    max_order: int = attr.ib(default=5)
    stepwise: bool = attr.ib(default=True)
    supress_warnings: bool = attr.ib(default=True)
    random: bool = attr.ib(default=True)
    n_fits: int = attr.ib(default=50)
    with_intercept: bool = attr.ib(default=False)
    random_state: int = attr.ib(default=secrets.randbelow(10**20))


@attr.s(kw_only=True)
class ArimaProjectionParameters:
    n_periods: int = attr.ib(default=42)
    return_conf_int: bool = attr.ib(default=True)
    alpha: float = attr.ib(default=0.2)


@attr.s(kw_only=True)
class CashFlowProjectionParameters:
    version: int = 3
