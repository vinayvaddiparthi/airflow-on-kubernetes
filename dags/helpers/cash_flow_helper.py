import logging
import pandas as pd

from typing import List
from scipy import stats


def calculate_opening_balances(
    opening_balance: float, df: pd.DataFrame, credits_label: str, debits_label: str
) -> List[float]:
    running_balance = opening_balance

    opening_balances = []
    opening_balances.append(running_balance)

    for index in range(0, len(df.index) - 1):
        running_balance = (
            running_balance
            + df[credits_label].iloc[index]
            - df[debits_label].iloc[index]
        )

        opening_balances.append(running_balance)

    return opening_balances


def guardrail_linear_regression(
    cash_flow_df: pd.DataFrame, projection_df: pd.DataFrame
) -> bool:
    cash_flow_copy_df = cash_flow_df.copy()
    projection_copy_df = projection_df.copy()

    cash_flow_copy_df.insert(0, "x", range(1, len(cash_flow_copy_df) + 1))
    cash_flow_copy_df["x^2"] = cash_flow_copy_df["x"] ** 2
    cash_flow_copy_df["y^2"] = cash_flow_copy_df["balance"] ** 2
    cash_flow_copy_df["xy"] = cash_flow_copy_df["x"] * cash_flow_copy_df["balance"]

    n = len(cash_flow_copy_df)
    sum_x = cash_flow_copy_df["x"].sum()
    sum_y = cash_flow_copy_df["balance"].sum()
    sum_xx = cash_flow_copy_df["x^2"].sum()
    sum_xy = cash_flow_copy_df["xy"].sum()

    b = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x**2)
    a = 1 / n * sum_y - b / n * sum_x

    cash_flow_copy_df["yf"] = cash_flow_copy_df["x"] * b + a
    cash_flow_copy_df["yyf"] = cash_flow_copy_df["balance"] - cash_flow_copy_df["yf"]
    cash_flow_copy_df["yyf2"] = cash_flow_copy_df["yyf"] ** 2
    cash_flow_copy_df["xmx"] = cash_flow_copy_df["x"] - sum_x / n
    cash_flow_copy_df["xmx2"] = cash_flow_copy_df["xmx"] ** 2

    sse = cash_flow_copy_df["yyf2"].sum()
    sum_xmx2 = cash_flow_copy_df["xmx2"].sum()

    projection_copy_df.insert(
        0,
        "xnew",
        range(
            len(cash_flow_copy_df) + 1,
            len(projection_copy_df) + len(cash_flow_copy_df) + 1,
        ),
    )
    projection_copy_df["ynew"] = projection_copy_df["xnew"] * b + a

    dof = n - 2
    mse = sse / dof
    conf_level = 0.95
    tval = stats.t.ppf(1 - (1 - conf_level) / 2, dof)

    projection_copy_df["pxmx"] = projection_copy_df["xnew"] - sum_x / n
    projection_copy_df["pmx"] = projection_copy_df["pxmx"] ** 2
    projection_copy_df["psef"] = 1 + 1 / n + projection_copy_df["pmx"] / sum_xmx2
    projection_copy_df["ul"] = projection_copy_df["ynew"] + tval * (
        (mse * projection_copy_df["psef"]) ** (1 / 2)
    )
    projection_copy_df["ll"] = projection_copy_df["ynew"] - tval * (
        (mse * projection_copy_df["psef"]) ** (1 / 2)
    )

    opening_balance_lower_limit = projection_copy_df["ll"].iloc[-1]
    opening_balance_upper_limit = projection_copy_df["ul"].iloc[-1]

    projection_opening_balance = (
        cash_flow_copy_df["balance"].iloc[-1]
        + cash_flow_copy_df["credits"].iloc[-1]
        - cash_flow_copy_df["debits"].iloc[-1]
    )
    projection_copy_df["balance_prediction"] = calculate_opening_balances(
        projection_opening_balance,
        projection_copy_df,
        "credits_prediction",
        "debits_prediction",
    )

    final_opening_balance = projection_copy_df["balance_prediction"].iloc[-1]

    logging.info(
        f"Opening balance lower limit: {opening_balance_lower_limit}, Opening balance upper limit: {opening_balance_upper_limit}, Projected opening balance: {final_opening_balance}"
    )

    return not (
        opening_balance_lower_limit
        <= final_opening_balance
        <= opening_balance_upper_limit
    )
