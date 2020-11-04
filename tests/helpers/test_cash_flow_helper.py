import os
import pandas as pd
import glob
import json
import pytest

from dags.helpers.cash_flow_helper import guardrail_linear_regression

pass_files = [
    (
        f"tests/data/post_guardrail_linear_regression/pass{index}/cash_flow_df.json",
        f"tests/data/post_guardrail_linear_regression/pass{index}/projection_df.json",
    )
    for index in range(5)
]
fail_files = [
    (
        f"tests/data/post_guardrail_linear_regression/fail{index}/cash_flow_df.json",
        f"tests/data/post_guardrail_linear_regression/fail{index}/projection_df.json",
    )
    for index in range(5)
]


@pytest.mark.parametrize("cash_flow_file,projection_file", pass_files)
def test_guardrail_pass(cash_flow_file, projection_file):
    with open(cash_flow_file, "r") as outfile:
        cash_flow = json.load(outfile)
    with open(projection_file, "r") as outfile:
        projection = json.load(outfile)

    cash_flow_df = pd.DataFrame.from_dict(cash_flow, orient="index")
    projection_df = pd.DataFrame.from_dict(projection, orient="index")

    cash_flow_df.index = pd.to_datetime(cash_flow_df.index)
    projection_df.index = pd.to_datetime(projection_df.index)

    cash_flow_df.sort_index(inplace=True)
    projection_df.sort_index(inplace=True)

    assert not guardrail_linear_regression(cash_flow_df, projection_df)


@pytest.mark.parametrize("cash_flow_file,projection_file", fail_files)
def test_guardrail_fail(cash_flow_file, projection_file):
    with open(cash_flow_file, "r") as outfile:
        cash_flow = json.load(outfile)
    with open(projection_file, "r") as outfile:
        projection = json.load(outfile)

    cash_flow_df = pd.DataFrame.from_dict(cash_flow, orient="index")
    projection_df = pd.DataFrame.from_dict(projection, orient="index")

    cash_flow_df.index = pd.to_datetime(cash_flow_df.index)
    projection_df.index = pd.to_datetime(projection_df.index)

    cash_flow_df.sort_index(inplace=True)
    projection_df.sort_index(inplace=True)

    assert guardrail_linear_regression(cash_flow_df, projection_df)
