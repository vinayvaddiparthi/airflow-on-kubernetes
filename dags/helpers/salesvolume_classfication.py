import pandas as pd
import re
from typing import Union, List, Tuple


def clean_description(desc: str) -> str:

    desc.replace("Â", "to").replace("Â", "to").replace("Ã´", "o").replace(
        "Ã¨", "e"
    ).replace("Ãª", "e")

    # remove non alphanumeric characters
    desc = re.sub(r"[^\w]", "", desc).upper()

    return desc


def refine_e_transfer_lookup(desc: str, nsd: bool) -> bool:

    if "ELECTRONICFUNDSTRANSFER" not in desc:
        return nsd

    cc_list = ["VISA", "MC", "MASTERCARD", "IDP"]

    for cc in cc_list:
        if cc in desc:
            return False

    return nsd


def categorize_transactions(
    transaction: pd.Series,
    precise_entries: pd.DataFrame,
    imprecise_entries: pd.DataFrame,
) -> Tuple[str, bool]:

    category = "no_match"
    is_nsd = False

    desc = transaction["description"]

    # 1. check for blank or all numbers

    if desc == "":

        category = "no-description"
        is_nsd = True

        return category, is_nsd

    pattern = re.compile("^\d+(?:-\d+)*$")

    if pattern.match(desc.strip("\n")):

        category = "all-numbers"
        is_nsd = True

        return category, is_nsd

    # 2. check for precise matches (CRA, NSF)

    matching_entry = [
        (type, nsd)
        for key, type, nsd in zip(
            precise_entries["key"],
            precise_entries["transaction_type"],
            precise_entries["is_non_sales_deposit"],
        )
        if key == desc
    ]

    if matching_entry:

        category = matching_entry[0][0]
        is_nsd = matching_entry[0][1]

        return category, is_nsd

    # 3. Check for cleaned description match (the rest)

    cleaned_desc = clean_description(desc)

    matching_entry = [
        (type, nsd)
        for key, type, nsd in zip(
            imprecise_entries["key"],
            imprecise_entries["transaction_type"],
            imprecise_entries["is_non_sales_deposit"],
        )
        if key in cleaned_desc
    ]

    if matching_entry:

        category = matching_entry[0][0]
        is_nsd = matching_entry[0][1]

        if category == "Transfer":

            is_nsd = refine_e_transfer_lookup(cleaned_desc, is_nsd)

        return category, is_nsd

    return category, is_nsd


def get_industry_characteristics(
    df_merchant_industry: pd.DataFrame, merchant_guid: str
) -> Tuple[bool, bool]:

    # accept_e_transfer = df_merchant_industry.loc[df_merchant_industry['guid'] == merchant_guid, "accepts_e_transfer"].squeeze()

    accepts_e_transfer_list = [
        flag
        for m_guid, flag in zip(
            df_merchant_industry["guid"], df_merchant_industry["accepts_e_transfer"]
        )
        if m_guid == merchant_guid
    ]

    accepts_e_transfer = accepts_e_transfer_list[0] or False

    # accept_lrc = df_merchant_industry.loc[df_merchant_industry['guid'] == merchant_guid, "accepts_lrc"].squeeze()

    accepts_lrc_list = [
        flag
        for m_guid, flag in zip(
            df_merchant_industry["guid"], df_merchant_industry["accepts_lrc"]
        )
        if m_guid == merchant_guid
    ]

    accepts_lrc = accepts_lrc_list[0] or False

    return accepts_e_transfer, accepts_lrc


def process_transactions(
    category: str,
    credit: float,
    accepts_e_transfer: bool,
    accepts_lrc: bool,
    is_lrc: bool,
    is_nsd: bool,
) -> float:

    revenue = (
        0
        if is_nsd
        else process_revenue(category, credit, accepts_e_transfer, accepts_lrc, is_lrc)
    )

    reversal = 0 if is_nsd else process_reversal(category, credit, accepts_lrc, is_lrc)

    processed_amount = revenue - reversal

    return processed_amount


def process_revenue(
    category: str,
    credit: float,
    accepts_e_transfer: bool,
    accepts_lrc: bool,
    is_lrc: bool,
) -> float:

    revenue = credit

    if not accepts_e_transfer and not accepts_lrc:
        if (
            category in ["Deposit", "Merchant Processing Deposit", "Return", "Reversal"]
            and not is_lrc
        ):
            revenue = credit
        elif category == "Foreign Exchange Deposit" and not is_lrc:
            revenue = credit * 0.9
    elif accepts_e_transfer and not accepts_lrc:
        if (
            category
            in [
                "Deposit",
                "Merchant Processing Deposit",
                "E-Transfer",
                "Return",
                "Reversal",
            ]
            and not is_lrc
        ):
            revenue = credit
        elif category == "Foreign Exchange Deposit" and not is_lrc:
            revenue = credit * 0.9
    elif not accepts_e_transfer and accepts_lrc:
        if category in ["Deposit", "Merchant Processing Deposit", "Return", "Reversal"]:
            revenue = credit
        elif category == "Foreign Exchange Deposit":
            revenue = credit * 0.9
    elif accepts_e_transfer and accepts_lrc:
        if category in [
            "Deposit",
            "Merchant Processing Deposit",
            "E-Transfer",
            "Return",
            "Reversal",
        ]:
            revenue = credit
        elif category == "Foreign Exchange Deposit":
            revenue = credit * 0.9

    return revenue


def process_reversal(
    category: str, credit: float, accepts_lrc: bool, is_lrc: bool
) -> float:

    flag = accepts_lrc or not is_lrc

    if (category not in ["Return", "Reversal"]) or not flag:
        reversal = 0.0
    elif flag:
        reversal = credit

    return reversal


def calculate_sales_volume(
    transaction: pd.Series,
    precise_entries: pd.DataFrame,
    imprecise_entries: pd.DataFrame,
    df_merchant_industry: pd.DataFrame,
) -> List[Union[str, bool, float]]:
    credit = transaction["credit"]

    is_lrc = credit > 10000 and credit % 10 == 0

    merchant_guid = transaction["merchant_guid"]

    category, is_nsd = categorize_transactions(
        transaction, precise_entries, imprecise_entries
    )

    accepts_e_transfer, accepts_lrc = get_industry_characteristics(
        df_merchant_industry, merchant_guid
    )

    processed_credit = process_transactions(
        category, credit, accepts_e_transfer, accepts_lrc, is_lrc, is_nsd
    )

    return [category, is_nsd, processed_credit]
