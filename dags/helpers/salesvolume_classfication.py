import pandas as pd
import re
from typing import Union, List


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
) -> List[Union[str, bool]]:

    category = "no_match"
    is_nsd = False

    desc = transaction["description"]

    # 1. check for blank or all numbers

    if desc == "":

        category = "no-description"
        is_nsd = True

        return [category, is_nsd]

    pattern = re.compile("^\d+(?:-\d+)*$")

    if pattern.match(desc.strip("\n")):

        category = "all-numbers"
        is_nsd = True

        return [category, is_nsd]

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

        return [category, is_nsd]

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

        return [category, is_nsd]

    return [category, is_nsd]
