import pandas as pd
import re
from typing import List, Union, Optional


def tc_description_clean(desc: str) -> str:

    desc.replace("Â", "to").replace("Â", "to").replace("Ã´", "o").replace(
        "Ã¨", "e"
    ).replace("Ãª", "e")

    # remove spaces
    desc = re.sub(r"\s", "", desc)

    # remove non alphanumeric characters
    desc = re.sub(r"[^\w]", "", desc)

    return desc.upper()


def refine_e_transfer_lookup(desc: str, nsd) -> Optional[bool]:

    if "ELECTRONICFUNDSTRANSFER" not in desc:
        return nsd

    cc_list = ["VISA", "MC", "MASTERCARD", "IDP"]

    for cc in cc_list:
        if cc in desc:
            return False

    return nsd


def categorize_transactions(
    test_data: pd.Series, lookup_entries: pd.DataFrame
) -> Union[str, bool]:

    category = None
    is_nsd = False

    desc = test_data["description"]

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

    precise_entries = lookup_entries[lookup_entries["has_match"]]

    matching_entry = precise_entries[precise_entries["key"] == desc]

    if not matching_entry.empty:

        # take the first match
        first_match = matching_entry[:1].reset_index(drop=True)

        category = first_match.loc[0, "transaction_type"]
        is_nsd = first_match.loc[0, "is_non_sales_deposit"]

        return [category, is_nsd]

    # 3. Check for cleaned description match (the rest)

    inprecise_entries = lookup_entries[~lookup_entries["has_match"]]

    cleaned_desc = tc_description_clean(desc)

    matching_entry = [
        (t, n)
        for k, t, n in zip(
            inprecise_entries["key"],
            inprecise_entries["transaction_type"],
            inprecise_entries["is_non_sales_deposit"],
        )
        if k in cleaned_desc
    ]

    if matching_entry:

        category = matching_entry[0][0]
        is_nsd = matching_entry[0][1]

        if category == "Transfer":

            is_nsd = refine_e_transfer_lookup(cleaned_desc, is_nsd)

        return [category, is_nsd]

    category = "no_match"

    return [category, is_nsd]
