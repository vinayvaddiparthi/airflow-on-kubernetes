import pandas as pd
import re
from transliterate import translit
from transliterate.base import TranslitLanguagePack, registry
from typing import Union, List, Tuple
from utils.french_language_pack import FrenchLanguagePack


class SalesClassification:
    def __init__(self):

        registry.register(UpdatedFrenchLanguagePack)

    def __clean_description(self, description: str) -> str:
        """
        Cleans and transliterates the transaction description

        Args:
            desc (str): transaction description

        Returns:
            str: cleaned description
        """
        # transliterate French characters to English
        transliterated_description = translit(description, "fr", reversed=True)
        # remove non alphanumeric characters
        description = re.sub(r"[^\w]", "", transliterated_description).upper()

        return description

    def __refine_e_transfer_lookup(self, description: str, is_nsd: bool) -> bool:
        """
        Refines the non-sales deposit flag for e-transfer transactions

        Args:
            desc (str): transaction description
            is_nsd (bool): boolean flag indicating whether the transaction is a non-sales deposit

        Returns:
            bool: boolean flag indicating whether the transaction is a non-sales deposit
        """

        if "ELECTRONICFUNDSTRANSFER" not in description:
            return is_nsd

        cc_list = ["VISA", "MC", "MASTERCARD", "IDP"]

        for cc in cc_list:
            if cc in description:
                return False

        return is_nsd

    def __categorize_transactions(
        self,
        transaction: pd.Series,
        precise_entries: pd.DataFrame,
        imprecise_entries: pd.DataFrame,
    ) -> Tuple[str, bool]:
        """
        Categories the transaction based on the description

        Args:
            transaction (pd.Series): a transaction row
            precise_entries (pd.DataFrame): lookup entries used for exact keyword matching
            imprecise_entries (pd.DataFrame): lookup entries used for non-exact keyword matching

        Returns:
            Tuple[str, bool]: a tuple with below items
            category (str): category of the transaction
            is_nsd (bool): boolean flag indicating whether the transaction is a non-sales deposit
        """

        category = "no-match"
        is_nsd = False

        desc = transaction["description"]

        # 1. check for blank or all numbers

        if desc == "":

            category = "no-description"
            is_nsd = True

            return category, is_nsd

        pattern = re.compile(r"^\d+(?:-\d+)*$")

        if pattern.match(desc.strip("\n")):

            category = "all-numbers"
            is_nsd = True

            return category, is_nsd

        # 2. check for precise matches (CRA, NSF)

        # Used a list comprehension along with zip to get the first value of the list
        # instead of a simple df.loc since loc is slow
        matching_entry = []
        for key, type, nsd in zip(
            precise_entries["key"],
            precise_entries["transaction_type"],
            precise_entries["is_non_sales_deposit"],
        ):
            match = re.search(r"\b" + key + r"\b", desc)
            if match and match[0] == key:
                matching_entry.append((type, nsd))

        if matching_entry:

            category = matching_entry[0][0]
            is_nsd = matching_entry[0][1]

            return category, is_nsd

        # 3. Check for cleaned description match (the rest)

        cleaned_desc = self.__clean_description(desc)

        # Used a list comprehension along with zip to get the first value of the list
        # instead of a simple df.loc since loc is slow
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

                is_nsd = self.__refine_e_transfer_lookup(cleaned_desc, is_nsd)

            return category, is_nsd

        return category, is_nsd

    def __get_industry_characteristics(
        self, df_merchant_industry: pd.DataFrame, merchant_guid: str
    ) -> Tuple[bool, bool]:
        """
        Fetches the e-transfer and large round credit characteristics of the industry

        Args:
            df_merchant_industry (pd.DataFrame): dataframe with merchant and industry characteristics
            merchant_guid (str): merchant guid

        Returns:
            Tuple[bool, bool]: a tuple with below items
            accepts_e_transfer (bool): boolean flag indicating whether the industry accepts e-transfers
            accepts_lrc (bool): boolean flag indicating whether the industry accepts large round credits
        """

        # accept_e_transfer = df_merchant_industry.loc[df_merchant_industry['guid'] == merchant_guid, "accepts_e_transfer"].squeeze()
        # Used a list comprehension along with zip to get the first value of the list instead of a simple df.loc since loc is slow

        accepts_e_transfer_list = [
            flag
            for m_guid, flag in zip(
                df_merchant_industry["guid"], df_merchant_industry["accepts_e_transfer"]
            )
            if m_guid == merchant_guid
        ]

        accepts_e_transfer = (
            accepts_e_transfer_list[0] if accepts_e_transfer_list else False
        )

        # accept_lrc = df_merchant_industry.loc[df_merchant_industry['guid'] == merchant_guid, "accepts_lrc"].squeeze()
        # Used a list comprehension along with zip to get the first value of the list instead of a simple df.loc since loc is slow

        accepts_lrc_list = [
            flag
            for m_guid, flag in zip(
                df_merchant_industry["guid"], df_merchant_industry["accepts_lrc"]
            )
            if m_guid == merchant_guid
        ]

        accepts_lrc = accepts_lrc_list[0] if accepts_lrc_list else False

        return accepts_e_transfer, accepts_lrc

    def __process_transactions(
        self,
        category: str,
        credit: float,
        accepts_e_transfer: bool,
        accepts_lrc: bool,
        is_lrc: bool,
        is_nsd: bool,
    ) -> float:
        """
        Processes each transaction and returns the true sales amount. Will be 0 if the transaction is a
        reversal or a non-sales deposit. Implementation replicated from proc_trans_amount method in
        sales_volume_service.rb service in core

        Args:
            category (str): category of the transaction
            credit (float): credit amount of the transaction if any else null
            accepts_e_transfer (bool): boolean flag indicating whether the industry accepts e-transfers
            accepts_lrc (bool): boolean flag indicating whether the industry accepts large round credits
            is_lrc (bool): boolean flag indicating whether the transaction is a large round credit
            is_nsd (bool): boolean flag indicating whether the transaction is a non-sales deposit

        Returns:
            float: sales amount of the transaction
        """

        revenue = (
            0.0
            if is_nsd
            else self.__process_revenue(
                category, credit, accepts_e_transfer, accepts_lrc, is_lrc
            )
        )

        reversal = (
            0.0
            if is_nsd
            else self.__process_reversal(category, credit, accepts_lrc, is_lrc)
        )

        processed_amount = revenue - reversal

        return processed_amount

    def __process_revenue(
        self,
        category: str,
        credit: float,
        accepts_e_transfer: bool,
        accepts_lrc: bool,
        is_lrc: bool,
    ) -> float:
        """
        Processes the credit amount of the transaction based on whether the transaction is a large round credit
        and if the industry accepts e-transfers

        Args:
            category (str): category of the transaction
            credit (float): credit amount of the transaction if any else null
            accepts_e_transfer (bool): boolean flag indicating whether the industry accepts e-transfers
            accepts_lrc (bool): boolean flag indicating whether the industry accepts large round credits
            is_lrc (bool): boolean flag indicating whether the transaction is a large round credit

        Returns:
            float: processed credit amount of the transaction
        """

        revenue = 0.0

        if not accepts_e_transfer and not accepts_lrc:
            if (
                category
                in ["Deposit", "Merchant Processing Deposit", "Return", "Reversal"]
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
            if category in [
                "Deposit",
                "Merchant Processing Deposit",
                "Return",
                "Reversal",
            ]:
                revenue = credit
            elif category == "Foreign Exchange Deposit":
                revenue = credit * 0.9
        else:
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

    def __process_reversal(
        self, category: str, credit: float, accepts_lrc: bool, is_lrc: bool
    ) -> float:
        """
        Processes the credit amount of the transaction based on whether the transaction is a large round credit
        and if the industry accepts e-transfers

        Args:
            category (str): category of the transaction
            credit (float): credit amount of the transaction if any else null
            accepts_lrc (bool): boolean flag indicating whether the industry accepts large round credits
            is_lrc (bool): boolean flag indicating whether the transaction is a large round credit

        Returns:
            float: processed credit amount of the transaction
        """
        reversal = 0.0

        if category not in ["Return", "Reversal"]:
            return reversal

        if accepts_lrc or not is_lrc:
            reversal = credit

        return reversal

    def calculate_sales_volume(
        self,
        transaction: pd.Series,
        precise_entries: pd.DataFrame,
        imprecise_entries: pd.DataFrame,
        df_merchant_industry: pd.DataFrame,
    ) -> List[Union[str, bool, float]]:
        """
        Calculates the sales volume for a transaction
        The logic for the calculation has been ported from the sales_volume_service in Zetatango
        https://github.com/Zetatango/zetatango/blob/master/app/services/sales_volume_service.rb

        Args:
            transaction (pd.Series): a transaction row
            precise_entries (pd.DataFrame): lookup entries used for exact keyword matching
            imprecise_entries (pd.DataFrame): lookup entries used for non-exact keyword matching
            df_merchant_industry (pd.DataFrame): dataframe with merchant and industry characteristics

        Returns:
            List[Union[str, bool, float]]: a list with below 3 items
            category (str): category of the transaction
            is_nsd (bool): boolean flag indicating whether the transaction is a non-sales deposit
            processed_credit (float): processed credit amount of the transaction
        """

        credit = transaction["credit"]

        is_lrc = credit > 10000 and credit % 10 == 0

        merchant_guid = transaction["merchant_guid"]

        category, is_nsd = self.__categorize_transactions(
            transaction, precise_entries, imprecise_entries
        )

        accepts_e_transfer, accepts_lrc = self.__get_industry_characteristics(
            df_merchant_industry, merchant_guid
        )

        processed_credit = self.__process_transactions(
            category, credit, accepts_e_transfer, accepts_lrc, is_lrc, is_nsd
        )

        return [category, is_nsd, processed_credit]


class UpdatedFrenchLanguagePack(FrenchLanguagePack):

    reversed_specific_mapping = (
        "ÀàâÉéÈèÊêËëÎîÏïÔôÛûÙùÜüÇç",
        "AaaEeEeEeEeIiIiOoUuUuUuCc",
    )
    reversed_specific_pre_processor_mapping = {
        "Ã©": "e",
        "Ã¨": "e",
        "Ãª": "e",
        "Ã´": "o",
        "Â": "to",
        "Œ": "Oe",
        "œ": "oe",
    }
