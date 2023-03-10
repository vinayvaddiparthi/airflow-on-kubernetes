import attr
from typing import List, Optional, Union

from airflow.models import Variable

from sqlalchemy.sql import ClauseElement
from sqlalchemy import literal_column


@attr.s
class DecryptionSpec:
    schema: str = attr.ib()
    table: str = attr.ib()
    columns: List[str] = attr.ib()
    format: Optional[Union[List[Optional[str]], str]] = attr.ib(default=None)
    catalog: str = attr.ib(default=None)
    whereclause: Optional[ClauseElement] = attr.ib(default=None)


generic_import_executor_config = {
    "resources": {
        "requests": {"memory": "2Gi"},
    },
}


core_import_executor_config = {
    "resources": {
        "requests": {"memory": "4Gi"},
    },
}


decryption_executor_config = {
    "KubernetesExecutor": {
        "annotations": {
            "iam.amazonaws.com/role": "arn:aws:iam::810110616880:role/"
            "KubernetesAirflowProductionZetatangoPiiRole"
        }
    },
    "resources": {
        "requests": {"memory": "2Gi"},
    },
}


core_decryption_spec = [
    DecryptionSpec(
        schema="CORE_PRODUCTION" if (Variable.get(key="environment") == "production") else "CORE_STAGING",
        table="BANK_ACCOUNT_ATTRIBUTES",
        columns=["value"],
    ),
    DecryptionSpec(
        schema="CORE_PRODUCTION" if (Variable.get(key="environment") == "production") else "CORE_STAGING",
        table="MERCHANT_ATTRIBUTES",
        columns=["value"],
        whereclause=literal_column("$1:key").in_(
            [
                "industry",
                "bank_connection_required",
                "selected_bank_account",
                "manual_sic_code",
                "manual_business_online",
                "selected_sales_volume_accounts",
                "selected_insights_bank_accounts",
                "merchant_black_flag",
                "merchant_black_flag_reason",
                "merchant_black_flag_date",
                "merchant_red_flag",
                "merchant_red_flag_reason",
                "merchant_red_flag_date",
                "merchant_on_hold_flag",
                "merchant_on_hold_flag_reason",
                "merchant_on_hold_flag_date",
                "desired_bank_account_balance",
            ]
        ),
    ),
    DecryptionSpec(
        schema="CORE_PRODUCTION" if (Variable.get(key="environment") == "production") else "CORE_STAGING",
        table="LENDING_ADJUDICATIONS",
        columns=[
            "offer_results",
            "adjudication_results",
            "notes",
        ],
        format=["yaml", "yaml", None],
    ),
    DecryptionSpec(
        schema="CORE_PRODUCTION" if (Variable.get(key="environment") == "production") else "CORE_STAGING",
        table="LENDING_ADJUDICATION_DECISIONS",
        columns=["notes"],
    ),
    DecryptionSpec(
        schema="CORE_PRODUCTION" if (Variable.get(key="environment") == "production") else "CORE_STAGING",
        table="LENDING_LOAN_ATTRIBUTES",
        columns=["value"],
        whereclause=literal_column("$1:key").in_(["external_id"]),
    ),
    DecryptionSpec(
        schema="CORE_PRODUCTION" if (Variable.get(key="environment") == "production") else "CORE_STAGING",
        table="QUICKBOOKS_ACCOUNTING_TRANSACTIONS",
        columns=["account", "split"],
    ),
    DecryptionSpec(
        schema="CORE_PRODUCTION" if (Variable.get(key="environment") == "production") else "CORE_STAGING",
        table="LEADS",
        columns=[
            "applicant_email",
            "applicant_first_name",
            "applicant_last_name",
            "merchant_name",
        ],
    ),
    DecryptionSpec(
        schema="CORE_PRODUCTION" if (Variable.get(key="environment") == "production") else "CORE_STAGING",
        table="LEAD_ATTRIBUTES",
        columns=["value"],
        whereclause=literal_column("$1:key").in_(
            [
                "bank_connection_required",
                "banking_institution",
                "bank_account_institution_number",
                "bank_account_transit_number",
                "marketing_qualified_lead",
                "selected_insights_bank_accounts",
                "desired_bank_account_balance",
                "merchant_self_attested_average_monthly_sales",
                "merchant_self_attested_date_established",
            ]
        ),
    ),
    DecryptionSpec(
        schema="CORE_PRODUCTION" if (Variable.get(key="environment") == "production") else "CORE_STAGING",
        table="EMAILS",
        columns=["from", "html_body", "subject", "text_body", "to"],
    ),
]


idp_decryption_spec = [
    DecryptionSpec(
        schema="IDP_PRODUCTION" if (Variable.get(key="environment") == "production") else "IDP_STAGING",
        table="POLY_PROPERTIES",
        columns=["value"],
        whereclause=literal_column("$1:key").in_(
            [
                "merchant",
                "applicant",
                "applicants",
                "insights_preference_email",
                "product_preference",
                "phone_number",
                "mfa_mode",
                "role",
                "sso",
                "lead",
                "leads_source",
                "preferred_language",
                "referrer",
                "sso_linked_account",
                "loc_notification_preference",
            ]
        ),
    ),
]


kyc_decryption_spec = [
    DecryptionSpec(
        schema="KYC_PRODUCTION" if (Variable.get(key="environment") == "production") else "KYC_STAGING",
        table="INDIVIDUALS_APPLICANTS",
        columns=[
            "date_of_birth",
            "first_name",
            "last_name",
            "middle_name",
        ],
    ),
    DecryptionSpec(
        schema="KYC_PRODUCTION" if (Variable.get(key="environment") == "production") else "KYC_STAGING",
        table="INDIVIDUAL_ATTRIBUTES",
        columns=["value"],
        format="marshal",
        whereclause=literal_column("$1:key").in_(["default_beacon_score"]),
    ),
    DecryptionSpec(
        schema="KYC_PRODUCTION" if (Variable.get(key="environment") == "production") else "KYC_STAGING",
        table="ENTITIES_BANK_ACCOUNT_ATTRIBUTES",
        columns=["value"],
        whereclause=literal_column("$1:key").in_(
            [
                "account_number",
                "confirmed",
                "flinks_account_holder",
                "flinks_account_id",
                "flinks_account_type",
                "flinks_login_guid",
                "institution_number",
                "last_transaction_date",
                "source",
                "stale",
                "transit_number",
                "valid_holder",
                "verified",
            ]
        ),
    ),
]
