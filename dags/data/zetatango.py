import attr
from typing import List, Optional, Union

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


kubernetes_config = {
    "KubernetesExecutor": {
        "annotations": {
            "iam.amazonaws.com/role": "arn:aws:iam::810110616880:role/"
            "KubernetesAirflowProductionZetatangoPiiRole"
        }
    },
}


decryption_executor_config = {
    **kubernetes_config,
    "resources": {
        "requests": {"memory": "2Gi"},
    },
}


quickbooks_decryption_executor_config = {
    **kubernetes_config,
    "resources": {
        "requests": {"memory": "4Gi"},
    },
}


core_decryption_spec = [
    DecryptionSpec(
        schema="CORE_PRODUCTION",
        table="BANK_ACCOUNT_ATTRIBUTES",
        columns=["value"],
    ),
    DecryptionSpec(
        schema="CORE_PRODUCTION",
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
                "merchant_identified_accounts",
            ]
        ),
    ),
    DecryptionSpec(
        schema="CORE_PRODUCTION",
        table="LENDING_ADJUDICATIONS",
        columns=[
            "offer_results",
            "adjudication_results",
            "notes",
        ],
        format=["yaml", "yaml", None],
    ),
    DecryptionSpec(
        schema="CORE_PRODUCTION",
        table="LENDING_ADJUDICATION_DECISIONS",
        columns=["notes"],
    ),
    DecryptionSpec(
        schema="CORE_PRODUCTION",
        table="LENDING_LOAN_ATTRIBUTES",
        columns=["value"],
        whereclause=literal_column("$1:key").in_(["external_id"]),
    ),
    DecryptionSpec(
        schema="CORE_PRODUCTION",
        table="LEADS",
        columns=[
            "applicant_email",
            "applicant_first_name",
            "applicant_last_name",
            "merchant_name",
        ],
    ),
    DecryptionSpec(
        schema="CORE_PRODUCTION",
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
        schema="CORE_PRODUCTION",
        table="EMAILS",
        columns=["from", "html_body", "subject", "text_body", "to"],
    ),
    DecryptionSpec(
        schema="CORE_PRODUCTION",
        table="OBJECT_BLOBS",
        columns=["blob_value"],
        whereclause=literal_column(
            "parse_json(replace($1:metadata, '\\\"', '\\'')):type"
        ).in_(["adjudication_results", "offer_results"]),
        format=["yaml", "yaml"],
    ),
]


idp_decryption_spec = [
    DecryptionSpec(
        schema="IDP_PRODUCTION",
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
                "casl_preference",
            ]
        ),
    ),
]


kyc_decryption_spec = [
    DecryptionSpec(
        schema="KYC_PRODUCTION",
        table="INDIVIDUALS_APPLICANTS",
        columns=[
            "date_of_birth",
            "first_name",
            "last_name",
            "middle_name",
        ],
    ),
    DecryptionSpec(
        schema="KYC_PRODUCTION",
        table="FLINKS_LOGINS",
        columns=[
            "login_id",
        ],
    ),
    DecryptionSpec(
        schema="KYC_PRODUCTION",
        table="INDIVIDUAL_ATTRIBUTES",
        columns=["value"],
        format="marshal",
        whereclause=literal_column("$1:key").in_(["default_beacon_score"]),
    ),
    DecryptionSpec(
        schema="KYC_PRODUCTION",
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
                "plaid_account_holder",
                "plaid_item_guid",
                "plaid_account_id",
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
    DecryptionSpec(
        schema="KYC_PRODUCTION",
        table="ENTITIES_MERCHANT_ATTRIBUTES",
        columns=["value"],
        whereclause=literal_column("$1:key").in_(
            [
                "self_attested_average_monthly_sales",
                "self_attested_date_established",
            ]
        ),
    ),
]


qbo_decryption_spec = [
    DecryptionSpec(
        schema="CORE_PRODUCTION",
        table="QUICKBOOKS_ACCOUNTING_TRANSACTIONS",
        columns=[
            "account",
            "split",
            "additional_info",
            "description",
            "name",
        ],
    ),
]
