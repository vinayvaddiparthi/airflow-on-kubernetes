import pendulum
from utils.table_swap_dag import create_table_swap_dag

DAG_PARSE_WORKAROUND = "airflow DAG"


globals()["airflow_cg_production_to_snowflake"] = create_table_swap_dag(
    "cg_staging_to_snowflake",
    pendulum.datetime(2020, 1, 22, tzinfo=pendulum.timezone("America/Toronto")),
    "cg_lms_staging",
    "sf_creditgenie",
    "creditgenie",
    [
        {
            "src": {"schema": "cg-lms", "table": "transaction_discount"},
            "dst": {"schema": "staging", "table": "transaction_discount"},
        },
        {
            "src": {"schema": "cg-lms", "table": "refund_partialrefund"},
            "dst": {"schema": "staging", "table": "refund_partialrefund"},
        },
        {
            "src": {"schema": "cg-lms", "table": "bankaccount_bankaccount"},
            "dst": {"schema": "staging", "table": "bankaccount_bankaccount"},
        },
        {
            "src": {"schema": "cg-lms", "table": "bankaccount_historicalbankaccount"},
            "dst": {"schema": "staging", "table": "bankaccount_historicalbankaccount"},
        },
        {
            "src": {"schema": "cg-lms", "table": "bankaccount_historicalinstitution"},
            "dst": {"schema": "staging", "table": "bankaccount_historicalinstitution"},
        },
        {
            "src": {"schema": "cg-lms", "table": "bankaccount_institution"},
            "dst": {"schema": "staging", "table": "bankaccount_institution"},
        },
        {
            "src": {"schema": "cg-lms", "table": "customer_customer"},
            "dst": {"schema": "staging", "table": "customer_customer"},
        },
        {
            "src": {"schema": "cg-lms", "table": "customer_historicalcustomer"},
            "dst": {"schema": "staging", "table": "customer_historicalcustomer"},
        },
        {
            "src": {"schema": "cg-lms", "table": "financing_discountrates"},
            "dst": {"schema": "staging", "table": "financing_discountrates"},
        },
        {
            "src": {"schema": "cg-lms", "table": "financing_facility"},
            "dst": {"schema": "staging", "table": "financing_facility"},
        },
        {
            "src": {"schema": "cg-lms", "table": "financing_facilitytransfer"},
            "dst": {"schema": "staging", "table": "financing_facilitytransfer"},
        },
        {
            "src": {"schema": "cg-lms", "table": "financing_financing"},
            "dst": {"schema": "staging", "table": "financing_financing"},
        },
        {
            "src": {"schema": "cg-lms", "table": "financing_financingrequest"},
            "dst": {"schema": "staging", "table": "financing_financingrequest"},
        },
        {
            "src": {"schema": "cg-lms", "table": "financing_financingstatistics"},
            "dst": {"schema": "staging", "table": "financing_financingstatistics"},
        },
        {
            "src": {"schema": "cg-lms", "table": "financing_historicaldiscountrates"},
            "dst": {"schema": "staging", "table": "financing_historicaldiscountrates"},
        },
        {
            "src": {"schema": "cg-lms", "table": "financing_historicalfacility"},
            "dst": {"schema": "staging", "table": "financing_historicalfacility"},
        },
        {
            "src": {"schema": "cg-lms", "table": "financing_historicalfinancing"},
            "dst": {"schema": "staging", "table": "financing_historicalfinancing"},
        },
        {
            "src": {
                "schema": "cg-lms",
                "table": "financing_historicalfinancingrequest",
            },
            "dst": {
                "schema": "staging",
                "table": "financing_historicalfinancingrequest",
            },
        },
        {
            "src": {
                "schema": "cg-lms",
                "table": "financing_historicalfinancingstatistics",
            },
            "dst": {
                "schema": "staging",
                "table": "financing_historicalfinancingstatistics",
            },
        },
        {
            "src": {
                "schema": "cg-lms",
                "table": "financing_historicalinterestfrequency",
            },
            "dst": {
                "schema": "staging",
                "table": "financing_historicalinterestfrequency",
            },
        },
        {
            "src": {"schema": "cg-lms", "table": "financing_historicalproduct"},
            "dst": {"schema": "staging", "table": "financing_historicalproduct"},
        },
        {
            "src": {
                "schema": "cg-lms",
                "table": "financing_historicalrepaymentfrequency",
            },
            "dst": {
                "schema": "staging",
                "table": "financing_historicalrepaymentfrequency",
            },
        },
        {
            "src": {"schema": "cg-lms", "table": "financing_interestfrequency"},
            "dst": {"schema": "staging", "table": "financing_interestfrequency"},
        },
        {
            "src": {"schema": "cg-lms", "table": "financing_product"},
            "dst": {"schema": "staging", "table": "financing_product"},
        },
        {
            "src": {"schema": "cg-lms", "table": "financing_repaymentfrequency"},
            "dst": {"schema": "staging", "table": "financing_repaymentfrequency"},
        },
        {
            "src": {"schema": "cg-lms", "table": "refund_fullrefund"},
            "dst": {"schema": "staging", "table": "refund_fullrefund"},
        },
        {
            "src": {"schema": "cg-lms", "table": "refund_historicalfullrefund"},
            "dst": {"schema": "staging", "table": "refund_historicalfullrefund"},
        },
        {
            "src": {"schema": "cg-lms", "table": "refund_historicalpartialrefund"},
            "dst": {"schema": "staging", "table": "refund_historicalpartialrefund"},
        },
        {
            "src": {"schema": "cg-lms", "table": "transaction_historicaldiscount"},
            "dst": {"schema": "staging", "table": "transaction_historicaldiscount"},
        },
        {
            "src": {"schema": "cg-lms", "table": "transaction_historicalmanualfee"},
            "dst": {"schema": "staging", "table": "transaction_historicalmanualfee"},
        },
        {
            "src": {
                "schema": "cg-lms",
                "table": "transaction_historicalmanualrepayment",
            },
            "dst": {
                "schema": "staging",
                "table": "transaction_historicalmanualrepayment",
            },
        },
        {
            "src": {"schema": "cg-lms", "table": "transaction_historicalnsf"},
            "dst": {"schema": "staging", "table": "transaction_historicalnsf"},
        },
        {
            "src": {
                "schema": "cg-lms",
                "table": "transaction_historicalofflinerepayment",
            },
            "dst": {
                "schema": "staging",
                "table": "transaction_historicalofflinerepayment",
            },
        },
        {
            "src": {"schema": "cg-lms", "table": "transaction_historicaltransaction"},
            "dst": {"schema": "staging", "table": "transaction_historicaltransaction"},
        },
        {
            "src": {
                "schema": "cg-lms",
                "table": "transaction_historicaltransactionbucket",
            },
            "dst": {
                "schema": "staging",
                "table": "transaction_historicaltransactionbucket",
            },
        },
        {
            "src": {
                "schema": "cg-lms",
                "table": "transaction_historicaltransactionrequest",
            },
            "dst": {
                "schema": "staging",
                "table": "transaction_historicaltransactionrequest",
            },
        },
        {
            "src": {"schema": "cg-lms", "table": "transaction_manualfee"},
            "dst": {"schema": "staging", "table": "transaction_manualfee"},
        },
        {
            "src": {"schema": "cg-lms", "table": "transaction_manualrepayment"},
            "dst": {"schema": "staging", "table": "transaction_manualrepayment"},
        },
        {
            "src": {"schema": "cg-lms", "table": "transaction_nsf"},
            "dst": {"schema": "staging", "table": "transaction_nsf"},
        },
        {
            "src": {"schema": "cg-lms", "table": "transaction_offlinerepayment"},
            "dst": {"schema": "staging", "table": "transaction_offlinerepayment"},
        },
        {
            "src": {"schema": "cg-lms", "table": "transaction_transaction"},
            "dst": {"schema": "staging", "table": "transaction_transaction"},
        },
        {
            "src": {"schema": "cg-lms", "table": "transaction_transactionbucket"},
            "dst": {"schema": "staging", "table": "transaction_transactionbucket"},
        },
        {
            "src": {"schema": "cg-lms", "table": "transaction_transactioncategory"},
            "dst": {"schema": "staging", "table": "transaction_transactioncategory"},
        },
        {
            "src": {"schema": "cg-lms", "table": "transaction_transactionrequest"},
            "dst": {"schema": "staging", "table": "transaction_transactionrequest"},
        },
    ],
)
