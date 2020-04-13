select correlation_guid,
       posted_at,
       credit_amount,
       debit_amount,
       account_number,
       facility,
       ns_account_internal_id,
       ns_subsidiary_id
from "{{db_name}}".{{schema_name}}.fct_platform_erp_transactions
where to_date(created_at) = '{{created_at}}'