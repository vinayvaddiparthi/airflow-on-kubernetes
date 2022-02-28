copy into {table} 
(id,file_id,merchant_guid,batch_timestamp,batch_balance,account_guid,account_category,account_currency,transaction_guid,rel_transaction_id,credit,debit,balance,date,description,predicted_category,is_nsd) 
from 
(
    select 
    $1:id::varchar,
    $1:file_id::varchar,
    $1:merchant_guid::varchar,
    $1:batch_timestamp::varchar,
    $1:batch_balance::varchar,
    $1:account_guid::varchar,
    $1:account_category::varchar,
    $1:account_currency::varchar,
    $1:transaction_guid::varchar,
    $1:rel_transaction_id::varchar,
    $1:credit::varchar,
    $1:debit::varchar,
    $1:balance::varchar,
    $1:date::varchar,
    $1:description::varchar,
    $1:predicted_category::varchar,
    $1:is_nsd::boolean 
    from @{stage}
)
file_format=(type=parquet compression=auto) on_error=abort_statement