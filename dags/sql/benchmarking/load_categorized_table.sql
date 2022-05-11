copy into {table} 
(
    id,
    file_id,
    merchant_guid,
    batch_timestamp,
    batch_balance,
    account_guid,
    account_category,
    account_currency,
    transaction_guid,
    rel_transaction_id,
    credit,
    debit,
    balance,
    date,
    description,
    predicted_category,
    is_nsd,
    processed_credit
) 
from 
(
    select 
    $1:id::varchar,
    $1:file_id::varchar,
    $1:merchant_guid::varchar,
    try_to_timestamp($1:batch_timestamp::varchar),
    $1:batch_balance::number(38,2),
    $1:account_guid::varchar,
    $1:account_category::varchar,
    $1:account_currency::varchar,
    $1:transaction_guid::varchar,
    $1:rel_transaction_id::varchar,
    $1:credit::number(38,2),
    $1:debit::number(38,2),
    $1:balance::number(38,2),
    try_to_date($1:date::varchar),
    $1:description::varchar,
    $1:predicted_category::varchar,
    $1:is_nsd::boolean ,
    $1:processed_credit::number(38,2)
    from @{stage}
)
file_format=(type=parquet compression=auto) on_error=abort_statement