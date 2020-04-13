with account_facility as (
    select account.account_number   as account_number,
           account.id               as account_id,
           account.ubl_id           as ubl_id,
           ubl.bb_facility_id       as facility_id,
           transfer.created_at      as facility_transfered,
           transfer.new_facility_id as new_value,
           transfer.old_facility_id as old_value
    from analytics_production.dbt_ario.ledger_accounts account
             left join analytics_production.dbt_ario.facility_transfers transfer on account.ubl_id = transfer.ubl_id
             left join analytics_production.dbt_ario.lending_ubls ubl on ubl.id = account.ubl_id
),
     trx as (
         select transactions.correlation_guid,
                transactions.posted_at,
                transactions.credit_amount,
                transactions.debit_amount,
                account_facility.*,
                iff(account_facility.facility_transfered is not null,
                    iff(transactions.posted_at < account_facility.facility_transfered, account_facility.old_value,
                        account_facility.new_value), account_facility.facility_id) as facility
         from analytics_production.dbt_ario.ledger_transactions transactions
                  left join account_facility on transactions.account_id = account_facility.account_id
         where transactions.created_at like '{{created_at}}%'
     ),
     trx_fix as (
         select distinct trx.correlation_guid,
                         trx.posted_at,
                         trx.credit_amount,
                         trx.debit_amount,
                         trx.account_number,
                         nvl(trx.facility, max(trx.facility) over (partition by trx.CORRELATION_GUID)) as facility
         from trx
     )
select trx_fix.correlation_guid,
       trx_fix.posted_at,
       trx_fix.credit_amount,
       trx_fix.debit_amount,
       trx_fix.account_number,
       trx_fix.facility,
       map.ns_account_internal_id,
       map.ns_subsidiary_id
from trx_fix
         join erp.platform.account_mapping map
              on map.account_number = trx_fix.account_number and map.facility = trx_fix.facility
         left join erp.platform.uploaded uploaded
              on trx_fix.correlation_guid = uploaded.correlation_guid
where uploaded.correlation_guid is null