create or replace table erp.{{env}}.tli_unbalanced_{{created_date_trim}} as
with t1 as (select tran_id,
                   subsidiary_id,
                   round((sum(credit) + sum(debit)), 2) as balance
            from erp.test.tli_{{created_date_trim}}
group by tran_id, subsidiary_id
order by tran_id, subsidiary_id)
select distinct tli.*
from erp.{{env}}.tli_{{created_date_trim}} tli
left join t1
on tli.tran_id = t1.tran_id and tli.subsidiary_id = t1.subsidiary_id
where t1.balance != 0
   or t1.subsidiary_id is null;