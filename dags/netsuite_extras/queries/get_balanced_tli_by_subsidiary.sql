insert into erp.{{env}}.tli_balanced
select *
from erp.{{env}}.tli_raw
    qualify round((sum(credit+debit) over (partition by tran_id, subsidiary_id)), 2) = 0
order by tran_id, subsidiary_id