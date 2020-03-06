select to_char(tli.tran_date),
       tli.old_gl,
       tli.account_internal_id,
       tli.subsidiary_id,
       sum(tli.credit) as credit,
       sum(tli.debit)  as debit
from erp.{{env}}.tli_balanced tli
where tli.old_gl is not null
group by 1, 4, 2, 3;