select to_char(tli.tran_date),
       tli.old_gl,
       tli.account_internal_id,
       tli.subsidiary_id,
       sum(tli.credit) as credit,
       sum(tli.debit)  as debit
from erp.{{env}}.tli_{{create_date_trim}} tli
left join erp.{{env}}.tli_unbalanced_{{create_date_trim}} unbalanced
on unbalanced.id = tli.id
where unbalanced.id is null
and tli.old_gl is not null
group by 1, 4, 2, 3;