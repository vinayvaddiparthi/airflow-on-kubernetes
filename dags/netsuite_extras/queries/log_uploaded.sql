merge into erp.{{env}}.tli_uploaded t2 using (
    select tli.*,
       status.je_internal_id as journal_entry_internal_id
    from erp.{{env}}.tli_balanced tli
        left join erp.{{env}}.journal_entry_status status
    on to_date(status.created_date) = to_date(tli.created_date) and status.transaction_date = tli.tran_date and status.subsidiary_id = tli.subsidiary_id
    where status.uploaded = 'TRUE'
    ) t1
    on t1.id = t2.id
    when not matched then
insert (id,
        tran_id,
        new_gl,
        old_gl,
        account_internal_id,
        subsidiary,
        subsidiary_id,
        document_description,
        credit,
        debit,
        tran_date,
        created_date,
        journal_entry_internal_id)
values (t1.id,
    t1.tran_id,
    t1.new_gl,
    t1.old_gl,
    t1.account_internal_id,
    t1.subsidiary,
    t1.subsidiary_id,
    t1.document_description,
    t1.credit,
    t1.debit,
    t1.tran_date,
    t1.created_date,
    t1.journal_entry_internal_id);