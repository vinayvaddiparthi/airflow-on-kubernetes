merge into erp.{{env}}.tli_error t2 using (
    select tli.*,
    status.error_msg
    from erp.{{env}}.tli_{{created_date_trim}} tli
        left join erp.test.journal_entry_status status
    on to_date(status.created_date) = to_date(tli.created_date) and status.transaction_date = tli.tran_date and status.subsidiary_id = tli.subsidiary_id
    where status.uploaded = 'FALSE'
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
        error_msg)
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
    t1.error_msg);