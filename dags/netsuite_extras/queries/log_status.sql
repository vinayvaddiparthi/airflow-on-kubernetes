insert into erp.{{env}}.journal_entry_status
values ('{{je_internal_id}}',
        '{{error_msg}}',
        '{{created_date}}',
        '{{transaction_date}}',
        '{{execution_time}}',
        '{{uploaded}}',
        '{{subsidiary_id}}');