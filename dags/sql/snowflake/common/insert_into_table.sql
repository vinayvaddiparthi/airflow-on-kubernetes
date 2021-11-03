insert into {{ params.table_name }}
    select * from {{ params.source_table_name }};