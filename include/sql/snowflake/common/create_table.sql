create or replace table {{ params.table_name }} as
    select $1 as fields from @{{ params.stage_name }};