insert into erp.{{env}}.tli_unbalanced_historical
select '{{execition_time}}' as execition_time,
    t1.*
from erp.{{env}}.tli_unbalanced_{{created_date_trim}} t1;