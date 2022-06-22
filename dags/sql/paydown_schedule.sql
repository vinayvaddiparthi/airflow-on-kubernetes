create or replace table {{ params.table }} (
	guid varchar(255),
    repayment_date date,
    opening_balance number(38,2),
    repayment_amount number(38,2),
    interest number(38,2),
	principal number(38,2),
	closing_balance number(38,2)
);

grant select on {{ params.table }} to role snowflake_non_pii_ro;
grant select on {{ params.table }} to role snowflake_pii_ro;
grant select on {{ params.table }} to role looker_role;

insert into {{ params.table }}
select 
$1 as guid,
$2 as repayment_date,
$3 as opening_balance,
$4 as repayment_amount,
$5 as interest,
$6 as principal,
$7 as closing_balance
from @{{ params.stage }}