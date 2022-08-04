create table if not exists {{params.table}}(
    id varchar(255),
    guid varchar(255),
    default_flag boolean,
    bankruptcy boolean,
    outstanding_balance float,
    calculated_dpd varchar(255),
    provision_rate float,
    provision_loss float,
    created_datetime_UTC timestamp
);
grant select on {{ params.table }} to role snowflake_non_pii_ro;
grant select on {{ params.table }} to role snowflake_pii_ro;
grant select on {{ params.table }} to role looker_role;

insert into {{ params.table }}
select
$1 as id,
$2 as guid,
$3 as default_flag,
$4 as bankruptcy,
$5 as outstanding_balance,
$6 as calculated_dpd,
$7 as provision_rate,
$8 as provision_loss,
CURRENT_TIMESTAMP()
from @{{ params.stage }};

select count(*) from {{params.table}}
