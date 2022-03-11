create table if not exists {{ params.sv_table }} (
	macro_industry varchar(255),
	province varchar(100),
	week date,
	avg_sales number(38,2),
	sample_size number(38,0),
	report_ts timestamp_ntz(9)
);

insert into {{ params.sv_table }} (
    macro_industry,
    province,
    week,
    avg_sales,
    sample_size,
    report_ts
)
with weekly_balances as (
    select * from {{ params.trx_table }}
),
merchant as (
    select * from {{ params.merchant_table }}
),
merchant_refined as (
    select 
    guid,
    upper(macro_industry)                       as macro_industry,
    case when not is_null_value(addresses:legal_business_address) then addresses:legal_business_address[0]:province::varchar
        when not is_null_value(addresses:operating_at_address) then addresses:operating_at_address[0]:province::varchar
        when not is_null_value(addresses:mailing_address) then addresses:mailing_address[0]:province::varchar
        else null 
    end                                         as address_province,
    iff((trim(incorporated_in) = '' or incorporated_in = 'CD' or incorporated_in = 'null'), 
            address_province, incorporated_in)  as province
    from merchant
    where macro_industry is not null
),
merchant_industry as (
    select
    wb.merchant_guid,
    wb.account_guid,
    wb.date,
    wb.credits,
    mr.macro_industry,
    mr.province
    from weekly_balances as wb
    inner join merchant_refined as mr 
    on wb.merchant_guid = mr.guid
),
merchant_industry_aggregated as (
    select
    macro_industry,
    province,
    last_day(date, 'week') - 6 as week,
    round(avg(credits),2) as avg_sales,
    count(*) as sample_size,
    current_timestamp() as report_ts
    from merchant_industry
    group by macro_industry, province, week
    order by week desc, macro_industry, province
)
select
macro_industry,
province,
week,
avg_sales,
sample_size,
report_ts
from merchant_industry_aggregated

