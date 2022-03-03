create table if not exists {{ params.sv_table }} (
	macro_industry varchar(255),
	province varchar(100),
	starting_day_of_week date,
	median_weekly_sales_volume number(38,2),
	sample_size number(38,0),
	report_ts timestamp_ntz(9)
);

delete from {{ params.sv_table }}
where date(report_ts) = current_date();


insert into {{ params.sv_table }} (
    macro_industry,
    province,
    starting_day_of_week,
    median_weekly_sales_volume,
    sample_size,
    report_ts
)
with weekly_balances as (
    select
    merchant_guid,
    account_guid,
    last_day(date, 'week') - 6 as starting_day_of_week,
    coalesce(sum(credit),0) as weekly_sales_volume
    from {{ params.trx_table }}
    where is_nsd = False
    group by merchant_guid, account_guid, starting_day_of_week
    order by starting_day_of_week desc
),
merchant as (
    select * from {{ params.merchant_table }}
),
merchant_refined as (
    select 
    guid,
    initcap(replace(macro_industry, '_', ' '))  as macro_industry,
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
    wb.starting_day_of_week,
    wb.weekly_sales_volume,
    mr.macro_industry,
    mr.province
    from weekly_balances  as wb
    join merchant_refined as mr
        on wb.merchant_guid = mr.guid
),
merchant_industry_aggregated as (
    select
    macro_industry,
    province,
    starting_day_of_week,
    round(median(weekly_sales_volume),2) as median_weekly_sales_volume,
    count(*) as sample_size,
    current_timestamp() as report_ts
    from merchant_industry
    group by macro_industry, province, starting_day_of_week
    order by starting_day_of_week desc, macro_industry, province
)
select
macro_industry,
province,
starting_day_of_week,
median_weekly_sales_volume,
sample_size,
report_ts
from merchant_industry_aggregated