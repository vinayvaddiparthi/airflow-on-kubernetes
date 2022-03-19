create table if not exists {{ params.sv_table }} (
	macro_industry varchar(255),
	province varchar(100),
	month date,
	avg_sales number(38,2),
    growth_rate number(38,5),
	sample_size number(38,0),
	report_ts timestamp_ntz(9)
);

delete from {{ params.sv_table }}
where date(report_ts) = current_date();

insert into {{ params.sv_table }} (
    macro_industry,
    province,
    month,
    avg_sales,
    growth_rate,
    sample_size,
    report_ts
)
with monthly_balances as (
    select
    merchant_guid,
    account_guid,
    date_trunc('month', date) as month,
    coalesce(sum(credit),0)   as monthly_sales_volume
    from {{ params.trx_table }}
    where is_nsd = False
    group by merchant_guid, account_guid, month
    order by month desc
),
merchant as (
    select 
    guid,
    upper(macro_industry)   as macro_industry,
    'CD'                    as province
    from {{ params.merchant_table }}
    where macro_industry is not null
),
merchant_industry as (
    select
    mb.merchant_guid,
    mb.account_guid,
    mb.month,
    mb.monthly_sales_volume,
    m.macro_industry,
    m.province
    from monthly_balances as mb
    inner join merchant as m
        on mb.merchant_guid = m.guid
),
merchant_industry_aggregated as (
    select
    macro_industry,
    province,
    month,
    round(avg(monthly_sales_volume),2)  as avg_sales,
    count(*)                            as sample_size,
    current_timestamp()                 as report_ts
    from merchant_industry
    group by macro_industry, province, month
),
benchmarks_with_growth_rate as (
    select
    macro_industry,
    province,
    month,
    avg_sales,
    lag(avg_sales, 1, 0) over (partition by macro_industry order by month) 
        as avg_sales_prev_month,
    case when avg_sales_prev_month = 0 then 0 
         else avg_sales/avg_sales_prev_month - 1 
    end as growth_rate,
    sample_size,
    report_ts
    from merchant_industry_aggregated
)
select
macro_industry,
province,
month,
avg_sales,
growth_rate,
sample_size,
report_ts
from benchmarks_with_growth_rate