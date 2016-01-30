create table itunes_raw as
select title, version, units, developer_proceeds, begin_date,
to_date((substring(begin_date, 7,5)||'-' ||substring(begin_date,0,3)||'-'||substring(begin_date, 4,2)),'YYYY-MM-DD') as date,
customer_currency,
country_code,
apple_id,
customer_price,
subscription,
period
from itunes_raw_first_half

union all

select title, version, units, developer_proceeds, begin_date,
to_date((substring(begin_date, 7,5)||'-' ||substring(begin_date,0,3)||'-'||substring(begin_date, 4,2)),'YYYY-MM-DD') as date,
customer_currency,
country_code,
apple_id,
customer_price,
subscription,
period
from itunes_raw_second_half ;