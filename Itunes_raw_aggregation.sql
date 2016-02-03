create table itunes_raw as
select title,
version,
units,
developer_proceeds,
to_date((substring(begin_date, 7,5)||'-' ||substring(begin_date,0,3)||'-'||substring(begin_date, 4,2)),'YYYY-MM-DD') as date,
customer_currency,
country_code,
apple_id,
customer_price,
customer_price/ber.rate as eur_amount,
subscription,
period
from itunes_raw_first_half fh
left join bs_exchange_rates ber
on date_trunc('week',date((TIMESTAMP 'epoch' + ber.timestamp * INTERVAL '1 Second '))) = date_trunc('week',to_date((substring(fh.begin_date, 7,5)||'-' ||substring(fh.begin_date,0,3)||'-'||substring(fh.begin_date, 4,2)),'YYYY-MM-DD'))
and ber.currency = fh.customer_currency

union all

select title,
 version,
 units,
 developer_proceeds,
to_date((substring(begin_date, 7,5)||'-' ||substring(begin_date,0,3)||'-'||substring(begin_date, 4,2)),'YYYY-MM-DD') as date,
customer_currency,
country_code,
apple_id,
customer_price,
customer_price/ber.rate as eur_amount,
subscription,
period
from itunes_raw_second_half sh
left join bs_exchange_rates ber
on date_trunc('week',date((TIMESTAMP 'epoch' + ber.timestamp * INTERVAL '1 Second '))) = date_trunc('week',to_date((substring(fh.begin_date, 7,5)||'-' ||substring(fh.begin_date,0,3)||'-'||substring(fh.begin_date, 4,2)),'YYYY-MM-DD'))
and ber.currency = sh.customer_currency
;