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
customer_price/coalesce(ber.rate, lag(ber.rate,1) ignore nulls over (partition by currency order by date((TIMESTAMP 'epoch' + ber.timestamp * INTERVAL '1 Second ')) )) as eur_amount,
subscription,
period
from itunes_raw_first_half fh
left join bs_exchange_rates ber
on date((TIMESTAMP 'epoch' + ber.timestamp * INTERVAL '1 Second ')) = to_date((substring(fh.begin_date, 7,5)||'-' ||substring(fh.begin_date,0,3)||'-'||substring(fh.begin_date, 4,2)),'YYYY-MM-DD')
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
customer_price/coalesce(ber.rate, lag(ber.rate,1) ignore nulls over (partition by currency order by date((TIMESTAMP 'epoch' + ber.timestamp * INTERVAL '1 Second ')) )) as eur_amount,
subscription,
period
from itunes_raw_second_half sh
left join bs_exchange_rates ber
on date((TIMESTAMP 'epoch' + ber.timestamp * INTERVAL '1 Second ')) = to_date((substring(sh.begin_date, 7,5)||'-' ||substring(sh.begin_date,0,3)||'-'||substring(sh.begin_date, 4,2)),'YYYY-MM-DD')
and ber.currency = sh.customer_currency
;