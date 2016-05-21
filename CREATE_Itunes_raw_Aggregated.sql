create table itunes_raw as
select distinct title,
version,
case
when Product_types ='IAY' then 'In-App Purchase'
when Product_types ='3F' then 'Re-Download'
when Product_types ='3T' then 'IPad Re-Download'
when Product_types ='1F' then 'Download'
when Product_types ='7' then 'IPhone IPod Update'
when Product_types ='1' then 'IPhone IPod Download'
when Product_types ='IA3' then 'Restore In-App Purchase'
when Product_types ='7F' then 'Update'
when Product_types ='IA1' then 'In-App Purchase'
when Product_types ='1T' then 'IPad Download'
when Product_types ='7T' then 'IPad Update'
else 'Unknown' end as Product_Type,
units,
developer_proceeds,
to_date((substring(begin_date, 7,5)||'-' ||substring(begin_date,0,3)||'-'||substring(begin_date, 4,2)),'YYYY-MM-DD') as date,
customer_currency,
country_code,
apple_id,
customer_price,
units * (customer_price/coalesce(ber.rate, lag(ber.rate,1) ignore nulls over (partition by currency order by date((TIMESTAMP 'epoch' + ber.timestamp * INTERVAL '1 Second ')) ))) as eur_amount,
subscription,
period
from itunes_raw_first_half fh
left join bs_exchange_rates ber on date((TIMESTAMP 'epoch' + ber.timestamp * INTERVAL '1 Second ')) = to_date((substring(fh.begin_date, 7,5)||'-' ||substring(fh.begin_date,0,3)||'-'||substring(fh.begin_date, 4,2)),'YYYY-MM-DD') and ber.currency = fh.customer_currency

union all

select distinct title,
version,
case
when Product_types ='IAY' then 'In-App Purchase'
when Product_types ='3F' then 'Re-Download'
when Product_types ='3T' then 'IPad Re-Download'
when Product_types ='1F' then 'Download'
when Product_types ='7' then 'IPhone IPod Update'
when Product_types ='1' then 'IPhone IPod Download'
when Product_types ='IA3' then 'Restore In-App Purchase'
when Product_types ='7F' then 'Update'
when Product_types ='IA1' then 'In-App Purchase'
when Product_types ='1T' then 'IPad Download'
when Product_types ='7T' then 'IPad Update'
else 'Unknown' end as Product_Type,
units,
developer_proceeds,
to_date((substring(begin_date, 7,5)||'-' ||substring(begin_date,0,3)||'-'||substring(begin_date, 4,2)),'YYYY-MM-DD') as date,
customer_currency,
country_code,
apple_id,
customer_price,
units * (customer_price/coalesce(ber.rate, lag(ber.rate,1) ignore nulls over (partition by currency order by date((TIMESTAMP 'epoch' + ber.timestamp * INTERVAL '1 Second ')) ))) as eur_amount,
subscription,
period
from itunes_raw_second_half sh
left join bs_exchange_rates ber on date((TIMESTAMP 'epoch' + ber.timestamp * INTERVAL '1 Second ')) = to_date((substring(sh.begin_date, 7,5)||'-' ||substring(sh.begin_date,0,3)||'-'||substring(sh.begin_date, 4,2)),'YYYY-MM-DD') and ber.currency = sh.customer_currency ;