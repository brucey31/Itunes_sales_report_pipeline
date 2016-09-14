__author__ = 'brucepannaman'

import os
import boto
import configparser
from datetime import date, timedelta
from subprocess import call, check_output
import psycopg2



config = configparser.ConfigParser()
ini = config.read('conf2.ini')


AWS_ACCESS_KEY_ID = config.get('AWS Credentials', 'key')
AWS_SECRET_ACCESS_KEY = config.get('AWS Credentials', 'secret')
RED_HOST = config.get('Redshift Creds', 'host')
RED_PORT = config.get('Redshift Creds', 'port')
RED_USER = config.get('Redshift Creds', 'user')
RED_PASSWORD = config.get('Redshift Creds', 'password')


ISR = "Itunes_sales_reports"


conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
bucket = conn.get_bucket('bibusuu')

start_date_1 = date.today() -timedelta(days=365)
# start_date_1 = date(2015,10,20)
end_date_1 = date(2015, 10, 25)

start_date_2 = date(2015, 10, 26)
end_date_2 = date(2016, 06, 13)

start_date_3 = date(2016, 6, 14)
end_date_3 = date(2016, 9, 11)

start_date_4 = date(2016, 9, 12)
end_date_4 = date.today() - timedelta(days=1)

print "Looking for Sales files in s3 not downloaded yet"

print "Finished processing Itunes Sales Data \n Now Importing into redshift"
    # Update files to redshift once completed

# Connect to RedShift
conn_string = "dbname=%s port=%s user=%s password=%s host=%s" %(RED_USER, RED_PORT, RED_USER, RED_PASSWORD, RED_HOST)
print "Connecting to database\n        ->%s" % (conn_string)
conn = psycopg2.connect(conn_string)

cursor = conn.cursor()

# Update the redshift table with the new results
print "Deleting old table Itunes_raw_2"
cursor.execute("drop table if exists ITunes_raw_2;")
print "Creating new table \n ITunes_raw_2 "
cursor.execute("Create table itunes_raw_2(Provider varchar(10),Provider_country varchar(5),SKU varchar(100),Developer varchar(200),Title varchar(200),Version varchar(10),Product_types varchar(20),Units int,Developer_proceeds decimal,Begin_date varchar(20),end_date varchar(20),Customer_currency varchar(5),Country_code varchar(5),Country_proceeds varchar(5),Apple_id varchar(40),customer_price decimal,Promo_code varchar(20),Parent_id int,Subscription varchar(20),Period varchar(20),Catagory varchar(15),CMB varchar(10));")
print "Copying ITunes TXT data from S3 to  \n ITunes_raw_2 "
cursor.execute("COPY itunes_raw_2  FROM 's3://bibusuu/Itunes_sales_reports/old_files/'  CREDENTIALS 'aws_access_key_id=AKIAITPOBFF7K7ZPLIRQ;aws_secret_access_key=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' DELIMITER '\t' IGNOREHEADER 1 GZIP;")
print "Deleting old table ITunes_raw_first_half"
cursor.execute("Drop Table if exists \n ITunes_raw_first_half")
print "Renaming table  \n ITunes_raw_2 \nto \n ITunes_raw_first_half"
cursor.execute("ALTER TABLE ITunes_raw_2 RENAME TO ITunes_raw_first_half")


conn.commit()
conn.close()


print "Finished processing Itunes sales data \n Now Importing into redshift"

# Update files to redshift once completed

# Connect to RedShift
conn_string = "dbname=%s port=%s user=%s password=%s host=%s" %(RED_USER, RED_PORT, RED_USER, RED_PASSWORD, RED_HOST)
print "Connecting to database\n        ->%s" % (conn_string)
conn = psycopg2.connect(conn_string)

cursor = conn.cursor()

# Update the redshift table with the new results
print "Deleting old table Itunes_raw_2"
cursor.execute("drop table if exists ITunes_raw_2;")
print "Creating new table \n ITunes_raw_2 "
cursor.execute("Create table itunes_raw_2(Provider varchar(10),Provider_country varchar(5),SKU varchar(100),Developer varchar(200),Title varchar(200),Version varchar(10),Product_types varchar(20),Units int,Developer_proceeds decimal,Begin_date varchar(20),end_date varchar(20),Customer_currency varchar(5),Country_code varchar(5),Country_proceeds varchar(5),Apple_id varchar(40),customer_price decimal,Promo_code varchar(20),Parent_id int,Subscription varchar(20),Period varchar(20),Catagory varchar(15),CMB varchar(10),Device varchar(10),Supported_platforms varchar(15));")
print "Copying ITunes TXT data from S3 to  \n ITunes_raw_2 "
cursor.execute("COPY itunes_raw_2  FROM 's3://bibusuu/Itunes_sales_reports/new_files/'  CREDENTIALS 'aws_access_key_id=AKIAITPOBFF7K7ZPLIRQ;aws_secret_access_key=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' DELIMITER '\t' IGNOREHEADER 1 GZIP;")
print "Deleting old table ITunes_raw_second_half"
cursor.execute("Drop Table if exists \n ITunes_raw_second_half")
print "Renaming table  \n ITunes_raw_2 \nto \n ITunes_second_half"
cursor.execute("ALTER TABLE ITunes_raw_2 RENAME TO ITunes_raw_second_half")

conn.commit()
conn.close()

while start_date_3 <= end_date_3:

    rs = check_output(["s3cmd", "ls", "s3://bibusuu/Itunes_sales_reports/new_files_2/%s" % start_date_3.strftime("%Y%m%d")])

    if len(rs) > 1:
         print "File Exists for %s \n Moving on ;-)" % start_date_3

    else:

        print "Downloading ITunes sales report for %s" % start_date_3
        call(["java", "Autoingestion", "autoingestion.properties", "80082574", "Sales", "Daily", "Summary", "%s" % start_date_3.strftime("%Y%m%d")])
        if os.path.isfile("S_D_80082574_%s.txt.gz" % start_date_3.strftime("%Y%m%d")):
            print "Uploading ITunes sales report for %s" % start_date_3
            call(["s3cmd", "put", "S_D_80082574_%s.txt.gz" % start_date_3.strftime("%Y%m%d"), "s3://bibusuu/Itunes_sales_reports/new_files_2/%s/S_D_80082574_%s.txt.gz" % (start_date_3.strftime("%Y%m%d"), start_date_3)])
            print "Removing local file for %s" % start_date_3
            os.remove("S_D_80082574_%s.txt.gz" % start_date_3.strftime("%Y%m%d"))

        else:
            print 'File for %s is not available from the Itunes Store yet' % start_date_3



    start_date_3 = start_date_3 + timedelta(days=1)

while start_date_4 <= end_date_4:

    rs = check_output(["s3cmd", "ls", "s3://bibusuu/Itunes_sales_reports/new_files_3/%s" % start_date_4.strftime("%Y%m%d")])

    if len(rs) > 1:
         print "File Exists for %s \n Moving on ;-)" % start_date_4

    else:

        print "Downloading ITunes sales report for %s" % start_date_4
        call(["java", "Autoingestion", "autoingestion.properties", "80082574", "Sales", "Daily", "Summary", "%s" % start_date_4.strftime("%Y%m%d")])
        if os.path.isfile("S_D_80082574_%s.txt.gz" % start_date_4.strftime("%Y%m%d")):
            print "Uploading ITunes sales report for %s" % start_date_4
            call(["s3cmd", "put", "S_D_80082574_%s.txt.gz" % start_date_4.strftime("%Y%m%d"), "s3://bibusuu/Itunes_sales_reports/new_files_3/%s/S_D_80082574_%s.txt.gz" % (start_date_4.strftime("%Y%m%d"), start_date_4)])
            print "Removing local file for %s" % start_date_4
            os.remove("S_D_80082574_%s.txt.gz" % start_date_4.strftime("%Y%m%d"))

        else:
            print 'File for %s is not available from the Itunes Store yet' % start_date_4



    start_date_4 = start_date_4 + timedelta(days=1)

print "Finished processing Itunes sales data \n Now Importing into redshift"

# Update files to redshift once completed

# Connect to RedShift
conn_string = "dbname=%s port=%s user=%s password=%s host=%s" %(RED_USER, RED_PORT, RED_USER, RED_PASSWORD, RED_HOST)
print "Connecting to database\n        ->%s" % (conn_string)
conn = psycopg2.connect(conn_string)

cursor = conn.cursor()

# Update the redshift table with the new results
print "Deleting old table Itunes_raw_2"
cursor.execute("drop table if exists ITunes_raw_2;")
print "Creating new table \n ITunes_raw_2 "
cursor.execute("Create table itunes_raw_2(Provider varchar(10),Provider_country varchar(5),SKU varchar(100),Developer varchar(200),Title varchar(200),Version varchar(10),Product_types varchar(20),Units int,Developer_proceeds decimal,Begin_date varchar(20),end_date varchar(20),Customer_currency varchar(5),Country_code varchar(5),Country_proceeds varchar(5),Apple_id varchar(40),customer_price decimal,Promo_code varchar(20),Parent_id int,Subscription varchar(20),Period varchar(20),Catagory varchar(15),CMB varchar(10),Device varchar(10),Supported_platforms varchar(15), proceeds_reason varchar(100));")
print "Copying ITunes TXT data from S3 to  \n ITunes_raw_2 "
cursor.execute("COPY itunes_raw_2  FROM 's3://bibusuu/Itunes_sales_reports/new_files_2/'  CREDENTIALS 'aws_access_key_id=AKIAITPOBFF7K7ZPLIRQ;aws_secret_access_key=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' DELIMITER '\t' IGNOREHEADER 1 GZIP;")
print "Deleting old table ITunes_raw_third_bit"
cursor.execute("Drop Table if exists \n ITunes_raw_third_bit")
print "Renaming table  \n ITunes_raw_2 \nto \n ITunes_raw_third_bit"
cursor.execute("ALTER TABLE ITunes_raw_2 RENAME TO ITunes_raw_third_bit")

print "Deleting old table Itunes_raw_2"
cursor.execute("drop table if exists ITunes_raw_2;")
print "Creating new table \n ITunes_raw_2 "
cursor.execute("Create table itunes_raw_2(Provider varchar(10),Provider_country varchar(5),SKU varchar(100),Developer varchar(200),Title varchar(200),Version varchar(10),Product_types varchar(20),Units int,Developer_proceeds decimal,Begin_date varchar(20),end_date varchar(20),Customer_currency varchar(5),Country_code varchar(5),Country_proceeds varchar(5),Apple_id varchar(40),customer_price decimal,Promo_code varchar(20),Parent_id int,Subscription varchar(20),Period varchar(20),Catagory varchar(15),CMB varchar(10),Device varchar(10),Supported_platforms varchar(15), proceeds_reason varchar(100), preserved_pricing varchar(250), client varchar(250));")
print "Copying ITunes TXT data from S3 to  \n ITunes_raw_2 "
cursor.execute("COPY itunes_raw_2  FROM 's3://bibusuu/Itunes_sales_reports/new_files_3/'  CREDENTIALS 'aws_access_key_id=AKIAITPOBFF7K7ZPLIRQ;aws_secret_access_key=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' DELIMITER '\t' IGNOREHEADER 1 GZIP;")
print "Deleting old table ITunes_raw_fourth_bit"
cursor.execute("Drop Table if exists \n ITunes_raw_fourth_bit")
print "Renaming table  \n ITunes_raw_2 \nto \n ITunes_raw_fourth_bit"
cursor.execute("ALTER TABLE ITunes_raw_2 RENAME TO ITunes_raw_fourth_bit")

print "Deleting old table ITunes_raw"
cursor.execute("Drop Table if exists ITunes_raw")
print "Aggregating all of the Itunes Sales Reports"
cursor.execute("create table itunes_raw as  select  distinct title,  version,  case when Product_types ='IAY' then 'In-App Purchase'when Product_types ='3F' then 'Re-Download'when Product_types ='3T' then 'IPad Re-Download'when Product_types ='1F' then 'Download'when Product_types ='7' then 'IPhone IPod Update'when Product_types ='1' then 'IPhone IPod Download'when Product_types ='IA3' then 'Restore In-App Purchase'when Product_types ='7F' then 'Update'when Product_types ='IA1' then 'In-App Purchase'when Product_types ='1T' then 'IPad Download'when Product_types ='7T' then 'IPad Update'else 'Unknown' end as Product_Type,  units,developer_proceeds,  to_date((substring(begin_date, 7,5)||'-' ||substring(begin_date,0,3)||'-'||substring(begin_date, 4,2)),'YYYY-MM-DD') as date,  customer_currency,  country_code,  apple_id,  customer_price,  units * (customer_price/coalesce(ber.rate, lag(ber.rate,1) ignore nulls over (partition by currency order by date((TIMESTAMP 'epoch' + ber.timestamp * INTERVAL '1 Second ')) ))) as eur_amount,  subscription,  period  from itunes_raw_first_half fh  left join bs_exchange_rates ber on date((TIMESTAMP 'epoch' + ber.timestamp * INTERVAL '1 Second ')) = to_date((substring(fh.begin_date, 7,5)||'-' ||substring(fh.begin_date,0,3)||'-'||substring(fh.begin_date, 4,2)),'YYYY-MM-DD') and ber.currency = fh.customer_currency  union all select distinct title, version, case when Product_types ='IAY' then 'In-App Purchase'when Product_types ='3F' then 'Re-Download'when Product_types ='3T' then 'IPad Re-Download'when Product_types ='1F' then 'Download'when Product_types ='7' then 'IPhone IPod Update'when Product_types ='1' then 'IPhone IPod Download'when Product_types ='IA3' then 'Restore In-App Purchase'when Product_types ='7F' then 'Update'when Product_types ='IA1' then 'In-App Purchase'when Product_types ='1T' then 'IPad Download'when Product_types ='7T' then 'IPad Update'else 'Unknown' end as Product_Type,  units,  developer_proceeds, to_date((substring(begin_date, 7,5)||'-' ||substring(begin_date,0,3)||'-'||substring(begin_date, 4,2)),'YYYY-MM-DD') as date, customer_currency, country_code, apple_id, customer_price, units * (customer_price/coalesce(ber.rate, lag(ber.rate,1) ignore nulls over (partition by currency order by date((TIMESTAMP 'epoch' + ber.timestamp * INTERVAL '1 Second ')) ))) as eur_amount, subscription, period from itunes_raw_second_half sh left join bs_exchange_rates ber on date((TIMESTAMP 'epoch' + ber.timestamp * INTERVAL '1 Second ')) = to_date((substring(sh.begin_date, 7,5)||'-' ||substring(sh.begin_date,0,3)||'-'||substring(sh.begin_date, 4,2)),'YYYY-MM-DD') and ber.currency = sh.customer_currency  union all  select distinct title, version, case when Product_types ='IAY' then 'In-App Purchase'when Product_types ='3F' then 'Re-Download'when Product_types ='3T' then 'IPad Re-Download'when Product_types ='1F' then 'Download'when Product_types ='7' then 'IPhone IPod Update'when Product_types ='1' then 'IPhone IPod Download'when Product_types ='IA3' then 'Restore In-App Purchase'when Product_types ='7F' then 'Update'when Product_types ='IA1' then 'In-App Purchase'when Product_types ='1T' then 'IPad Download'when Product_types ='7T' then 'IPad Update'else 'Unknown' end as Product_Type,  units,  developer_proceeds, to_date((substring(begin_date, 7,5)||'-' ||substring(begin_date,0,3)||'-'||substring(begin_date, 4,2)),'YYYY-MM-DD') as date, customer_currency, country_code, apple_id, customer_price, units * (customer_price/coalesce(ber.rate, lag(ber.rate,1) ignore nulls over (partition by currency order by date((TIMESTAMP 'epoch' + ber.timestamp * INTERVAL '1 Second ')) ))) as eur_amount, subscription, period from ITunes_raw_third_bit sh left join bs_exchange_rates ber on date((TIMESTAMP 'epoch' + ber.timestamp * INTERVAL '1 Second ')) = to_date((substring(sh.begin_date, 7,5)||'-' ||substring(sh.begin_date,0,3)||'-'||substring(sh.begin_date, 4,2)),'YYYY-MM-DD') and ber.currency = sh.customer_currency union all select distinct title, version, case when Product_types ='IAY' then 'In-App Purchase'when Product_types ='3F' then 'Re-Download'when Product_types ='3T' then 'IPad Re-Download'when Product_types ='1F' then 'Download'when Product_types ='7' then 'IPhone IPod Update'when Product_types ='1' then 'IPhone IPod Download'when Product_types ='IA3' then 'Restore In-App Purchase'when Product_types ='7F' then 'Update'when Product_types ='IA1' then 'In-App Purchase'when Product_types ='1T' then 'IPad Download'when Product_types ='7T' then 'IPad Update'else 'Unknown' end as Product_Type,  units,  developer_proceeds, to_date((substring(begin_date, 7,5)||'-' ||substring(begin_date,0,3)||'-'||substring(begin_date, 4,2)),'YYYY-MM-DD') as date, customer_currency, country_code, apple_id, customer_price, units * (customer_price/coalesce(ber.rate, lag(ber.rate,1) ignore nulls over (partition by currency order by date((TIMESTAMP 'epoch' + ber.timestamp * INTERVAL '1 Second ')) ))) as eur_amount, subscription, period from ITunes_raw_fourth_bit sh left join bs_exchange_rates ber on date((TIMESTAMP 'epoch' + ber.timestamp * INTERVAL '1 Second ')) = to_date((substring(sh.begin_date, 7,5)||'-' ||substring(sh.begin_date,0,3)||'-'||substring(sh.begin_date, 4,2)),'YYYY-MM-DD') and ber.currency = sh.customer_currency ;")
print "Deleting old table ITunes_raw_first_half"
cursor.execute("Drop Table if exists  ITunes_raw_first_half")
print "Deleting old table ITunes_raw_second_half"
cursor.execute("Drop Table if exists ITunes_raw_second_half")
print "Deleting old table ITunes_raw_third_bit"
cursor.execute("Drop Table if exists ITunes_raw_third_bit")
print "Deleting old table ITunes_raw_fourth_bit"
cursor.execute("Drop Table if exists ITunes_raw_fourth_bit")

print "Complete \nEnjoy your Itunes Data"

conn.commit()
conn.close()
