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

# start_date = date(2015, 11, 14)cd
# end_date = date(2015, 11, 18)

start_date = date.today() -timedelta(days=20)
end_date = date.today() -timedelta(days=1)

print "Looking for Sales files in s3 not downloaded yet"

while start_date <= end_date:

    rs = check_output(["s3cmd", "ls", "s3://bibusuu/Itunes_sales_reports/%s" % start_date.strftime("%Y%m%d")])

    if len(rs) > 1:
         print "File Exists for %s \n Moving on ;-)" % start_date

    else:

        print "Downloading ITunes sales report for %s" % start_date
        call(["java", "Autoingestion", "autoingestion.properties", "80082574","Sales", "Daily", "Summary", "%s" % start_date.strftime("%Y%m%d")])
        print "Uploading ITunes sales report for %s" % start_date
        call(["s3cmd", "put", "S_D_80082574_%s.txt.gz" % start_date.strftime("%Y%m%d"), "s3://bibusuu/Itunes_sales_reports/%s/S_D_80082574_%s.txt.gz" % (start_date.strftime("%Y%m%d"),start_date)])
        print "Removing local file for %s" % start_date
        os.remove("S_D_80082574_%s.txt.gz" % start_date.strftime("%Y%m%d"))



    start_date = start_date + timedelta(days=1)

print "Finished processing LRS Data\nNow Importing into redshift"
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
cursor.execute("Create table itunes_raw_2(Provider varchar(10),Provider_country varchar(5),SKU varchar(50),Developer varchar(200),Title varchar(200),Version varchar(10),Product_types varchar(20),Units int,Developer_proceeds decimal,Begin_date varchar(20),end_date varchar(20),Customer_currency varchar(5),Country_code varchar(5),Country_proceeds varchar(5),Apple_id varchar(40),customer_price decimal,Promo_code varchar(20),Parent_id int,Subscription varchar(20),Period varchar(20),Catagory varchar(15),CMB varchar(10));")
print "Copying ITunes TXT data from S3 to  \n ITunes_raw_2 "
cursor.execute("COPY itunes_raw_2  FROM 's3://bibusuu/Itunes_sales_reports/'  CREDENTIALS 'aws_access_key_id=AKIAITPOBFF7K7ZPLIRQ;aws_secret_access_key=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' DELIMITER '\t' IGNOREHEADER 1 GZIP;")
print "Dropping Table  \n ITunes_raw "
cursor.execute("DROP TABLE ITunes_raw;")
print "Renaming table  \n ITunes_raw_2 \nto \n ITunes_raw"
cursor.execute("ALTER TABLE ITunes_raw_2 RENAME TO ITunes_raw")


conn.commit()
conn.close()
