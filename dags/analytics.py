from etlpsql import etl_psql, create_psql_engine
from etlmysql import create_mysql_engine, mysql_export
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from os import environ
from time import sleep

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

print("Waiting for the data generator...")
sleep(1)
print("ETL Starting...")

# Write the solution here


################################

devices, psql_engine = create_psql_engine()

Session = sessionmaker(bind=psql_engine)
session = Session()

etl_subquery = etl_psql(devices, session)

result2 = session.query(etl_subquery)
for row in list(result2):  # [0:5]:
    print(row)

##################################
print('To SQL')
#################################

devices_agg, mysql_engine = create_mysql_engine(
    drop_if_exist=1)


mysql_export(result2, devices_agg, mysql_engine)

Session_sql = sessionmaker(bind=mysql_engine)
session_sql = Session_sql()

for row in list(session_sql.query(devices_agg).all()):  # [0:5]:
    print(row)
