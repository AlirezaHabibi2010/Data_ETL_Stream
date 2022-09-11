from time import sleep

from etlmysql import create_mysql_engine, mysql_load
from etlpsql import create_psql_engine, etl_psql, psql_obj
from sqlalchemy.orm import sessionmaker

print("Waiting for the data generator...")
sleep(20)
print("ETL Starting...")

##########################################################
# Extract and Transofrom data #############################
# Create the psql engine and make a session
psql_engine = create_psql_engine()

# device metadata Table in psql
devices = psql_obj()

Session = sessionmaker(bind=psql_engine)
session = Session()

# create subquery for extract and transform the data
# etl_psql aggegate max temperature, count of data points and sum of the distance
# for each device per each hour
etl_subquery = etl_psql(devices, session)

# query the etl_subquary and recive the data
data_agg = session.query(etl_subquery)

# print the agg data
if True:
    for row in list(data_agg):  # [0:5]:
        print(row)

##########################################################
# Export to MySQL #########################################

# Create mySQL engine and Table
devices_agg, mysql_engine = create_mysql_engine(drop_if_exist=True)

# Load data_agg to Table ('devices_agg')
mysql_load(data_agg, devices_agg, mysql_engine)

# print the 'devices_agg' table from MySQL
if True:
    Session_sql = sessionmaker(bind=mysql_engine)
    session_sql = Session_sql()
    for row in list(session_sql.query(devices_agg).all()):  # [0:5]:
        print(row)

print("ETL finished successfully.")
