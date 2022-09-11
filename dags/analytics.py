from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import JSON
from sqlalchemy.sql import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy import cast
from sqlalchemy.types import Float, String
from sqlalchemy import (Column, Float, Integer, MetaData, String, Table,
                        create_engine)
from os import environ
from time import sleep

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

print("Waiting for the data generator...")
sleep(1)
print("ETL Starting...")

while True:
    try:
        psql_engine = create_engine(
            environ["POSTGRESQL_CS"],
            pool_pre_ping=True,
            pool_size=10,  # echo=True,
        )
        break
    except OperationalError:
        sleep(0.1)
print("Connection to PostgresSQL successful.")
print(environ["POSTGRESQL_CS"])

# Write the solution here
print("Connection to MUUUU successful.")

metadata_obj = MetaData()
devices = Table(
    "devices",
    metadata_obj,
    Column("device_id", String),
    Column("temperature", Integer),
    Column("location", String),
    Column("time", String),
)


Session = sessionmaker(bind=psql_engine)
session = Session()

################################

subquery_location = session.query(
    devices.c.device_id,
    devices.c.time,
    devices.c.temperature,
    (cast(devices.c.location, JSON)).label("location_json"),
    (cast(devices.c.time, Integer) / 3600).label("hour"),
).subquery()

subquery_location_lag = (
    session.query(
        subquery_location.c.device_id,
        subquery_location.c.time,
        subquery_location.c.hour,
        subquery_location.c.temperature,
        subquery_location.c.location_json["latitude"].as_float().label("lat1"),
        subquery_location.c.location_json["longitude"].as_float().label(
            "lon1"),
        func.lag(subquery_location.c.location_json["latitude"].as_float())
        .over(
            partition_by=(subquery_location.c.hour,
                          subquery_location.c.device_id),
            order_by=subquery_location.c.time,
        )
        .label("lat2"),
        func.lag(subquery_location.c.location_json["longitude"].as_float())
        .over(
            partition_by=(subquery_location.c.hour,
                          subquery_location.c.device_id),
            order_by=subquery_location.c.time,
        )
        .label("lon2"),
    )
    .order_by(subquery_location.c.device_id, subquery_location.c.time)
    .subquery()
)

subquery_distance = (
    session.query(
        subquery_location_lag.c.device_id,
        # subquery_location_lag.c.time,
        subquery_location_lag.c.hour,
        func.count("*").label("data_point_pre_hour"),
        func.max(subquery_location_lag.c.temperature).label("max_temperature"),
        (
            func.sum(
                6371.0
                * func.acos(
                    func.least(
                        1.0,
                        func.cos(func.radians(subquery_location_lag.c.lat1))
                        * func.cos(func.radians(subquery_location_lag.c.lat2))
                        * func.cos(
                            func.radians(
                                subquery_location_lag.c.lon1
                                - subquery_location_lag.c.lon2
                            )
                        )
                        + func.sin(func.radians(subquery_location_lag.c.lat1))
                        * func.sin(func.radians(subquery_location_lag.c.lat2)),
                    )
                )
            )
        ).label("total_distance_in_km_per_hour"),
    )
    .group_by(subquery_location_lag.c.device_id, subquery_location_lag.c.hour)
    .order_by(subquery_location_lag.c.hour, subquery_location_lag.c.device_id)
).subquery()


result2 = session.query(subquery_distance)
for row in list(result2):  # [0:5]:
    print(row)


###################
print('To SQL')
##################
print(environ["MYSQL_CS"])
while True:
    try:
        mysql_engine = create_engine(
            environ["MYSQL_CS"],
            pool_pre_ping=True,
            pool_size=10,  # echo=True,
        )
        metadata_obj_sql = MetaData()
        devices_agg = Table(
            "devices_agg",
            metadata_obj_sql,
            Column("device_id", String(60)),
            Column("hour", Integer),
            Column("data_point_pre_hour", Integer),
            Column("max_temperature", Integer),
            Column("total_distance_in_km_per_hour", Float),
        )
        metadata_obj_sql.create_all(mysql_engine)
        break
    except OperationalError:
        sleep(0.1)
print("Connection to MySQL successful.")
print(environ["MYSQL_CS"])


Session_sql = sessionmaker(bind=mysql_engine)
session_sql = Session_sql()

print(session_sql.query(devices_agg))


'''result2 = (
    ('2a400568-fb97-48f0-9ced-82dcbb67d3a9', 461900, 3257, 50, 32664970.3650984),
    ('d1e19d21-129a-4bf2-aba0-7a3c7ea8c259', 461900, 3257, 50, 32644730.4946857),
    ('fdb6e849-f412-4e93-be04-3a9669122570', 461900, 3257, 50, 32307617.9479189),
    ('2a400568-fb97-48f0-9ced-82dcbb67d3a9', 461901, 2984, 50, 30029555.858479),
    ('d1e19d21-129a-4bf2-aba0-7a3c7ea8c259', 461901, 2984, 50, 29614865.3377209),
    ('fdb6e849-f412-4e93-be04-3a9669122570', 461901, 2984, 50, 29503980.0607846),
)'''

all_values = list()
for row in list(result2):  # [0:5]:
    print(row)
    data = dict(device_id=row[0],
                hour=row[1],
                data_point_pre_hour=row[2],
                max_temperature=row[3],
                total_distance_in_km_per_hour=row[4],
                )
    all_values += [data]

conn = mysql_engine.connect()
result = conn.execute(devices_agg.insert(), all_values)
print(session_sql.query(devices_agg).all())


# drop Table


def drop_table(table_name, engine):
    Base = declarative_base()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = metadata.tables[table_name]
    if table is not None:
        Base.metadata.drop_all(engine, [table], checkfirst=True)


drop_table('devices_agg', engine=mysql_engine)

# devices_agg.drop(mysql_engine)
# devices_agg.commit()
