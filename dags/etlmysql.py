from os import environ
from time import sleep

from sqlalchemy import (Column, Float, Integer, MetaData, String, Table,
                        create_engine)
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import Float, String


def mysql_obj():
    metadata_obj_sql = MetaData()
    devices_agg = Table(
        "devices_agg",
        metadata_obj_sql,
        Column("device_id", String(60), primary_key=True),
        Column("hour", Integer, primary_key=True),
        Column("data_point_pre_hour", Integer),
        Column("max_temperature", Integer),
        Column("total_distance_in_km_per_hour", Float),
    )
    return metadata_obj_sql, devices_agg


def drop_table(table_name, engine):
    Base = declarative_base()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    try:
        table = metadata.tables[table_name]
        if table is not None:
            Base.metadata.drop_all(engine, [table], checkfirst=True)
    except:
        pass


def create_mysql_engine(drop_if_exist=0):
    while True:
        try:
            mysql_engine = create_engine(
                environ["MYSQL_CS"],
                pool_pre_ping=True,
                pool_size=10,  # echo=True,
            )
            if drop_if_exist:
                drop_table('devices_agg', engine=mysql_engine)

            metadata_obj_sql, devices_agg = mysql_obj()

            metadata_obj_sql.create_all(mysql_engine)
            break
        except OperationalError:
            sleep(0.1)
    print("Connection to MySQL successful.")
    print(environ["MYSQL_CS"])
    return devices_agg, mysql_engine


def mysql_export(data, devices_agg, mysql_engine):

    all_values = list()
    for row in list(data):  # [0:5]:
        print(row)
        row_dict = dict(device_id=row[0],
                        hour=row[1],
                        data_point_pre_hour=row[2],
                        max_temperature=row[3],
                        total_distance_in_km_per_hour=row[4],
                        )
        all_values += [row_dict]

    conn = mysql_engine.connect()
    conn.execute(devices_agg.insert(), all_values)
