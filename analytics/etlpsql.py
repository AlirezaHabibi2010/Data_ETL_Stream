from os import environ
from time import sleep

from sqlalchemy import (JSON, Column, Integer, MetaData, String, Table, cast,
                        create_engine)
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import func
from sqlalchemy.types import String


def psql_obj():
    """
    Create Table Schema
    return:
        devices: device metadata Table in psql
    """
    print("Create PostgreSQL object")
    metadata_obj = MetaData()
    devices = Table(
        "devices",
        metadata_obj,
        Column("device_id", String),
        Column("temperature", Integer),
        Column("location", String),
        Column("time", String),
    )
    return devices


def create_psql_engine():
    """
    Launch the psql engine
    return:
        psql_engine: psql engine
    """
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

    return psql_engine


def etl_psql(devices, session):
    """
    Create the etl_psql subquery to aggregate max temperature, count of data points and sum of the distance
    for each device per each hour
    input:
        devices: sqlalchemy devices metadata
        session: sqlalchemy session
    output:
        subquery_distance_temp_count: sqlalchemy query
    """
    print("Create ETL PostgreSQL subquery")

    # Cast location to json - create hour column
    subquery_location = session.query(
        devices.c.device_id,
        devices.c.time,
        devices.c.temperature,
        (cast(devices.c.location, JSON)).label("location_json"),
        (cast(devices.c.time, Integer) / 3600).label("hour"),
    ).subquery()

    # Lag window for location
    subquery_location_lag = (
        session.query(
            subquery_location.c.device_id,
            subquery_location.c.time,
            subquery_location.c.hour,
            subquery_location.c.temperature,
            subquery_location.c.location_json["latitude"].as_float().label(
                "lat1"),
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

    # Aggregate max temprature, sum(distance), count(*) for each device per hour
    subquery_distance_temp_count = (
        session.query(
            subquery_location_lag.c.device_id,
            # subquery_location_lag.c.time,
            subquery_location_lag.c.hour,
            func.count("*").label("data_point_pre_hour"),
            func.max(subquery_location_lag.c.temperature).label(
                "max_temperature"),
            (
                func.sum(
                    6371.0
                    * func.acos(
                        func.least(
                            1.0,
                            func.cos(func.radians(
                                subquery_location_lag.c.lat1))
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

    return subquery_distance_temp_count
