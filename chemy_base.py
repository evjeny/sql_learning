import os
import sqlalchemy
from sqlalchemy import (
    MetaData, Table, Column,
    Integer, Text, TIMESTAMP,
)

metadata_obj = MetaData()

users_table = Table(
    "users",
    metadata_obj,
    Column("id", Integer(), primary_key=True),
    Column("name", Text()),
    Column("age", Integer()),
)

services_table = Table(
    "services",
    metadata_obj,
    Column("id", Integer(), primary_key=True),
    Column("name", Text()),
    Column("description", Text())
)

usages_table = Table(
    "usages",
    metadata_obj,
    Column("id", Integer(), primary_key=True),
    Column("user_id", Integer()),
    Column("service_id", Integer()),
    Column("timestamp", TIMESTAMP())
)

week_numbers_table = Table(
    "week_numbers",
    metadata_obj,
    Column("week_num", Integer(), primary_key=True)
)

sales_table = Table(
    "sales",
    metadata_obj,
    Column("id", Integer(), primary_key=True),
    Column("week_begin", Integer()),
    Column("week_end", Integer()),
    Column("sale", Integer())
)

def get_engine() -> sqlalchemy.Engine:
    SQL_CONNECTION_STRING = f"sqlite:///{os.getcwd()}/service_usage.db"
    print(SQL_CONNECTION_STRING)

    return sqlalchemy.create_engine(SQL_CONNECTION_STRING, echo=False)
