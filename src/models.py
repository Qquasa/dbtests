from sqlalchemy import Table, Column, String, Integer, MetaData

metadata_obj = MetaData()

worker_table = Table(
    "worker",
    metadata_obj,
    Column('id', Integer, primary_key=True),
    Column('username', String),
)