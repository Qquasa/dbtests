from sqlalchemy import text, insert

from database import sync_engine, async_engine, sesison_factory, Base
from models import metadata_obj, WorkerORM

def create_tables():
    sync_engine.echo=False
    Base.metadata.drop_all(sync_engine) #use date from class Base
    Base.metadata.create_all(sync_engine)
    sync_engine.echo=True

def insert_data():
    with sesison_factory() as session:
        worker_bobr = WorkerORM(username="Bobr")
        worker_wolk = WorkerORM(username="Wolk")
        session.add_all([worker_bobr, worker_wolk])
        session.commit()