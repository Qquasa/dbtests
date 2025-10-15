from sqlalchemy import text, insert, select, update, delete

from database import sync_engine, async_engine, session_factory, Base
from models import metadata_obj, WorkerORM

class SyncORM:
    @staticmethod
    def create_tables():
        sync_engine.echo=False
        Base.metadata.drop_all(sync_engine) #use date from class Base
        Base.metadata.create_all(sync_engine)
        sync_engine.echo=True

    @staticmethod
    def insert_data():
        with session_factory() as session:
            worker_bobr = WorkerORM(username="Bobr")
            worker_wolk = WorkerORM(username="Wolk")
            session.add_all([worker_bobr, worker_wolk])
            session.flush()
            session.commit()
    
    @staticmethod
    def select_worker():
        with session_factory() as session:
            #  worker_id = 1
            #  worker_wolk = session.get(WorkerORM, worker_id)
             query = select(WorkerORM)
             result = session.execute(query)
             workers = result.scalars().all()
             print(f"{workers=}")

    @staticmethod
    def update_worker(worker_id: int = 2, new_username: str = "Duck"):
        with session_factory() as session:
            worker_wolk = session.get(WorkerORM, worker_id)
            if worker_wolk is None:
                print(f"Работник с ID {worker_id} не найден.")
                return
            worker_wolk.username = new_username
            session.refresh(worker_wolk)
            session.commit()