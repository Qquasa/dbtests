from sqlalchemy import text, insert, select, update, delete, func, cast, Integer, and_
from sqlalchemy.orm import aliased, joinedload, selectinload
from database import sync_engine, async_engine, session_factory, Base
from models import metadata_obj, WorkerORM, ResumeORM, Workload

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
    
    @staticmethod
    def insert_resume():
        with session_factory() as session:
            resume_bobr1 = ResumeORM(
                 title="Python Junior Developer", compensation=50000, workload=Workload.fulltime, worker_id=1)
            resume_bobr2 = ResumeORM(
                title="Python Разработчик", compensation=150000, workload=Workload.fulltime, worker_id=1)
            resume_wolk1 = ResumeORM(
                title="Python Data Engineer", compensation=250000, workload=Workload.parttime, worker_id=2)
            resume_wolk2 = ResumeORM(
                title="Data Scientist", compensation=300000, workload=Workload.fulltime, worker_id=2)
            session.add_all([resume_bobr1, resume_bobr2,
                             resume_wolk1, resume_wolk2])
            session.commit()

    @staticmethod
    def select_resume_avg_compensation(like_language: str = "Python"):
        """
        select workload, avg(compensation)::int as avg_compensation
        from resumes
        where title like '%Python%' and compensation > 40000
        group by workload
        having avg(compensation) > 70000
        """
        with session_factory() as session:
            query = (
                select(
                    ResumeORM.workload,
                    cast(func.avg(ResumeORM.compensation), Integer).label("avg_compensation"),
                )
                .filter(and_(
                    ResumeORM.title.contains(like_language),
                    ResumeORM.compensation > 40000,
                ))
                .group_by(ResumeORM.workload)
            )
        print(query.compile(compile_kwargs={'literal_binds': True}))
        res = session.execute(query)
        result = res.all()
        print(result)

    @staticmethod
    def insert_additional_resume():
        with session_factory() as session:
            worker = [
                {"username": "Fish"},
                {"username": "Cat"},
                {"username": "Dog"},
            ]
            resume = [
                {"title": "Python программист", "compensation": 60000, "workload": "fulltime", "worker_id": 3},
                {"title": "Machine Learning Engineer", "compensation": 70000, "workload": "parttime", "worker_id": 3},
                {"title": "Python Data Scientist", "compensation": 80000, "workload": "parttime", "worker_id": 4},
                {"title": "Python Analyst", "compensation": 90000, "workload": "fulltime", "worker_id": 4},
                {"title": "Python Junior Developer", "compensation": 100000, "workload": "fulltime", "worker_id": 5},
            ]
            insert_worker = insert(WorkerORM).values(worker)
            insert_resume = insert(ResumeORM).values(resume)
            session.execute(insert_worker)
            session.execute(insert_resume)
            session.commit()

    @staticmethod
    def join_cte_subquery_window_func():
        """
        WITH helper2 AS (
            SELECT *, compensation-avg_workload_compensation AS compensation_diff
            FROM 
            (SELECT
                w.id,
                w.username,
                r.compensation,
                r.workload,
                avg(r.compensation) OVER (PARTITION BY workload)::int AS avg_workload_compensation
            FROM resumes r
            JOIN workers w ON r.worker_id = w.id) helper1
        )
        SELECT * FROM helper2
        ORDER BY compensation_diff DESC;
        """
        with session_factory() as session:
            w = aliased(WorkerORM)
            r = aliased(ResumeORM)
            subq = (
                select(
                    r,
                    w,
                    func.avg(r.compensation).over(partition_by=r.workload).cast(Integer).label("avg_workload_compensation"),
                )
                # .select_from(r)
                .join(r, r.worker_id == w.id).subquery("helper1")
            )           
            cte = (
                select(
                    subq.c.worker_id,
                    subq.c.username,
                    subq.c.compensation,
                    subq.c.workload,
                    subq.c.avg_workload_compensation,
                    (subq.c.compensation - subq.c.avg_workload_compensation).label("compensation_diff"),
                )
                .cte("helper2")
            )
            query = (
                select(cte)
                .order_by(cte.c.compensation_diff.desc())
            )

            res = session.execute(query)
            result = res.all()
            print(f"{len(result)=}. {result=}")

    @staticmethod
    def select_workers_with_lazy_relationship():
        with session_factory() as session:
            query = (
                select(WorkerORM)
            )
            res = session.execute(query)
            result = res.scalars().all()

            result[0].resume

    # one to one, many to one   -   joinedload       
    @staticmethod
    def select_workers_with_joined_relationship():
        with session_factory() as session:
            query = (
                select(WorkerORM)
                .options(joinedload(WorkerORM.resume))
            )

            res = session.execute(query)
            result = res.unique().scalars().all() #.unique() -required

            result[0].resume
            
    # one to many, many to many    -    selctionload
    @staticmethod
    def select_workers_with_selectinload_relationship():
        with session_factory() as session:
            query = (
                select(WorkerORM)
                .options(selectinload(WorkerORM.resume))
            )

            res = session.execute(query)
            result = res.unique().scalars().all() #.unique() -required

            resume_1 = result[0].resume
            print(resume_1)