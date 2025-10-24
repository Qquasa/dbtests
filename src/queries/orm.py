from sqlalchemy import text, insert, select, update, delete, func, cast, Integer, and_
from sqlalchemy.orm import aliased, joinedload, selectinload, contains_eager
from database import sync_engine, async_engine, session_factory, async_session_factory, Base
from models import metadata_obj, WorkerORM, ResumeORM, Workload
from schemas import WorkerRelDTO

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

    @staticmethod
    def select_workers_with_joined_relationship():
            # one to one, many to one   -   joinedload   
        with session_factory() as session:
            query = (
                select(WorkerORM)
                .options(joinedload(WorkerORM.resume))
            )

            res = session.execute(query)
            result = res.unique().scalars().all() #.unique() -required

            result[0].resume
            
    @staticmethod
    def select_workers_with_selectinload_relationship():
            # one to many, many to many    -    selctionload
        with session_factory() as session:
            query = (
                select(WorkerORM)
                .options(selectinload(WorkerORM.resume))
            )

            res = session.execute(query)
            result = res.scalars().all() 

            resume_1 = result[0].resume
            print(resume_1)

    @staticmethod
    def select_workers_with_condition_releationship():
        with session_factory() as session:
            query = (
                select(WorkerORM)
                .options(selectinload(WorkerORM.resume_parttime))
            )
            res = session.execute(query)
            result = res.scalars().all()

            print(result)

    @staticmethod
    def select_workers_with_condition_releationship_contains_eager():
        with session_factory() as session:
            query = (
                select(WorkerORM)
                .join(WorkerORM.resume)
                .options(contains_eager(WorkerORM.resume))
                .filter(ResumeORM.workload == 'fultime')
            )
            res = session.execute(query)
            result = res.unique().scalars().all()

    @staticmethod
    def select_workers_with_relationship_contains_eager_with_limit():
        # https://stackoverflow.com/a/72298903/22259413 
        with session_factory() as session:
            subq = (
                select(ResumeORM.id.label("parttime_resume_id"))
                .filter(ResumeORM.worker_id == WorkerORM.id)
                .order_by(WorkerORM.id.desc())
                .limit(1)
                .scalar_subquery()
                .correlate(WorkerORM)
            )

            query = (
                select(WorkerORM)
                .join(ResumeORM, ResumeORM.id.in_(subq))
                .options(contains_eager(WorkerORM.resume))
            )

            res = session.execute(query)
            result = res.unique().scalars().all()
            print(result)

    @staticmethod
    def convert_workers_to_dto():
        with session_factory() as session:
            query = (
                select(WorkerORM)
                .options(selectinload(WorkerORM.resume))
                .limit(2)
            )

            res = session.execute(query)
            result_orm = res.scalars().all()
            print(f"{result_orm=}")
            result_dto = [WorkerRelDTO.model_validate(row, from_attributes=True) for row in result_orm]
            print(f"{result_dto=}")
            return result_dto
        

class AsyncORM:
    # Асинхронный вариант, не показанный в видео
    @staticmethod
    async def create_tables():
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)

    @staticmethod
    async def insert_workers():
        async with async_session_factory() as session:
            worker_jack = WorkerORM(username="Jack")
            worker_michael = WorkerORM(username="Michael")
            session.add_all([worker_jack, worker_michael])
            # flush взаимодействует с БД, поэтому пишем await
            await session.flush()
            await session.commit()

    @staticmethod
    async def select_workers():
        async with async_session_factory() as session:
            query = select(WorkerORM)
            result = await session.execute(query)
            workers = result.scalars().all()
            print(f"{workers=}")

    # @staticmethod
    # async def update_worker(worker_id: int = 2, new_username: str = "Misha"):
    #     async with async_session_factory() as session:
    #         worker_michael = await session.get(WorkerORM, worker_id)
    #         worker_michael.username = new_username
    #         await session.refresh(worker_michael)
    #         await session.commit()

    @staticmethod
    async def insert_resumes():
        async with async_session_factory() as session:
            resume_jack_1 = ResumeORM(
                title="Python Junior Developer", compensation=50000, workload=Workload.fulltime, worker_id=1)
            resume_jack_2 = ResumeORM(
                title="Python Разработчик", compensation=150000, workload=Workload.fulltime, worker_id=1)
            resume_michael_1 = ResumeORM(
                title="Python Data Engineer", compensation=250000, workload=Workload.parttime, worker_id=2)
            resume_michael_2 = ResumeORM(
                title="Data Scientist", compensation=300000, workload=Workload.fulltime, worker_id=2)
            session.add_all([resume_jack_1, resume_jack_2, 
                             resume_michael_1, resume_michael_2])
            await session.commit()

    @staticmethod
    async def select_resumes_avg_compensation(like_language: str = "Python"):
        """
        select workload, avg(compensation)::int as avg_compensation
        from resumes
        where title like '%Python%' and compensation > 40000
        group by workload
        having avg(compensation) > 70000
        """
        async with async_session_factory() as session:
            query = (
                select(
                    ResumeORM.workload,
                    # 1 вариант использования cast
                    # cast(func.avg(ResumesOrm.compensation), Integer).label("avg_compensation"),
                    # 2 вариант использования cast (предпочтительный способ)
                    func.avg(ResumeORM.compensation).cast(Integer).label("avg_compensation"),
                )
                .select_from(ResumeORM)
                .filter(and_(
                    ResumeORM.title.contains(like_language),
                    ResumeORM.compensation > 40000,
                ))
                .group_by(ResumeORM.workload)
                .having(func.avg(ResumeORM.compensation) > 70000)
            )
            print(query.compile(compile_kwargs={"literal_binds": True}))
            res = await session.execute(query)
            result = res.all()
            print(result[0].avg_compensation)

    @staticmethod
    async def insert_additional_resumes():
        async with async_session_factory() as session:
            workers = [
                {"username": "Artem"},  # id 3
                {"username": "Roman"},  # id 4
                {"username": "Petr"},   # id 5
            ]
            resumes = [
                {"title": "Python программист", "compensation": 60000, "workload": "fulltime", "worker_id": 3},
                {"title": "Machine Learning Engineer", "compensation": 70000, "workload": "parttime", "worker_id": 3},
                {"title": "Python Data Scientist", "compensation": 80000, "workload": "parttime", "worker_id": 4},
                {"title": "Python Analyst", "compensation": 90000, "workload": "fulltime", "worker_id": 4},
                {"title": "Python Junior Developer", "compensation": 100000, "workload": "fulltime", "worker_id": 5},
            ]
            insert_workers = insert(WorkerORM).values(workers)
            insert_resumes = insert(ResumeORM).values(resumes)
            await session.execute(insert_workers)
            await session.execute(insert_resumes)
            await session.commit()

    @staticmethod
    async def join_cte_subquery_window_func():
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
        async with async_session_factory() as session:
            r = aliased(ResumeORM)
            w = aliased(WorkerORM)
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

            res = await session.execute(query)
            result = res.all()
            print(f"{len(result)=}. {result=}")

    @staticmethod
    async def select_workers_with_lazy_relationship():
        async with async_session_factory() as session:
            query = (
                select(WorkerORM)
            )
            
            res = await session.execute(query)
            result = res.scalars().all()

            # worker_1_resumes = result[0].resumes  # -> Приведет к ошибке
            # Нельзя использовать ленивую подгрузку в асинхронном варианте!

            # Ошибка: sqlalchemy.exc.MissingGreenlet: greenlet_spawn has not been called; can't call await_only() here. 
            # Was IO attempted in an unexpected place? (Background on this error at: https://sqlalche.me/e/20/xd2s)
            

    @staticmethod
    async def select_workers_with_joined_relationship():
        async with async_session_factory() as session:
            query = (
                select(WorkerORM)
                .options(joinedload(WorkerORM.resume))
            )
            
            res = await session.execute(query)
            result = res.unique().scalars().all()

            worker_1_resumes = result[0].resume
            # print(worker_1_resumes)
            
            worker_2_resumes = result[1].resume
            # print(worker_2_resumes)

    @staticmethod
    async def select_workers_with_selectin_relationship():
        async with async_session_factory() as session:
            query = (
                select(WorkerORM)
                .options(selectinload(WorkerORM.resume))
            )
            
            res = await session.execute(query)
            result = res.scalars().all()

            worker_1_resumes = result[0].resume
            # print(worker_1_resumes)
            
            worker_2_resumes = result[1].resume
            # print(worker_2_resumes)

    @staticmethod
    async def select_workers_with_condition_relationship():
        async with async_session_factory() as session:
            query = (
                select(WorkerORM)
                .options(selectinload(WorkerORM.resume_parttime))
            )

            res = await session.execute(query)
            result = res.scalars().all()
            print(result)

    @staticmethod
    async def select_workers_with_condition_relationship_contains_eager():
        async with async_session_factory() as session:
            query = (
                select(WorkerORM)
                .join(WorkerORM.resume)
                .options(contains_eager(WorkerORM.resume))
                .filter(ResumeORM.workload == 'parttime')
            )

            res = await session.execute(query)
            result = res.unique().scalars().all()
            print(result)

    @staticmethod
    async def select_workers_with_relationship_contains_eager_with_limit():
        # Горячо рекомендую ознакомиться: https://stackoverflow.com/a/72298903/22259413 
        async with async_session_factory() as session:
            subq = (
                select(ResumeORM.id.label("parttime_resume_id"))
                .filter(ResumeORM.worker_id == WorkerORM.id)
                .order_by(WorkerORM.id.desc())
                .limit(1)
                .scalar_subquery()
                .correlate(WorkerORM)
            )

            query = (
                select(WorkerORM)
                .join(ResumeORM, ResumeORM.id.in_(subq))
                .options(contains_eager(WorkerORM.resume))
            )

            res = await session.execute(query)
            result = res.unique().scalars().all()
            print(result)

    @staticmethod
    async def convert_workers_to_dto():
        async with async_session_factory() as session:
            query = (
                select(WorkerORM)
                .options(selectinload(WorkerORM.resume))
                .limit(2)
            )

            res = await session.execute(query)
            result_orm = res.scalars().all()
            print(f"{result_orm=}")
            result_dto = [WorkerRelDTO.model_validate(row, from_attributes=True) for row in result_orm]
            print(f"{result_dto=}")
            return result_dto
        
    # @staticmethod
    # async def add_vacancies_and_replies():
    #     async with async_session_factory() as session:
    #         new_vacancy = VacanciesOrm(title="Python разработчик", compensation=100000)
    #         get_resume_1 = select(ResumeORM).options(selectinload(ResumeORM.vacancies_replied)).filter_by(id=1)
    #         get_resume_2 = select(ResumeORM).options(selectinload(ResumeORM.vacancies_replied)).filter_by(id=2)
    #         resume_1 = (await session.execute(get_resume_1)).scalar_one()
    #         resume_2 = (await session.execute(get_resume_2)).scalar_one()
    #         resume_1.vacancies_replied.append(new_vacancy)
    #         resume_2.vacancies_replied.append(new_vacancy)
    #         await session.commit()

    # @staticmethod
    # async def select_resumes_with_all_relationships():
    #     async with async_session_factory() as session:
    #         query = (
    #             select(ResumeORM)
    #             .options(joinedload(ResumeORM.worker))
    #             .options(selectinload(ResumeORM.vacancies_replied).load_only(VacanciesOrm.title))
    #         )

    #         res = await session.execute(query)
    #         result_orm = res.unique().scalars().all()
    #         print(f"{result_orm=}")
    #         # Обратите внимание, что созданная в видео модель содержала лишний столбец compensation
    #         # И так как он есть в схеме ResumesRelVacanciesRepliedDTO, столбец compensation был вызван
    #         # Алхимией через ленивую загрузку. В асинхронном варианте это приводило к краху программы
    #         result_dto = [ResumesRelVacanciesRepliedWithoutVacancyCompensationDTO.model_validate(row, from_attributes=True) for row in result_orm]
    #         print(f"{result_dto=}")
    #         return result_dto