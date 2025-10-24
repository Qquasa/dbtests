import asyncio
import os
import sys
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

sys.path.insert(1, os.path.join(sys.path[0], '..'))

from queries.orm import SyncORM, AsyncORM
from queries.core import SyncCORE

async def main():

    if "--core" in sys.argv and "--sync" in sys.argv:
        SyncCORE.create_tables()
        SyncCORE.insert_worker()
        SyncCORE.select_worker()

    # ORM
    elif "--orm" in sys.argv and "--sync" in sys.argv:
        SyncORM.create_tables()
        SyncORM.insert_data()
        SyncORM.select_worker()
        SyncORM.update_worker()
        SyncORM.insert_resume()
        SyncORM.select_resume_avg_compensation()
        # SyncORM.select_workers_with_joined_relationship()
        SyncORM.select_workers_with_selectinload_relationship()

# def create_fastapi_app():
#     app = FastAPI(title="FastAPI")
#     app.add_middleware(
#         CORSMiddleware,
#         allow_origins=["*"],
#     )
        
#     @app.get("/workers", tags=["Кандидат"])
#     async def get_workers():
#         workers = SyncORM.convert_workers_to_dto()
#         return workers
        
#     @app.get("/resumes", tags=["Резюме"])
#     async def get_resumes():
#         resumes = await AsyncORM.select_resume_with_all_relationships()
#         return resumes
    
#     return app
    

# app = create_fastapi_app()

if __name__ == "__main__":
    asyncio.run(main())
    if "--webserver" in sys.argv:
        uvicorn.run(
            app="src.main:app",
            reload=True,
        )