import asyncio
import os
import sys
import uvicorn

sys.path.insert(1, os.path.join(sys.path[0], '..'))

from queries.orm import SyncORM
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

if __name__ == "__main__":
    asyncio.run(main())
    if "--webserver" in sys.argv:
        uvicorn.run(
            app="src.main:app",
            reload=True,
        )