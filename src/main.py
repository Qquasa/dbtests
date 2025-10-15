import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))

from queries.orm import SyncORM
from queries.core import SyncCORE

SyncORM.create_tables()
SyncORM.insert_data()

SyncORM.select_worker()
SyncORM.update_worker()

# SyncCORE.select_worker()
# SyncCORE.update_worker()