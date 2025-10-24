from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict

from models import Workload


class WorkerAddDTO(BaseModel):
    username: str

class WorkerDTO(WorkerAddDTO):
    id: int

class ResumeAddDTO(BaseModel):
    title: str
    compensation: Optional[int]
    workload: Workload
    worker_id: int

class ResumeDTO(ResumeAddDTO):
    id: int
    created_at: datetime
    updated_at: datetime

class ResumesRelDTO(ResumeDTO):
    worker: "WorkerDTO"

class WorkerRelDTO(WorkerDTO):
    resumes: list["ResumeDTO"]