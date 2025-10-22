import datetime
from typing import Annotated
from sqlalchemy import Table, Column, String, Integer, MetaData, ForeignKey, text, CheckConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from database import Base, str_256
import enum

intpk = Annotated[int, mapped_column(primary_key=True)]
created_at = Annotated[datetime.datetime, mapped_column(server_default=text("TIMEZONE('utc', now())"))]
updated_at = Annotated[datetime.datetime, mapped_column(server_default=text("TIMEZONE('utc', now())"),
    onupdate=datetime.datetime.utcnow)]

class WorkerORM(Base):
    __tablename__ = "worker"

    id: Mapped[intpk]
    username: Mapped[str]

    resume: Mapped[list["ResumeORM"]] = relationship(
        back_populates="worker",
    )

    resume_parttime: Mapped[list["ResumeORM"]] = relationship(
        back_populates="worker",
        primaryjoin="and_(WorkerORM.id == ResumeORM.worker_id, ResumeORM.workload == 'parttime')",
        order_by="ResumeORM.id.desc()"
    )

class Workload(enum.Enum):
    parttime = "parttime"
    fulltime = "fulltime"

class ResumeORM(Base):
    __tablename__ = "resume"

    id: Mapped[intpk]
    title: Mapped[str_256]
    compensation: Mapped[int | None] # mapped_column(nullable=True)
    workload: Mapped[Workload]
    worker_id: Mapped[int] = mapped_column(ForeignKey('worker.id', ondelete="CASCADE")) #__tablename__ = "worker"
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]

    worker: Mapped["WorkerORM"] = relationship(
        back_populates="resume"
    )

    repr_cols_num = 3
    repr_cols = tuple("updated_at")

    __table_args__=(
        Index("title_index", "title"),
        CheckConstraint("compensation > 0", name="checl_compensation_positive"),
    )    















metadata_obj = MetaData()

worker_table = Table(
    "worker",
    metadata_obj,
    Column('id', Integer, primary_key=True),
    Column('username', String),
)