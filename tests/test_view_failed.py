# --------------------------------------

# SQLAlchemy ORM is always expecting you to define a primary key
# but sometimes you don't have one, like in this case, where we are querying a view
# so what happens? SQLAlchemy tries to figure out what primary key exists in the view.
# And somehow it picks a wrong primary key (in case we are querying a database that have multiple primary keys)
# This primary key then is used to identify the object and keep only one instance of it in the session
# and then, doesn't matter how many rows you query, it will always return the same object

# but if you run the query with connection(CORE), it works and return corrected result.

# --------------------------------------

from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Union
from uuid import uuid4

import pytest
import sqlalchemy as sa
from rich import print
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.ext.asyncio.session import AsyncSession as SmartAsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column

from naked_sqla.om.asession import AsyncSession, AsyncSessionFactory
from naked_sqla.view import init_view_in_base, view_table


class BaseSQL(MappedAsDataclass, DeclarativeBase):
    def __init_subclass__(
        cls,
        *args,
        **kw,
    ) -> None:
        init_view_in_base(cls)
        super().__init_subclass__(*args, **kw)


class Event(BaseSQL):
    __tablename__ = "Events"
    event: Mapped[str] = mapped_column(sa.String())
    created_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True))
    id: Mapped[str] = mapped_column(
        primary_key=True, default_factory=lambda: str(uuid4())
    )
    author_id: Mapped[str] = mapped_column(
        primary_key=True, default_factory=lambda: str(uuid4())
    )


def event_period_view_query():
    event_transitions = sa.select(
        Event.author_id,
        Event.event,
        Event.created_at,
        sa.func.lead(Event.event)
        .over(
            partition_by=Event.author_id,
            order_by=Event.created_at,
        )
        .label("next_event"),
        sa.func.lead(Event.created_at)
        .over(
            partition_by=Event.author_id,
            order_by=Event.created_at,
        )
        .label("next_created_at"),
    ).subquery()

    event_periods = sa.select(
        event_transitions.c.author_id,
        event_transitions.c.event,
        event_transitions.c.created_at.label("start_datetime"),
        sa.func.coalesce(event_transitions.c.next_created_at, sa.func.now()).label(
            "end_datetime"
        ),
    ).select_from(event_transitions)
    return event_periods


class EventPeriod(BaseSQL):
    __tablename__ = "EventPeriods"
    __table__ = view_table(__tablename__, BaseSQL.metadata, event_period_view_query())

    # id: Mapped[str]
    author_id: Mapped[str]
    event: Mapped[str]
    start_datetime: Mapped[datetime]
    end_datetime: Mapped[datetime]


@asynccontextmanager
async def init_naked_db():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    db = AsyncSessionFactory(engine)
    async with engine.begin() as conn:
        await conn.run_sync(BaseSQL.metadata.create_all)
    yield db


@asynccontextmanager
async def init_sqlachemy_db():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    db = async_sessionmaker(engine, expire_on_commit=False, autobegin=False)
    async with engine.begin() as conn:
        await conn.run_sync(BaseSQL.metadata.create_all)
    yield db


async def run_view_query(session: Union[AsyncSession, SmartAsyncSession]):
    now = datetime.now(timezone.utc)
    author_id = str(uuid4())
    objs = [
        Event(
            author_id=author_id,
            event="1",
            created_at=now - timedelta(days=3),
        ),
        Event(
            author_id=author_id,
            event="2",
            created_at=now - timedelta(days=2),
        ),
        Event(
            author_id=author_id,
            event="3",
            created_at=now - timedelta(days=1),
        ),
    ]
    await session.execute(sa.insert(Event).values([asdict(obj) for obj in objs]))

    query = sa.select(EventPeriod).order_by(EventPeriod.event)
    result = (await session.execute(query)).scalars().all()
    print(result)
    assert len(result) == 3
    return result


@pytest.mark.asyncio
async def test_naked_sqla_detect_view_identity_key_correctly():
    async with init_naked_db() as db:
        async with db.begin() as session:
            result = await run_view_query(session)
            assert result[0].event == "1"
            assert result[1].event == "2"
            assert result[2].event == "3"


@pytest.mark.asyncio
async def test_sqlalchemy_fail_to_detect_view_identity_key():
    async with init_sqlachemy_db() as db:
        async with db.begin() as session:
            result = await run_view_query(session)
            assert result[0].event == "1"
            assert result[1].event == "1"
            assert result[2].event == "1"
