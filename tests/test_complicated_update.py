# --------------------------------------

# it seems SQLAlchemy tries to figure out what changes happened to database, and try to apply it to the tracked objects from Sessoin
# if update query is too complicated, it just fail
# then worst thing happens, it shadow a completely different object as result of database
# but if you run the query with connection(CORE), it works and return corrected result.

# --------------------------------------


from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Union
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession as SmartAsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column

from naked_sqla.om.asession import AsyncSession, AsyncSessionFactory


class BaseSQL(MappedAsDataclass, DeclarativeBase): ...


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


async def complicated_update_scenario(session: Union[AsyncSession, SmartAsyncSession]):
    """
    A scenario where we need to update a row based on the next row's value.
    The updated row is the one that has the event value of "2".
    """
    now = datetime.now(timezone.utc)
    author_id = str(uuid4())
    objs = [
        Event(author_id=author_id, event="1", created_at=now - timedelta(days=4)),
        Event(author_id=author_id, event="2", created_at=now - timedelta(days=3)),
    ]

    inserted_objs = (
        await session.execute(
            sa.insert(Event).returning(Event), [asdict(obj) for obj in objs]
        )
    ).all()
    print(inserted_objs)

    event_lead = sa.select(
        Event.id,
        sa.func.lead(Event.event)
        .over(
            partition_by=Event.author_id,
            order_by=Event.created_at,
        )
        .label("next_event"),
    ).subquery()
    event_query = (
        sa.select(event_lead.c.id)
        .select_from(event_lead)
        .where(event_lead.c.next_event == "2")
    )

    query = (
        sa.update(Event)
        .where(Event.id.in_(event_query))
        .values(event="2")
        .returning(Event)
    )

    result = (await session.execute(query)).scalars().first()
    assert result is not None
    return result


@pytest.mark.asyncio
async def test_complicated_update_map_correctly_in_naked_sqla():
    async with init_naked_db() as db:
        async with db.begin() as session:
            result = await complicated_update_scenario(session)
            assert result.event == "2"


@pytest.mark.asyncio
async def test_complicated_update_map_incorrectly_in_sqlalchemy():
    async with init_sqlachemy_db() as db:
        async with db.begin() as session:
            result = await complicated_update_scenario(session)
            assert result.event == "1"
