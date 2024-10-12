from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
import pytest_asyncio
import sqlalchemy as sa
from rich import print
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import (
    Bundle,
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    mapped_column,
)

from naked_sqla.om.asession import AsyncSession, AsyncSessionFactory


class BaseSQL(MappedAsDataclass, DeclarativeBase): ...


class E1(BaseSQL):
    __tablename__ = "E1"
    event: Mapped[str] = mapped_column(sa.String())
    created_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True))
    id: Mapped[str] = mapped_column(
        primary_key=True, default_factory=lambda: str(uuid4())
    )
    author_id: Mapped[str] = mapped_column(
        primary_key=True, default_factory=lambda: str(uuid4())
    )

    __table_args__ = (
        sa.Index(
            "unique_active_status_new",
            author_id,  # per patient
            created_at,  # per time
            unique=True,  # only one
        ),
    )


class E2(BaseSQL):
    __tablename__ = "E2"
    event: Mapped[str] = mapped_column(sa.String())
    created_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True))
    id: Mapped[str] = mapped_column(
        primary_key=True, default_factory=lambda: str(uuid4())
    )
    author_id: Mapped[str] = mapped_column(
        primary_key=True, default_factory=lambda: str(uuid4())
    )


@pytest_asyncio.fixture(scope="module")
async def init_db():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    db = AsyncSessionFactory(engine)

    async with engine.begin() as conn:
        await conn.run_sync(BaseSQL.metadata.create_all)

    now = datetime.now(timezone.utc)
    author1 = str(uuid4())
    author2 = str(uuid4())
    author3 = str(uuid4())
    author4 = str(uuid4())
    objs = [
        E1(author_id=author1, event="1", created_at=now - timedelta(days=4)),
        E1(author_id=author2, event="2", created_at=now - timedelta(days=3)),
        E1(author_id=author3, event="3", created_at=now - timedelta(days=2)),
        E1(author_id=author4, event="4", created_at=now - timedelta(days=1)),
    ]
    parents = [
        E2(author_id=author1, event="1", created_at=now - timedelta(days=4)),
        E2(author_id=author2, event="2", created_at=now - timedelta(days=3)),
        E2(author_id=author3, event="3", created_at=now - timedelta(days=2)),
        E2(author_id=author4, event="4", created_at=now - timedelta(days=1)),
    ]
    async with db.begin() as session:
        await session.execute(sa.insert(E1).values([asdict(obj) for obj in objs]))
        await session.execute(sa.insert(E2).values([asdict(obj) for obj in parents]))

    yield db


@pytest_asyncio.fixture(scope="function")
async def session(init_db: AsyncSessionFactory):
    async with init_db.begin() as session:
        yield session


@pytest.mark.asyncio
async def test_multi_select(session: AsyncSession):
    query = (
        sa.select(E1, E2)
        .join(E2, E1.author_id == E2.author_id)
        .where(E1.author_id == E2.author_id)
        .order_by(E1.event)
    )
    result = (await session.tuples(query)).all()
    print(result)  # updated object
    assert len(result) == 4
    assert result[0][0].event == "1"
    assert result[1][0].event == "2"
    assert result[2][0].event == "3"
    assert result[3][0].event == "4"


@pytest.mark.asyncio
async def test_bundle_select(session: AsyncSession):
    event_bundle = Bundle(
        "event_bundle", E1.event.label("e1_event"), E2.event.label("e2_event")
    )

    query = (
        sa.select(event_bundle)
        .join(E2, E1.author_id == E2.author_id)
        .order_by(E1.event)
    )
    result = (await session.execute(query)).all()
    print(result)
    assert len(result) == 4
    # Accessing bundle elements
    for row in result:
        bundle = row[0]
        e1_event = bundle.e1_event
        e2_event = bundle.e2_event
        assert e1_event == e2_event

    assert result[0][0].e1_event == "1"
    assert result[1][0].e1_event == "2"
    assert result[2][0].e1_event == "3"
    assert result[3][0].e1_event == "4"


@pytest.mark.asyncio
async def test_with_column_select(session: AsyncSession):
    query = (
        sa.select(E1.event, E2.event)
        .join(E2, E1.author_id == E2.author_id)
        .order_by(E1.event)
    )
    result = (await session.execute(query)).all()
    print(result)
    assert len(result) == 4
    assert result[0][0] == "1"
    assert result[0][1] == "1"
    assert result[1][0] == "2"
    assert result[1][1] == "2"
    assert result[2][0] == "3"
    assert result[2][1] == "3"
    assert result[3][0] == "4"
    assert result[3][1] == "4"


@pytest.mark.asyncio
async def test_with_pg_insert(session: AsyncSession):
    author5 = str(uuid4())
    author6 = str(uuid4())
    pg_insert_stmt = (
        pg_insert(E1)
        .values(
            [
                {
                    "id": str(uuid4()),
                    "author_id": author5,
                    "event": "5",
                    "created_at": datetime.now(timezone.utc),
                },
                {
                    "id": str(uuid4()),
                    "author_id": author6,
                    "event": "6",
                    "created_at": datetime.now(timezone.utc),
                },
            ]
        )
        .returning(E1)
    )

    result = (await session.execute(pg_insert_stmt)).all()
    print(result)
    assert len(result) == 2
    assert result[0].event == "5"
    assert result[1].event == "6"
    assert result[0].author_id == author5
    assert result[1].author_id == author6
    await session.rollback()  # to not affect other tests
