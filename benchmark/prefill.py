import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column

from naked_sqla.om.asession import AsyncSessionFactory


class BaseSQL(MappedAsDataclass, DeclarativeBase): ...


class E1(BaseSQL):
    __tablename__ = "E1"
    event: Mapped[str] = mapped_column(sa.String(), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True))
    id: Mapped[str] = mapped_column(
        primary_key=True, default_factory=lambda: str(uuid4())
    )
    author_id: Mapped[str] = mapped_column(
        primary_key=True, default_factory=lambda: str(uuid4())
    )


@asynccontextmanager
async def init_naked_sqla_db():
    """Initialize Naked SQLAlchemy Session"""
    engine = create_async_engine("sqlite+aiosqlite:///test.db", echo=False)
    db = AsyncSessionFactory(engine)

    async with engine.begin() as conn:
        await conn.run_sync(BaseSQL.metadata.create_all)
    yield db


async def insert_data(session, num_records=100_000):
    """Insert 100k rows into the table"""
    now = datetime.now(timezone.utc)
    objs = [
        E1(event=f"event-{i}", created_at=now - timedelta(days=i))
        for i in range(num_records)
    ]
    await session.execute(sa.insert(E1), [asdict(obj) for obj in objs])


async def fn():
    async with init_naked_sqla_db() as db:
        async with db.begin() as session:
            await insert_data(session)


if __name__ == "__main__":
    asyncio.run(fn())
