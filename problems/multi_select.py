from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import sqlalchemy as sa
from rich import print
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column

from naked_sqla.session import AsyncSessionFactory


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


@asynccontextmanager
async def init_db():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    db = AsyncSessionFactory(engine)

    async with engine.begin() as conn:
        await conn.run_sync(BaseSQL.metadata.create_all)
    yield db


async def main():
    async with init_db() as db:
        async with db.begin() as session:
            now = datetime.now(UTC)
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

            await session.execute(sa.insert(E1).values([asdict(obj) for obj in objs]))
            await session.execute(
                sa.insert(E2).values([asdict(obj) for obj in parents])
            )

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


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
