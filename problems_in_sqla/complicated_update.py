# it seems SQLAlchemy tries to figure out what changes happened to database, and try to apply it to the tracked objects from Sessoin
# if update query is too complicated, it just fail
# then worst thing happens, it shadow a completely different object as result of database
# but if you run the query with connection(CORE), it works and return corrected result.


from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column

USE_CORE = False


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

    __table_args__ = (
        sa.Index(
            "unique_active_status_new",
            author_id,  # per patient
            created_at,  # per time
            unique=True,  # only one
        ),
    )


@asynccontextmanager
async def init_db():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    db = async_sessionmaker(engine, expire_on_commit=False, autobegin=False)

    async with engine.begin() as conn:
        await conn.run_sync(BaseSQL.metadata.create_all)
    yield db


async def test():
    async with init_db() as db:
        async with db.begin() as session:
            now = datetime.now(UTC)
            author_id = str(uuid4())
            objs = [
                Event(
                    author_id=author_id, event="1", created_at=now - timedelta(days=4)
                ),
                Event(
                    author_id=author_id, event="2", created_at=now - timedelta(days=3)
                ),
                Event(
                    author_id=author_id, event="3", created_at=now - timedelta(days=2)
                ),
                Event(
                    author_id=author_id, event="4", created_at=now - timedelta(days=1)
                ),
            ]

            session.add_all(objs)
            await session.flush()

            # updating the event that comes before the event with the value '2' to value '2'
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

            if USE_CORE:
                # updated object is correctly returned (has event='2')
                conn = await session.connection()
                result = (await conn.execute(query)).mappings().all()
                print(result)
                assert result[0]["event"] == "2"  # NICE!

            else:
                # updated object is not returned (has event='1') !!!!!!
                result = (await session.execute(query)).scalars().all()
                print(result)  # updated object
                print(objs)  # previous added objects are out of bound!

                assert result[0].event == "1"  # WTF?


if __name__ == "__main__":
    import asyncio

    asyncio.run(test())
