import typing
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any, Self
from uuid import uuid4

import sqlalchemy as sa
from rich import print
from sqlalchemy.exc import NoInspectionAvailable
from sqlalchemy.ext import compiler
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column
from sqlalchemy.schema import DDLElement

from naked_sqla.session import AsyncSessionFactory


class BaseSQL(MappedAsDataclass, DeclarativeBase):
    def __init_subclass__(cls, *args, **kw) -> None:
        if hasattr(cls, "__table__"):
            table_clause = cls.__table__  # type: ignore
            if isinstance(table_clause, View):
                for k in cls.__annotations__:
                    annotation = cls.__annotations__[k]
                    annotation = typing.get_args(annotation)[0]  # remove Mapped
                    cls.__annotations__[k] = annotation

        super().__init_subclass__(*args, **kw)


class CreateView(DDLElement):
    def __init__(self, name: str, selectable: sa.Select):
        self.name = name
        self.selectable = selectable


class DropView(DDLElement):
    def __init__(self, name, cascade=False, if_exists=False):
        self.name = name
        self.cascade = cascade
        self.if_exists = if_exists


@compiler.compiles(CreateView)
def _create_view(element: CreateView, compiler, **kw):
    return 'CREATE VIEW "%s" AS %s' % (
        element.name,
        compiler.sql_compiler.process(element.selectable, literal_binds=True),
    )


@compiler.compiles(DropView)
def _drop_view(element: DropView, compiler, **kw):
    text = "DROP VIEW "
    if element.if_exists:
        text += "IF EXISTS "
    text += f'"{element.name}"'
    if element.cascade:
        text += " CASCADE"
    return text


def view_exists(ddl, target, connection, **kw):
    try:
        return ddl.name in sa.inspect(connection).get_view_names()
    except NoInspectionAvailable:
        return False


def view_doesnt_exist(ddl, target, connection, **kw):
    return not view_exists(ddl, target, connection, **kw)


class View(sa.TableClause):
    exclude_in_sqlite: bool = False
    inherit_cache: bool = False  # type: ignore

    @classmethod
    def from_name(cls, name: str, *columns: sa.ColumnClause[Any], **kw: Any) -> Self:
        return cls(name, *columns, **kw)


def view(name, metadata, selectable, *, cascade=True):
    t = View.from_name(name)
    t._columns._populate_separate_keys(
        col._make_proxy(t) for col in selectable.selected_columns
    )
    sa.event.listen(
        metadata,
        "after_create",
        CreateView(name, selectable).execute_if(callable_=view_doesnt_exist),  # type: ignore
    )
    sa.event.listen(
        metadata,
        "before_drop",
        DropView(name, cascade=cascade).execute_if(callable_=view_exists),  # type: ignore
    )
    return t


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
    __table__ = view(__tablename__, BaseSQL.metadata, event_period_view_query())

    # id: Mapped[str]
    author_id: Mapped[str]
    event: Mapped[str]
    start_datetime: Mapped[datetime]
    end_datetime: Mapped[datetime]


@asynccontextmanager
async def init_db():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")

    async with engine.begin() as conn:
        await conn.run_sync(BaseSQL.metadata.create_all)

    db = AsyncSessionFactory(engine)
    yield db


async def test():
    async with init_db() as db:
        async with db.begin() as session:
            now = datetime.now(UTC)
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
            await session.execute(
                sa.insert(Event).values([asdict(obj) for obj in objs])
            )

            query = sa.select(EventPeriod).order_by(EventPeriod.event)
            result = (await session.execute(query)).scalars().all()
            print(result)

            unmapped_result = (await session.conn.execute(query)).all()
            print(unmapped_result)

            assert len(result) == 3

            assert result[0].event == "1"
            assert result[1].event == "2"
            assert result[2].event == "3"

            assert len(unmapped_result) == 3
            assert unmapped_result[0][1] == "1"
            assert unmapped_result[1][1] == "2"
            assert unmapped_result[2][1] == "3"


if __name__ == "__main__":
    import asyncio

    asyncio.run(test())
