"""
A view in SQL is like a virtual table that you create using a SQL query. It doesn't store data itself but pulls it from one or more actual tables whenever you use it. Here’s why you might use a view:

- Simplify Complex Queries: Instead of writing the same complicated joins or filters every time, you define them once in a view and just select from the view.
- Improve Security: You can restrict what data users see by only exposing certain columns or rows through the view, keeping sensitive information hidden.
- Enhance Maintainability: If the underlying table structure changes, you can update the view without having to change all the queries that use it.
- Consistency: Ensure that everyone is using the same logic to access data, reducing errors and inconsistencies.

In short, views make your SQL work cleaner, safer, and easier to manage.
"""

import typing
from typing import Any, Type

import sqlalchemy as sa
from sqlalchemy.exc import NoInspectionAvailable
from sqlalchemy.ext import compiler
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.schema import DDLElement


def init_view_in_base(cls: Type[DeclarativeBase]):
    """
    Calling this function is required to allow a declarative base to have views as tables.
    Look at the example below to see how to use it.

    Params:
        cls: The declarative base class to be initialized.

    Example:
        ```python hl_lines="9-10"

        from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass

        class BaseSQL(MappedAsDataclass, DeclarativeBase):
            def __init_subclass__(
                cls,
                *args,
                **kw,
            ) -> None:
                init_view_in_base(cls)
                super().__init_subclass__(*args, **kw)
        ```


    """
    if hasattr(cls, "__table__"):
        table_clause = cls.__table__  # type: ignore
        if isinstance(table_clause, _View):
            for k in cls.__annotations__:
                # a table initiated with __table__ cannot have mapped_column and Mapped
                # https://github.com/sqlalchemy/sqlalchemy/commit/3b7ffd2f9745e6038bbb7209635d3407fe8ff5ec
                annotation = cls.__annotations__[k]
                annotation = typing.get_args(annotation)[0]  # remove Mapped
                cls.__annotations__[k] = annotation


def _view_exists(ddl, target, connection, **kw):
    try:
        return ddl.name in sa.inspect(connection).get_view_names()
    except NoInspectionAvailable:
        return False


def _view_doesnt_exist(ddl, target, connection, **kw):
    return not _view_exists(ddl, target, connection, **kw)


class _View(sa.TableClause):
    exclude_in_sqlite: bool = False
    inherit_cache: bool = False  # type: ignore

    @classmethod
    def from_name(cls, name: str, *columns: sa.ColumnClause[Any], **kw: Any):
        return cls(name, *columns, **kw)


def view_table(name, metadata, selectable, *, cascade=True):
    """Create a view table.

    Params:
        name: The name of the view.
        metadata: The metadata object to bind the view to.
        selectable: The select statement to be used as the view.
        cascade: Whether to drop the view with cascade or not.

    Example:
        ```python
        from datetime import datetime
        from uuid import uuid4

        import sqlalchemy as sa
        from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column

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

            author_id: Mapped[str]
            event: Mapped[str]
            start_datetime: Mapped[datetime]
            end_datetime: Mapped[datetime]
        ```



    """

    t = _View.from_name(name)
    t._columns._populate_separate_keys(
        col._make_proxy(t) for col in selectable.selected_columns
    )
    sa.event.listen(
        metadata,
        "after_create",
        CreateView(name, selectable).execute_if(callable_=_view_doesnt_exist),  # type: ignore
    )
    sa.event.listen(
        metadata,
        "before_drop",
        DropView(name, cascade=cascade).execute_if(callable_=_view_exists),  # type: ignore
    )
    return t


class CreateView(DDLElement):
    """A CREATE VIEW statement, usually useful when using in migrations.

    Params:
        name: The name of the view.
        selectable: The select statement to be used as the view.


    Example:
        ```python
        def main():
            query = CreateView(ActiveBlog.__tablename__, active_view())
            connection.execute(query) # this will create the view
        ```

    """

    def __init__(self, name: str, selectable: sa.Select):
        self.name = name
        self.selectable = selectable


class DropView(DDLElement):
    """A DROP VIEW statement, usually useful when using in migrations.

    Params:
        name: The name of the view.
        cascade: Whether to drop the view with cascade or not.
        if_exists: Whether to drop the view if it exists or not.

    Example:
        ```python

        def main():
            query = DropView(ViewTable.__tablename__)
            connection.execute(query) # this will drop the view table
        ```

    """

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
