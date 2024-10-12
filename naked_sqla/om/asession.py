"""
This section is about the Session class. This class is a simple wrapper around the `Connection` object.
Session main purpose is to execute queries and return the result, as mapped to ORM objects.


By design, to ensure consistency and simplicity, Session are always within a transaction.
So you should always commit or rollback the transaction after you are done with the session.

Session Factory is a factory for creating sessions. You should use this factory to create a new session every time you need to start a new transaction.
"""

from contextlib import asynccontextmanager
from typing import Any, Literal, Optional, Tuple, TypeVar, overload

from sqlalchemy import ScalarResult
from sqlalchemy.engine import Result, TupleResult
from sqlalchemy.engine.interfaces import (
    _CoreAnyExecuteParams,
    _CoreKnownExecutionOptions,
)
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from sqlalchemy.sql import dml
from sqlalchemy.sql.base import Executable
from sqlalchemy.sql.selectable import TypedReturnsRows
from typing_extensions import assert_never

from naked_sqla.exception import BaseNakedSQLAException
from naked_sqla.om import bulk_persistent, context

_T = TypeVar("_T", bound=Any)


class InvalidSessionState(BaseNakedSQLAException):
    def __init__(self, state: str, expected: str):
        super().__init__(f"Invalid transaction state: {state}. Expected: {expected}")


class AsyncSessionFactory:
    """
    A factory for creating async sessions.

    Usually you should use this factory with Engine, and then use the `begin` method to create a session every time

    ??? warning "If you are using FastAPI, always use a single instance of this factory as a dependency."
        If you are using FastAPI, you should create a single instance of this factory and use it as a dependency.
        So you don't initiate a new AsyncSessionFactory every time you create a session. So something like this:

        ```python
        from fastapi import FastAPI, Depends
        from sqlalchemy.ext.asyncio import create_async_engine
        from naked_sqla.om.asession import AsyncSessionFactory

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        db = AsyncSessionFactory(engine)
        async def get_session_factory():
            return db
        ```

        And then call .begin() method in your route functions whenever you need to start a new transaction.


    params:
        engine:
            The engine object that will be used for creating connections.
        auto_commit:
            If True, the session will automatically commit the transaction after the block ends.
                If any exception occurs, it will rollback the transaction.
            If False, you have to manually commit or rollback the transaction.
            If you don't commit or rollback the transaction, it will be rolled back automatically.


    Example:
        ```python
        from sqlalchemy.ext.asyncio import create_async_engine
        from naked_sqla.om.asession import AsyncSessionFactory

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        db = AsyncSessionFactory(engine)
        ```

    """

    def __init__(self, engine: AsyncEngine, *, auto_commit: bool = True):
        self.engine = engine
        self.auto_commit = auto_commit

    @asynccontextmanager
    async def begin(self):
        """
        Create a new session, commits the transaction if auto_commit is True.
        Returns a context manager that yields a Session object.

        You can use this context manager with the `with` statement to access session.

        Example:
            ```python
            from sqlalchemy.ext.asyncio import create_async_engine
            from naked_sqla.om.asession import AsyncSessionFactory

            engine = create_engine("sqlite+aiosqlite:///:memory:")
            db = AsyncSessionFactory(engine)

            async def main():
                async with db.begin() as session:
                    query = ...
                    await session.execute(query)
            ```
        """

        async with self.engine.begin() as conn:
            try:
                yield AsyncSession(conn)
                if self.auto_commit:
                    await conn.commit()
                else:
                    if conn.in_transaction():
                        await conn.rollback()

            except Exception as err:
                if self.auto_commit:
                    await conn.rollback()
                raise err


class AsyncSession:
    """
    AsyncSession is the async version of the Session object.

    Session is an object that represents a single transaction to the database.
    You can execute query statements and commit or rollback the transaction.

    Session is a simple wrapper around Connection object.
    The difference between Session and Connection is that Session would map
        your query results to ORM objects.

    ??? danger "Session is completely incompatible with sqlalchemy ORM Session."
        Session is a simple wrapper around the Connection object.

        Please avoid using Session if you are depending on SQLAlchemy ORM. Any ORM feature in SQLAlchemy does not work with Session.

        These features could be:

        - **Tracking ORM objects**: methods like `add`, `flush`, `expunge`, etc.
        Because our Session does not have any ORM object tracking.
        Which simplifies the implementation and makes it way faster.

        - **Relationship loading**: methods like `load`, `lazyload`, etc.
        Because our Session does not make any implicit query to the database.
        You have to explicitly write the query.
        If you want to load related objects, you have to write a query for that
        and join the tables manually like you do in SQL.



    params:
        conn:
            The connection object that will be used for executing queries.

    Example:
        ```python
        from datetime import datetime
        from uuid import uuid4

        from sqlalchemy.ext.asyncio import create_async_engine
        from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column

        from naked_sqla.om.asession import AsyncSessionFactory


        class BaseSQL(MappedAsDataclass, DeclarativeBase): ...


        class Book(BaseSQL):
            __tablename__ = "Events"
            created_at: Mapped[datetime]
            name: Mapped[str]
            author_name: Mapped[str]
            id: Mapped[str] = mapped_column(
                primary_key=True, default_factory=lambda: str(uuid4())
            )


        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        db = AsyncSessionFactory(engine)

        async def get_db():
            return db
        ```
    """

    def __init__(self, conn: AsyncConnection):
        self.conn = conn
        self.state: Literal["open", "closed"] = "open"

    async def commit(self):
        """Commit the transaction."""
        if self.state == "closed":
            raise InvalidSessionState(self.state, "open")

        elif self.state == "open":
            await self.conn.commit()
            self.state = "closed"
        else:
            assert_never(self.state)

    async def rollback(self):
        """Rollback the transaction."""
        if self.state == "closed":
            raise InvalidSessionState(self.state, "open")

        elif self.state == "open":
            await self.conn.rollback()
            self.state = "closed"
        else:
            assert_never(self.state)

    @overload
    async def execute(
        self,
        statement: TypedReturnsRows[_T],
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[_CoreKnownExecutionOptions] = None,
    ) -> Result[_T]: ...

    @overload
    async def execute(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[_CoreKnownExecutionOptions] = None,
    ) -> Result[Any]: ...

    async def execute(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[_CoreKnownExecutionOptions] = None,
    ) -> Result[Any]:
        """
        Execute a query statement and return the result.

        After calling this method, you should call the `tuples` or `scalars` method to get the result.

        params:
            statement:
                The query statement to execute.
            parameters:
                The parameters to pass to the query.
            execution_options:
                The execution options to pass to the query.
        """

        if (
            isinstance(statement, dml.Insert)
            or isinstance(statement, dml.Update)
            or isinstance(statement, dml.Delete)
        ):
            return await bulk_persistent.orm_execute_statement(
                self.conn,
                statement,  # type: ignore
                parameters=parameters,
                execution_options=execution_options,
            )

        elif isinstance(statement, Executable):
            return await context.orm_execute_statement(
                self.conn,
                statement,
                parameters=parameters,
                execution_options=execution_options,
            )

        else:
            assert_never(statement)

    @overload
    async def tuples(
        self,
        statement: TypedReturnsRows[_T],
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[_CoreKnownExecutionOptions] = None,
    ) -> TupleResult[_T]: ...

    @overload
    async def tuples(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[_CoreKnownExecutionOptions] = None,
    ) -> TupleResult[Any]: ...

    async def tuples(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[_CoreKnownExecutionOptions] = None,
    ) -> TupleResult[Any]:
        """
        Execute a query statement and return the result as tuples.

        params:
            statement:
                The query statement to execute.
            parameters:
                The parameters to pass to the query.
            execution_options:
                The execution options to pass to the query.
        """

        result = (
            await self.execute(
                statement, parameters, execution_options=execution_options
            )
        ).tuples()
        return result

    @overload
    async def scalars(
        self,
        statement: TypedReturnsRows[Tuple[_T]],
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[_CoreKnownExecutionOptions] = None,
    ) -> ScalarResult[_T]: ...

    @overload
    async def scalars(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[_CoreKnownExecutionOptions] = None,
    ) -> ScalarResult[Any]: ...

    async def scalars(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[_CoreKnownExecutionOptions] = None,
    ) -> ScalarResult[Any]:
        """
        Execute a query statement and return the result as scalars.

        Scalar data types represent single values. They are the simplest forms of data types in programming.
        If you select two entities (e.x: two columns or two tables), it will return only first entity.
        Consider this as a shortcut when you are selecting only one entity, so you don't have to access to the first element of the tuple.

        params:
            statement:
                The query statement to execute.
            parameters:
                The parameters to pass to the query.
            execution_options:
                The execution options to pass to the query
        """
        result = (
            await self.execute(
                statement, parameters, execution_options=execution_options
            )
        ).scalars()
        return result
