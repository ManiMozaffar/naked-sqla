from contextlib import asynccontextmanager
from typing import Any, Literal, Optional, TypeVar, assert_never, overload

from sqlalchemy.engine import Result
from sqlalchemy.engine.interfaces import (
    CoreExecuteOptionsParameter,
    _CoreAnyExecuteParams,
)
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from sqlalchemy.sql import dml
from sqlalchemy.sql.base import Executable
from sqlalchemy.sql.selectable import TypedReturnsRows

from naked_sqla import bulk_persistent, context

_T = TypeVar("_T", bound=Any)


class AsyncSessionFactory:
    def __init__(self, engine: AsyncEngine):
        self.engine = engine

    @asynccontextmanager
    async def begin(self):
        async with self.engine.begin() as conn:
            yield AsyncSession(conn)


class AsyncSession:
    def __init__(self, conn: AsyncConnection):
        self.conn = conn
        self.state: Literal["open", "closed"] = "open"

    async def commit(self):
        if self.state == "closed":
            raise Exception("Session is already closed")

        await self.conn.commit()
        self.state = "closed"

    async def rollback(self):
        if self.state == "closed":
            raise Exception("Session is already closed")

        await self.conn.rollback()
        self.state = "closed"

    @overload
    async def execute(
        self,
        statement: TypedReturnsRows[_T],
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> Result[_T]: ...

    @overload
    async def execute(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> Result[Any]: ...

    async def execute(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> Result[Any]:
        match statement:
            case dml.Insert() | dml.Update() | dml.Delete():
                return await bulk_persistent.orm_execute_statement(
                    self.conn,
                    statement,  # type: ignore
                    parameters=parameters,
                    execution_options=execution_options,
                )

            case Executable():
                return await context.orm_execute_statement(
                    self.conn,
                    statement,
                    parameters=parameters,
                    execution_options=execution_options,
                )

            case _:
                assert_never(statement)
