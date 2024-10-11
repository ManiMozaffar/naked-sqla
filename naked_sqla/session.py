from contextlib import asynccontextmanager
from typing import Any, Literal, Optional, TypeVar, assert_never, overload

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

from naked_sqla import bulk_persistent, context

_T = TypeVar("_T", bound=Any)


class AsyncSessionFactory:
    def __init__(self, engine: AsyncEngine, *, auto_commit: bool = True):
        self.engine = engine
        self.auto_commit = auto_commit

    @asynccontextmanager
    async def begin(self):
        async with self.engine.begin() as conn:
            try:
                yield AsyncSession(conn)
                if self.auto_commit:
                    await conn.commit()
            except Exception as err:
                if self.auto_commit:
                    await conn.rollback()
                raise err


class AsyncSession:
    def __init__(self, conn: AsyncConnection):
        self.conn = conn
        self.state: Literal["open", "closed"] = "open"

    async def commit(self):
        if self.state == "closed":
            raise Exception("Session is already closed")
        elif self.state == "open":
            await self.conn.commit()
            self.state = "closed"
        else:
            assert_never(self.state)

    async def rollback(self):
        if self.state == "closed":
            raise Exception("Session is already closed")
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
        result = (
            await self.execute(
                statement, parameters, execution_options=execution_options
            )
        ).tuples()
        return result

    @overload
    async def scalars(
        self,
        statement: TypedReturnsRows[_T],
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
        result = (
            await self.execute(
                statement, parameters, execution_options=execution_options
            )
        ).scalars()
        return result
