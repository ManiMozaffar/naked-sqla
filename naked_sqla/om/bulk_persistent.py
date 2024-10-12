from typing import Optional

from sqlalchemy import Connection
from sqlalchemy.engine import CursorResult
from sqlalchemy.engine.interfaces import (
    _CoreAnyExecuteParams,
    _CoreKnownExecutionOptions,
)
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql import dml

from naked_sqla.om.context import QueryContext
from naked_sqla.om.loading import instances


def _return_orm_returning(
    result: CursorResult,
    statement: dml.Insert,
    execution_options: Optional[_CoreKnownExecutionOptions] = None,
):
    execution_context = result.context
    compile_state = execution_context.compiled.compile_state  # type: ignore
    execution_options = execution_options or {}

    if (
        compile_state.from_statement_ctx  # type: ignore
        and not compile_state.from_statement_ctx.compile_options._is_star  # type: ignore
    ):
        load_options = execution_options.get(
            "_sa_orm_load_options", QueryContext.default_load_options
        )

        querycontext = QueryContext(
            compile_state.from_statement_ctx,  # type: ignore
            compile_state.select_statement,  # type: ignore
            statement,  # type: ignore
            {},
            load_options,
            execution_options,
            None,
        )
        return instances(result, querycontext)
    else:
        return result


async def orm_execute_statement(
    conn: AsyncConnection,
    statement: dml.Insert,
    parameters: Optional[_CoreAnyExecuteParams] = None,
    execution_options: Optional[_CoreKnownExecutionOptions] = None,
):
    result = await conn.execute(
        statement, parameters=parameters, execution_options=execution_options
    )
    if not bool(statement._returning):
        return result
    return _return_orm_returning(result, statement, execution_options=execution_options)


def sync_orm_execute_statement(
    conn: Connection,
    statement: dml.Insert,
    parameters: Optional[_CoreAnyExecuteParams] = None,
    execution_options: Optional[_CoreKnownExecutionOptions] = None,
):
    result = conn.execute(
        statement, parameters=parameters, execution_options=execution_options
    )
    if not bool(statement._returning):
        return result
    return _return_orm_returning(result, statement, execution_options=execution_options)
