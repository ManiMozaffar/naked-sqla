from typing import Optional

from sqlalchemy.engine import CursorResult
from sqlalchemy.engine.interfaces import (
    CoreExecuteOptionsParameter,
    _CoreAnyExecuteParams,
)
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.orm import Session as _Session
from sqlalchemy.orm.context import QueryContext
from sqlalchemy.sql import dml

from naked_sqla.loading import instances


def _return_orm_returning(
    conn: AsyncConnection,
    result: CursorResult,
    statement: dml.Insert,
    execution_options: Optional[CoreExecuteOptionsParameter] = None,
):
    execution_context = result.context
    compile_state = execution_context.compiled.compile_state  # type: ignore

    sync_session = _Session(bind=conn.sync_connection)

    querycontext = QueryContext(
        compile_state.from_statement_ctx,  # type: ignore
        compile_state.select_statement,  # type: ignore
        statement,  # type: ignore
        {},
        sync_session,
        QueryContext.default_load_options,
        execution_options,
        None,
    )
    return instances(result, querycontext)


async def orm_execute_statement(
    conn: AsyncConnection,
    statement: dml.Insert,
    parameters: Optional[_CoreAnyExecuteParams] = None,
    execution_options: Optional[CoreExecuteOptionsParameter] = None,
):
    result = await conn.execute(
        statement,
        parameters=parameters,
        execution_options=execution_options,
    )
    if not bool(statement._returning):
        return result
    return _return_orm_returning(
        conn, result, statement, execution_options=execution_options
    )
