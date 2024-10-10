from typing import Optional

from sqlalchemy.engine.interfaces import (
    CoreExecuteOptionsParameter,
    _CoreAnyExecuteParams,
)
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.orm import Session as _Session
from sqlalchemy.orm.context import QueryContext
from sqlalchemy.sql.base import Executable

from naked_sqla.loading import instances


async def orm_execute_statement(
    conn: AsyncConnection,
    statement: Executable,
    parameters: Optional[_CoreAnyExecuteParams] = None,
    execution_options: Optional[CoreExecuteOptionsParameter] = None,
):
    result = await conn.execute(
        statement,
        parameters=parameters,
        execution_options=execution_options,
    )
    execution_context = result.context
    assert execution_context.compiled
    compile_state = execution_context.compiled.compile_state
    assert compile_state

    # Arbitrary session instance, just to get the query context
    # TODO: maybe even refactor query context to not need a session
    sync_session = _Session(bind=conn.sync_connection)

    querycontext = QueryContext(
        compile_state,
        compile_state.statement,
        compile_state.statement,
        {},
        sync_session,
        QueryContext.default_load_options,
        execution_options,
        None,
    )
    return instances(result, querycontext)
