from __future__ import annotations

from typing import Any, Optional, Type, TypeVar, Union

from sqlalchemy import util
from sqlalchemy.engine.interfaces import (
    _CoreAnyExecuteParams,
    _CoreKnownExecutionOptions,
    _CoreSingleExecuteParams,
)
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.orm.context import FromStatement, ORMCompileState
from sqlalchemy.sql.base import CompileState, Executable, Options
from sqlalchemy.sql.selectable import Select, SelectLabelStyle

from naked_sqla.loading import instances

_BindArguments = dict[str, Any]

_T = TypeVar("_T", bound=Any)


_EMPTY_DICT = util.immutabledict()


LABEL_STYLE_LEGACY_ORM = SelectLabelStyle.LABEL_STYLE_LEGACY_ORM


class QueryContext:
    runid: int

    compile_state: ORMCompileState

    class default_load_options(Options):
        _only_return_tuples = False
        _yield_per = None
        _sa_top_level_orm_context = None

    def __init__(
        self,
        compile_state: CompileState,
        statement: Union[Select[Any], FromStatement[Any]],
        user_passed_query: Union[
            Select[Any],
            FromStatement[Any],
        ],
        params: _CoreSingleExecuteParams,
        load_options: Union[
            Type[QueryContext.default_load_options],
            QueryContext.default_load_options,
        ],
        execution_options: Optional[_CoreKnownExecutionOptions] = None,
        bind_arguments: Optional[_BindArguments] = None,
    ):
        self.load_options = load_options
        self.execution_options = execution_options or _EMPTY_DICT
        self.bind_arguments = bind_arguments or _EMPTY_DICT
        self.compile_state = compile_state  # type: ignore
        self.query = statement

        # the query that the end user passed to Session.execute() or similar.
        # this is usually the same as .query, except in the bulk_persistence
        # routines where a separate FromStatement is manufactured in the
        # compile stage; this allows differentiation in that case.
        self.user_passed_query = user_passed_query

        self.params = params

        self.attributes = dict(compile_state.attributes)  # type: ignore
        self.yield_per = load_options._yield_per


async def orm_execute_statement(
    conn: AsyncConnection,
    statement: Executable,
    parameters: Optional[_CoreAnyExecuteParams] = None,
    execution_options: Optional[_CoreKnownExecutionOptions] = None,
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

    execution_options = execution_options or {}
    load_options = execution_options.get(  # type: ignore
        "_sa_orm_load_options", QueryContext.default_load_options
    )

    querycontext = QueryContext(
        compile_state,
        compile_state.statement,
        compile_state.statement,
        {},
        load_options,
        execution_options,
        None,
    )
    return instances(result, querycontext)
