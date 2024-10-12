import asyncio
import random
import time
import tracemalloc
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from statistics import median
from typing import Literal, Union
from uuid import uuid4

import psutil
import sqlalchemy as sa
from rich.console import Console
from rich.table import Table
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.ext.asyncio import AsyncSession as sa_AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column
from typing_extensions import assert_never

from naked_sqla.om.asession import AsyncSession, AsyncSessionFactory

DB_URL = "sqlite+aiosqlite:///test.db"
engine = create_async_engine(DB_URL, echo=False)


Methods = Literal["Naked SQLAlchemy", "SQLAlchemy Core", "SQLAlchemy ORM"]
SQL_EXECUTION_TIMES: list[float] = []


class BaseSQL(MappedAsDataclass, DeclarativeBase): ...


@sa.event.listens_for(engine.sync_engine, "before_cursor_execute")
def _record_query_start(conn, cursor, statement, parameters, context, executemany):
    conn.info["query_start"] = datetime.now()


@sa.event.listens_for(engine.sync_engine, "after_cursor_execute")
def _calculate_query_run_time(
    conn, cursor, statement, parameters, context, executemany
):
    final_time = (datetime.now() - conn.info["query_start"]).total_seconds()
    SQL_EXECUTION_TIMES.append(final_time)


class E1(BaseSQL):
    __tablename__ = "E1"
    event: Mapped[str] = mapped_column(sa.String(), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True))
    id: Mapped[str] = mapped_column(
        primary_key=True, default_factory=lambda: str(uuid4())
    )
    author_id: Mapped[str] = mapped_column(
        primary_key=True, default_factory=lambda: str(uuid4())
    )


@asynccontextmanager
async def init_naked_sqla_db():
    """Initialize Naked SQLAlchemy Session"""
    db = AsyncSessionFactory(engine)
    yield db


@asynccontextmanager
async def init_sqlalchemy_orm_db():
    """Initialize SQLAlchemy ORM Session"""
    async_session = async_sessionmaker(engine, expire_on_commit=False)
    yield async_session


async def refetch_all(session: Union[AsyncSession, sa_AsyncSession]):
    """Fetch all records using Naked SQLAlchemy and perform random updates using .update()"""
    result = (await session.scalars(sa.select(E1))).all()
    ids = [record.id for record in result]
    ids_to_update = random.sample(ids, min(10, len(ids)))
    new_event = f"Updated event {random.randint(1, 100)}"
    (
        await session.execute(
            sa.update(E1)
            .where(E1.id.in_(ids_to_update))
            .values(event=new_event)
            .returning(E1)
        )
    ).all()
    return len(result)


async def refetch_with_conn(conn: AsyncConnection):
    result = (await conn.execute(sa.select(E1))).mappings().all()
    ids = [record["id"] for record in result]
    ids_to_update = random.sample(ids, min(10, len(ids)))
    new_event = f"Updated event {random.randint(1, 100)}"
    (
        await conn.execute(
            sa.update(E1)
            .where(E1.id.in_(ids_to_update))
            .values(event=new_event)
            .returning(E1)
        )
    ).all()
    return len(result)


@dataclass
class BenchmarkResult:
    method: Methods
    rows_fetched: int
    execution_time: float
    memory_used_mb: float
    rss_memory_mb: float
    python_execution_time: float
    sql_execution_time: float


async def run_benchmark(method: Methods) -> BenchmarkResult:
    tracemalloc.start()
    process = psutil.Process()
    cpu_before = time.process_time()

    if method == "SQLAlchemy ORM":
        async with init_sqlalchemy_orm_db() as db:
            async with db.begin() as session:
                count = await refetch_all(session)

    elif method == "Naked SQLAlchemy":
        async with init_naked_sqla_db() as db:
            async with db.begin() as session:
                count = await refetch_all(session)

    elif method == "SQLAlchemy Core":
        async with engine.begin() as conn:
            count = await refetch_with_conn(conn)

    else:
        assert_never(method)

    cpu_after = time.process_time()
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    mem_info = process.memory_info()

    execution_time = cpu_after - cpu_before

    mem_used_mb = peak / (1024 * 1024)
    rss_mem_mb = mem_info.rss / (1024 * 1024)

    query1_execution_tim = SQL_EXECUTION_TIMES.pop()
    query2_execution_time = SQL_EXECUTION_TIMES.pop()
    sql_execution_time = query1_execution_tim + query2_execution_time
    python_execution_time = execution_time - sql_execution_time

    return BenchmarkResult(
        method=method,
        rows_fetched=count,
        execution_time=execution_time,
        memory_used_mb=mem_used_mb,
        rss_memory_mb=rss_mem_mb,
        python_execution_time=python_execution_time,
        sql_execution_time=sql_execution_time,
    )


async def benchmark():
    """Run the benchmark for both Naked SQLAlchemy and SQLAlchemy ORM multiple times and compute median"""
    # methods: list[Methods] = ["Naked SQLAlchemy", "SQLAlchemy ORM", "SQLAlchemy Core"]
    methods: list[Methods] = ["SQLAlchemy ORM", "Naked SQLAlchemy", "SQLAlchemy Core"]
    num_runs = 5
    all_results = {method: [] for method in methods}
    console = Console()

    for method in methods:
        console.print(f"\n[bold cyan]Running benchmark for {method}[/bold cyan]")
        for i in range(num_runs):
            result = await run_benchmark(method)
            all_results[method].append(result)
            console.print(
                f"Run {i + 1}/{num_runs}: "
                f"CPU Time: {result.execution_time:.2f}s, "
                f"Memory Used: {result.memory_used_mb:.2f}MB, "
            )

    median_results: list[BenchmarkResult] = []

    for method in methods:
        method_results = all_results[method]
        # Assuming rows fetched is the same each time
        rows_fetched = method_results[0].rows_fetched
        median_execution_time = median([r.execution_time for r in method_results])
        median_memory_used_mb = median([r.memory_used_mb for r in method_results])
        median_rss_memory_mb = median([r.rss_memory_mb for r in method_results])
        python_execution_time = median(
            [r.python_execution_time for r in method_results]
        )
        sql_execution_time = median([r.sql_execution_time for r in method_results])

        median_results.append(
            BenchmarkResult(
                method=method,
                rows_fetched=rows_fetched,
                execution_time=median_execution_time,
                memory_used_mb=median_memory_used_mb,
                rss_memory_mb=median_rss_memory_mb,
                python_execution_time=python_execution_time,
                sql_execution_time=sql_execution_time,
            )
        )

    table = Table(title="Benchmark Median Results")
    table.add_column("Method", justify="left", style="cyan", no_wrap=True)
    table.add_column("Rows Fetched", justify="right", style="magenta")
    table.add_column("Execution Time (s)", justify="right", style="green")
    table.add_column("SQL Execution time (s)", justify="right", style="green")
    table.add_column("Python Execution time (s)", justify="right", style="green")
    table.add_column("Memory Used (MB)", justify="right", style="green")
    table.add_column("RSS Memory (MB)", justify="right", style="green")

    for result in median_results:
        table.add_row(
            result.method,
            str(result.rows_fetched),
            f"{result.execution_time:.2f}",
            f"{result.sql_execution_time:.2f}",
            f"{result.python_execution_time:.2f}",
            f"{result.memory_used_mb:.2f}",
            f"{result.rss_memory_mb:.2f}",
        )

    console.print("\n", table)


if __name__ == "__main__":
    asyncio.run(benchmark())
