import asyncio
import json
from multiprocessing import Pool
import traceback
from typing import Any, Awaitable, Callable, Mapping, Union
import psycopg
from psycopg import sql
import uuid
from dataclasses import dataclass
from datetime import datetime

from legion.utils.db import PostgresAsyncConnection, get_pool, get_with_conn_retry


@dataclass(frozen=True)
class Job:
    id: int
    task_id: str
    job_queue_id: int
    status: str
    payload: dict
    priority: int
    run_at: str
    attempts: int
    max_attempts: int
    last_error: str
    created_at: datetime
    updated_at: datetime


async def subscribe(
    pool: PostgresAsyncConnection,
    queue_name: str,
    callback: Union[Callable[[Any], Any], Callable[[Any], Awaitable[None]]],
    raise_errors: bool = False,
    concurrency: int = 1,
):
    with_conn_retry = get_with_conn_retry(pool)

    @with_conn_retry
    async def get_job_queue_id(conn: PostgresAsyncConnection) -> int:
        query = sql.SQL(
            """
            select
                *
            from 
                legion.get_or_create_job_queue({queue_name})
        """
        ).format(queue_name=sql.Literal(queue_name))

        cur = await conn.execute(query)
        row = await cur.fetchone()
        if row is None:
            return None

        return row[0]

    @with_conn_retry
    async def get_job(conn: PostgresAsyncConnection, queue_id: int) -> Job | None:
        query = sql.SQL(
            """
            select
                *
            from 
                legion.get_next_job({queue_id})
        """
        ).format(queue_id=sql.Literal(queue_id))

        async with conn.cursor() as cur:
            await cur.execute(query)
            row = await cur.fetchone()

            if row is None:
                return None

            return Job(
                id=row[0],
                task_id=row[1],
                job_queue_id=row[2],
                status=row[3],
                payload=row[4],
                priority=row[5],
                run_at=row[6],
                attempts=row[7],
                max_attempts=row[8],
                last_error=row[9],
                created_at=row[10],
                updated_at=row[11],
            )

    @with_conn_retry
    async def fetch_and_start_job(
        query_conn: PostgresAsyncConnection,
        job_queue_id: int,
        callback: Callable[[Any], Any],
        raise_errors: bool = False,
    ) -> None:
        job = await get_job(job_queue_id)

        if job is None:
            return

        print(f"Starting job with id {job.id}")

        async with query_conn.transaction(f"job_{job.id}_{job.attempts + 1}"):
            await query_conn.execute(
                sql.SQL("select pg_advisory_xact_lock({job_id})").format(
                    job_id=sql.Literal(job.id)
                )
            )

            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(job.payload)
                else:
                    await asyncio.to_thread(callback, job.payload)

                success_query = sql.SQL(
                    """
                    update 
                        legion.jobs
                    set
                        status = 'completed',
                        updated_at = now()
                    where 
                        id = {job_id}
                """
                ).format(job_id=sql.Literal(job.id))

                await query_conn.execute(success_query)

            except Exception as e:
                error = traceback.format_exc()
                print(error)

                error_query = sql.SQL(
                    """
                    select * from legion.fail_job({job_id}, {error})
                    """
                ).format(job_id=sql.Literal(job.id), error=sql.Literal(error))

                await query_conn.execute(error_query)
                if raise_errors:
                    raise e

    job_queue_id = await get_job_queue_id()

    print(f"Listening to legion_queue_{job_queue_id} for queue {queue_name}")

    tasks = set()

    init_task = asyncio.create_task(
        fetch_and_start_job(
            job_queue_id,
            callback,
            raise_errors=raise_errors,
        ),
    )

    init_task.add_done_callback(tasks.discard)
    tasks.add(init_task)

    @with_conn_retry
    async def start_listener(
        notify_conn: PostgresAsyncConnection,
        job_queue_id: int,
    ) -> None:
        await notify_conn.execute(
            sql.SQL("listen legion_queue_{job_queue_id}").format(
                job_queue_id=sql.Literal(job_queue_id)
            )
        )
        async for _ in notify_conn.notifies():
            if len(tasks) >= concurrency:
                continue

            task = asyncio.create_task(
                fetch_and_start_job(
                    job_queue_id,
                    callback,
                    raise_errors=raise_errors,
                ),
            )

            task.add_done_callback(tasks.discard)
            tasks.add(task)

    await start_listener(job_queue_id)


class Legion:
    def __init__(
        self,
        postgresql_conn_string: str,
        raise_errors: bool = False,
    ):
        self.postgresql_conn_string = postgresql_conn_string
        self.handlers: Mapping[
            str, Union[Callable[[Any], Any], Callable[[Any], Awaitable[None]]]
        ] = dict()

        self.raise_errors = raise_errors

        self.loop = asyncio.new_event_loop()
        self.master_task = None
        self.queue_tasks: set[asyncio.Task] = set()

        self.terminate = False
        self.pool = None

    def register(self, queue_name: str):
        def decorator(fn: Callable[[Any], Any]):
            self.handlers[queue_name] = fn
            return fn

        return decorator

    def add(
        self,
        job_queue_name: str,
        task_id: str,
        priority: int = 0,
        max_attempts: int = 25,
        payload: dict = {},
    ):
        with psycopg.connect(self.postgresql_conn_string, autocommit=True) as conn:
            job_queue_id = conn.execute(
                sql.SQL(
                    """
                    select 
                        * 
                    from 
                        legion.get_or_create_job_queue({job_queue_name}) 
                """
                ).format(job_queue_name=sql.Literal(job_queue_name))
            ).fetchone()[0]

            cur = conn.execute(
                sql.SQL(
                    """
                    insert into 
                        legion.jobs (
                            task_id,
                            job_queue_id,
                            priority,
                            max_attempts,
                            payload
                        )
                    values (
                        {task_id},
                        {job_queue_id},
                        {priority},
                        {max_attempts},
                        {payload}
                    )
                    returning 
                        id
                """
                ).format(
                    task_id=sql.Literal(task_id),
                    job_queue_id=sql.Literal(job_queue_id),
                    priority=sql.Literal(priority),
                    max_attempts=sql.Literal(max_attempts),
                    payload=sql.Literal(json.dumps(payload)),
                )
            )
            return cur.fetchone()[0]

    async def ensure_pool_running(self):
        if self.pool is None:
            self.pool = get_pool(
                self.postgresql_conn_string,
                read_only=False,
            )

        if not self.pool.open:
            await self.pool.open()

        await self.pool.wait()

    async def start(self, concurrency: int = 1):
        while self.pool is None or not self.pool.open:
            await self.ensure_pool_running()

        for queue_name, handler in self.handlers.items():
            q_task = asyncio.create_task(
                subscribe(
                    self.pool,
                    queue_name,
                    handler,
                    raise_errors=self.raise_errors,
                    concurrency=concurrency,
                ),
            )
            q_task.add_done_callback(self.queue_tasks.discard)
            self.queue_tasks.add(
                q_task,
            )

        try:
            await asyncio.gather(*self.queue_tasks)
        except asyncio.CancelledError:
            if self.terminate:
                return

    def start_sync(self, concurrency: int = 1):
        f = asyncio.run_coroutine_threadsafe(
            self.start(concurrency=concurrency), loop=self.loop
        )

        f.result()

    async def stop(self):
        self.terminate = True

        if self.pool is not None and self.pool.open:
            await self.pool.close()

        self.master_task.cancel()
        for x in [*self.queue_tasks, self.master_task]:
            try:
                x.cancel()
            except asyncio.CancelledError:
                pass

    def stop_sync(self):
        self.loop.run_until_complete(self.stop())
