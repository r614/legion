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


async def get_job(conn: psycopg.AsyncConnection, queue_id: int) -> Job | None:
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


async def fetch_and_start_job(
    query_conn: psycopg.AsyncConnection,
    job_queue_id: int,
    callback: Callable[[Any], Any],
    raise_errors: bool = False,
) -> None:
    job = await get_job(query_conn, job_queue_id)

    print(f"Got job {job}")
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


async def subscribe(
    url: str,
    queue_name: str,
    callback: Union[Callable[[Any], Any], Callable[[Any], Awaitable[None]]],
    raise_errors: bool = False,
    concurrency: int = 1,
):
    async with await psycopg.AsyncConnection.connect(
        url, autocommit=True
    ) as notify_conn:
        async with await psycopg.AsyncConnection.connect(
            url, autocommit=True
        ) as query_conn:
            job_queue_exec = await notify_conn.execute(
                sql.SQL(
                    "select id from legion.job_queues where queue_name = {queue_name}"
                ).format(queue_name=sql.Literal(queue_name))
            )

            job_queue_id = await job_queue_exec.fetchone()

            if job_queue_id is not None:
                job_queue_id = job_queue_id[0]

            if job_queue_id is None:
                jq_insert_exec = await notify_conn.execute(
                    sql.SQL(
                        "insert into legion.job_queues (queue_name) values ({queue_name}) returning id"
                    ).format(queue_name=sql.Literal(queue_name)),
                )
                job_queue_id = (await jq_insert_exec.fetchone())[0]

            await notify_conn.execute(
                sql.SQL("listen legion_queue_{job_queue_id}").format(
                    job_queue_id=sql.Literal(job_queue_id)
                )
            )

            print(f"Listening to legion_queue_{job_queue_id} for queue {queue_name}")

            tasks = set()

            init_task = asyncio.create_task(
                fetch_and_start_job(
                    query_conn,
                    job_queue_id,
                    callback,
                    raise_errors=raise_errors,
                ),
            )

            init_task.add_done_callback(tasks.discard)
            tasks.add(init_task)

            async for _ in notify_conn.notifies():
                if len(tasks) < concurrency:
                    task = asyncio.create_task(
                        fetch_and_start_job(
                            query_conn,
                            job_queue_id,
                            callback,
                            raise_errors=raise_errors,
                        ),
                    )

                    task.add_done_callback(tasks.discard)
                    tasks.add(task)


class Legion:
    def __init__(
        self,
        postgresql_conn_string: str,
        raise_errors: bool = False,
    ):
        self.postgresql_conn_string = postgresql_conn_string
        self.handlers: Mapping[
            str, Union[Callable[[Any], Any], Callable[[Any], Awaitable[None]]]
        ] = {}
        self.raise_errors = raise_errors
        self.terminate = False
        self.queue_tasks: set[asyncio.Task] = set()

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

    async def start(self, concurrency: int = 1):
        for queue_name, handler in self.handlers.items():
            q_task = asyncio.create_task(
                subscribe(
                    self.postgresql_conn_string,
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
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.start(concurrency=concurrency))

    def stop(self):
        self.terminate = True
        for x in self.queue_tasks:
            try:
                x.cancel()
            except asyncio.CancelledError:
                pass
