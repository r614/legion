import asyncio
from concurrent.futures import (
    ALL_COMPLETED,
    Future,
    ThreadPoolExecutor,
    wait,
)
import json
import traceback
from typing import Any, Awaitable, Callable, Mapping, Union
import psycopg
from psycopg import sql
import uuid
from dataclasses import dataclass
from datetime import datetime
from psycopg_pool import ConnectionPool

from multiprocessing import get_logger


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


def get_job(conn: psycopg.Connection, queue_id: int) -> Job | None:
    query = sql.SQL(
        """
        select
            *
        from 
            legion.get_next_job({queue_id})
    """
    ).format(queue_id=sql.Literal(queue_id))

    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()

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


def fetch_and_start_job(
    pool: ConnectionPool,
    job_queue_id: int,
    callback: Callable[[Any], Any],
    raise_errors: bool = False,
) -> None:
    logger = get_logger()

    query_conn = pool.getconn()
    job = get_job(query_conn, job_queue_id)

    logger.info(f"Got job {job}")
    if job is None:
        return

    logger.info(f"Starting job with id {job.id}")

    with query_conn.transaction(f"job_{job.id}_{job.attempts + 1}"):
        query_conn.execute(
            sql.SQL("select pg_advisory_xact_lock({job_id})").format(
                job_id=sql.Literal(job.id)
            )
        )

        try:
            callback(job.payload)

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

            query_conn.execute(success_query)

        except Exception as e:
            error = traceback.format_exc()
            traceback.print_exc()

            error_query = sql.SQL(
                """
                select * from legion.fail_job({job_id}, {error})
                """
            ).format(job_id=sql.Literal(job.id), error=sql.Literal(error))

            query_conn.execute(error_query)
            if raise_errors:
                raise e


def subscribe(
    url: str,
    queue_name: str,
    callback: Union[Callable[[Any], Any], Callable[[Any], Awaitable[None]]],
    raise_errors: bool = False,
    concurrency: int = 1,
):
    logger = get_logger()

    pool = ConnectionPool(url)
    notify_conn = pool.getconn()

    process_pool = ThreadPoolExecutor(max_workers=concurrency)

    job_queue_id = notify_conn.execute(
        sql.SQL("select * from legion.get_or_create_job_queue({queue_name})").format(
            queue_name=sql.Literal(queue_name)
        )
    ).fetchone()[0]

    notify_conn.execute(
        sql.SQL("listen legion_queue_{job_queue_id}").format(
            job_queue_id=sql.Literal(job_queue_id)
        )
    )

    processing_jobs = set()

    logger.info(f"Listening to legion_queue_{job_queue_id} for queue {queue_name}")

    processing_jobs.add(
        process_pool.submit(
            fetch_and_start_job,
            pool,
            job_queue_id,
            callback,
            raise_errors=raise_errors,
        )
    )

    for _ in notify_conn.notifies():
        if len(processing_jobs) >= concurrency:
            _, processing_jobs = wait(
                processing_jobs, return_when=asyncio.FIRST_COMPLETED
            )

            for job in processing_jobs:
                if job.done():
                    job.result()

        processing_jobs.add(
            process_pool.submit(
                fetch_and_start_job,
                pool,
                job_queue_id,
                callback,
                raise_errors=raise_errors,
            )
        )


class Legion:
    def __init__(
        self,
        postgresql_conn_string: str,
        raise_errors: bool = False,
    ):
        self.handlers: Mapping[str, Callable[[Any], Any]] = {}
        self.url = postgresql_conn_string

        self.raise_errors = raise_errors

        self.queue_tasks: set[Future] = set()
        self.thread_pool = ThreadPoolExecutor()

        self.logger = get_logger()

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
        conn = psycopg.connect(self.url)
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

        with conn.transaction():
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

    def start(self, concurrency: int = 1):
        for queue_name, handler in self.handlers.items():
            self.logger.info(f"Starting handler for queue {queue_name}")
            self.queue_tasks.add(
                self.thread_pool.submit(
                    subscribe,
                    self.url,
                    queue_name,
                    handler,
                    raise_errors=self.raise_errors,
                    concurrency=concurrency,
                )
            )
        wait(self.queue_tasks, return_when=ALL_COMPLETED)

        for task in self.queue_tasks:
            task.result()

    def stop(self):
        self.thread_pool.shutdown(wait=True)
