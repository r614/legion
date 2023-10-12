import asyncio
from threading import Thread
from time import sleep
import pytest
from legion.migrate import run_migration
from legion import Legion
import psycopg

from legion.utils.db import PostgresAsyncConnection, get_pool, get_with_conn_retry


url = "postgresql://legion@localhost:5432/legiontest"


@pytest.fixture(scope="session")
def conn():
    return psycopg.connect(url)


def test_migration(conn):
    run_migration(url)

    with conn.transaction():
        result = conn.execute("select * from legion.jobs").fetchall()
        assert result == []


def test_async_db():
    run_migration(url)

    worker = Legion(postgresql_conn_string=url)
    worker.add("test_job", "test_job_1")

    async def test():
        pool = get_pool(url, read_only=False)
        await pool.open()
        with_conn_retry = get_with_conn_retry(pool)

        @with_conn_retry
        async def nested_find(conn: PostgresAsyncConnection):
            cur = await conn.execute("select * from legion.jobs")
            return await cur.fetchall()

        res = await nested_find()
        await pool.close()
        return res

    res = asyncio.run(test())
    assert len(res) == 1


def test_job(conn):
    run_migration("postgresql://legion@localhost:5432/legiontest")

    worker = Legion(
        postgresql_conn_string="postgresql://legion@localhost:5432/legiontest"
    )

    @worker.register("test_job")
    def test_job(payload):
        print("test_job")

    j_id = worker.add("test_job", "test_job_1")

    worker_thread = Thread(target=worker.start_sync)
    worker_thread.start()
    sleep(2)

    worker.stop_sync()
    with conn.transaction():
        result = conn.execute(
            f"select status from legion.jobs where id = {j_id}"
        ).fetchone()

        assert result == ("completed",)
