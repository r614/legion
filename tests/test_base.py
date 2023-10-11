import asyncio
from threading import Thread
from time import sleep
import pytest
from legion.migrate import run_migration
from legion import Legion
import psycopg


@pytest.fixture(scope="session")
def conn():
    return psycopg.connect("postgresql://legion@localhost:5432/legiontest")


def test_migration(conn):
    run_migration("postgresql://legion@localhost:5432/legiontest")

    with conn.transaction():
        result = conn.execute("select * from legion.jobs").fetchall()
        assert result == []


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

    worker.stop()
    with conn.transaction():
        result = conn.execute(
            f"select status from legion.jobs where id = {j_id}"
        ).fetchone()

        assert result == ("completed",)
