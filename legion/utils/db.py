import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
import functools
import random
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Concatenate,
    Optional,
    ParamSpec,
    TypeVar,
    cast,
)
from psycopg import (
    AsyncConnection,
    AsyncCursor,
    DatabaseError,
    IsolationLevel,
    OperationalError,
    sql,
)
from psycopg.rows import Row, AsyncRowFactory, kwargs_row, dict_row
from psycopg_pool import AsyncConnectionPool
from psycopg.errors import Error as PGError
from psycopg.errors import (
    AdminShutdown,
    CannotConnectNow,
    ConnectionDoesNotExist,
    ConnectionException,
    ConnectionFailure,
    CrashShutdown,
    DatabaseError,
    DeadlockDetected,
    DiskFull,
    DuplicateFile,
)
from psycopg.errors import (
    IdleSessionTimeout,
    InsufficientResources,
    IoError,
    OperationalError,
    OperatorIntervention,
    OutOfMemory,
    ProtocolViolation,
    QueryCanceled,
    SerializationFailure,
    SqlclientUnableToEstablishSqlconnection,
    SqlserverRejectedEstablishmentOfSqlconnection,
    SystemError,
    TooManyConnections,
    UndefinedFile,
)
from random import randint


class CABackoff:
    """Collision-avoidance Exponential Backoff

    Non-deterministically chooses a re-transmission slot with exponential backoff
    See: https://en.wikipedia.org/wiki/Exponential_backoff#Collision_avoidance

    The expected delay is `delay_quant / 2 * (2^retries - 1)`
    """

    def __init__(self, delay_quant: float, max_wait_time: float | None):
        self._retries = 0

        self._acc_wait_time = 0
        self._max_wait_time = max_wait_time

        self._delay_quant = delay_quant

        self._slot_exp = 1

    @property
    def retries(self):
        return self._retries

    @property
    def acc_wait_time(self):
        return self._acc_wait_time

    def retry(self) -> float | None:
        if (
            self._max_wait_time is not None
            and self._max_wait_time - self.acc_wait_time < 0.001
        ):
            return

        self._slot_exp *= 2
        slot = randint(0, self._slot_exp - 1)
        delay = slot * self._delay_quant

        if (
            self._max_wait_time is not None
            and self._acc_wait_time + delay > self._max_wait_time
        ):
            delay = self._max_wait_time - self._acc_wait_time

        self._retries += 1
        self._acc_wait_time += delay

        return delay


class PostgresAsyncConnection(AsyncConnection[Row]):
    ...


T = TypeVar("T")
P = ParamSpec("P")


def with_conn_retry(
    f: Callable[Concatenate[PostgresAsyncConnection[Any], P], Awaitable[T]],
    pool: AsyncConnectionPool,
) -> Callable[P, Awaitable[T]]:
    @functools.wraps(f)
    async def inner(*args: P.args, **kwargs: P.kwargs):
        try:
            retries = 0
            accum_retry_time = 0
            while True:
                try:
                    async with pool.connection() as conn:
                        backoff = CABackoff(
                            0.05,
                            30,
                        )
                        while True:
                            try:
                                async with conn.transaction():
                                    res = await f(conn, *args, **kwargs)
                                    return res
                            except (
                                # Class 40 - Transaction Rollback
                                SerializationFailure,
                                DeadlockDetected,
                            ) as e:
                                # Retry with the same connection
                                delay = backoff.retry()

                                if delay is None:
                                    raise e

                                await conn.rollback()
                                await asyncio.sleep(delay)
                except (
                    # Class 08 - Connection Exception
                    ConnectionException,
                    ConnectionDoesNotExist,
                    ConnectionFailure,
                    SqlclientUnableToEstablishSqlconnection,
                    SqlserverRejectedEstablishmentOfSqlconnection,
                    ProtocolViolation,
                    # Class 53 - Insufficient Resources
                    InsufficientResources,
                    DiskFull,
                    OutOfMemory,
                    TooManyConnections,
                    # Class 57 - Operator Intervention
                    OperatorIntervention,
                    QueryCanceled,
                    AdminShutdown,
                    CrashShutdown,
                    CannotConnectNow,
                    IdleSessionTimeout,
                    # Class 58 - System Error (errors external to PostgreSQL itself)
                    SystemError,
                    IoError,
                    UndefinedFile,
                    DuplicateFile,
                    OperationalError,
                ) as e:
                    # Connection is dead. Get a new one and retry
                    retries += 1

                    if accum_retry_time > 2:
                        raise

                    if retries == 1:
                        delay = 0
                    else:
                        delay = random.uniform(1, 2)

                    if delay != 0:
                        await asyncio.sleep(delay)
                        accum_retry_time += delay
        except DatabaseError as e:
            raise e

    return inner


def get_with_conn_retry(
    pool: AsyncConnectionPool,
) -> Callable[
    [Callable[Concatenate[PostgresAsyncConnection[Any], P], Awaitable[T]]],
    Callable[P, Awaitable[T]],
]:
    return functools.partial(with_conn_retry, pool=pool)


async def reset_conn(
    x: AsyncConnection[object],
    read_only: bool = False,
    isolation_level: IsolationLevel = IsolationLevel.SERIALIZABLE,
):
    x.prepare_threshold = 0

    if x.read_only != read_only:
        await x.set_read_only(read_only)

    if x.isolation_level != isolation_level:
        await x.set_isolation_level(isolation_level)


def get_pool(
    conninfo: str,
    read_only: bool = True,
    isolation_level: IsolationLevel = IsolationLevel.SERIALIZABLE,
) -> AsyncConnectionPool:
    return AsyncConnectionPool(
        conninfo=conninfo,
        min_size=1,
        max_size=3,
        timeout=timedelta(seconds=5) / timedelta(seconds=1),
        open=False,
        configure=functools.partial(
            reset_conn, read_only=read_only, isolation_level=isolation_level
        ),
        reset=functools.partial(
            reset_conn, read_only=read_only, isolation_level=isolation_level
        ),
        connection_class=PostgresAsyncConnection,
    )
