import sys
import psycopg
from pathlib import Path
import traceback

sql = (Path(__file__).parent / "core.sql").read_text()


def run_migration(postgresql_conn_string: str):
    try:
        conn = psycopg.connect(postgresql_conn_string)

        with conn.transaction():
            conn.execute(sql)
        return True

    except Exception as e:
        print("Failed to run migration")
        traceback.print_exc()
        return False
