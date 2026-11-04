from os import getenv
import json

import duckdb


S3_KEY = getenv("S3_KEY")
S3_SECRET = getenv("S3_SECRET")

con = duckdb.connect(":memory:")

con.execute("INSTALL httpfs; LOAD httpfs;")

if not S3_KEY or not S3_SECRET:
    raise RuntimeError("S3_KEY и S3_SECRET обязательны")

con.execute("""
    CREATE OR REPLACE SECRET selectel (
        TYPE S3,
        KEY_ID ?,
        SECRET ?,
        REGION 'ru-1',
        ENDPOINT 's3.ru-1.storage.selcloud.ru',
        URL_STYLE 'path',
        USE_SSL true
    )
""", [S3_KEY, S3_SECRET])

LIMIT_ROWS = 10_000

def _run_query(sql: str) -> list[dict]:
    safe_sql = f"SELECT * FROM ({sql}) AS _sub LIMIT {LIMIT_ROWS}"
    cur = con.cursor()
    try:
        df = cur.execute(safe_sql).fetchdf()
        return json.loads(df.to_json(orient="records", date_format="iso"))
    finally:
        cur.close()