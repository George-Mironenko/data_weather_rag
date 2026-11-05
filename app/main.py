from asyncio import to_thread

from fastapi import FastAPI

from app.api.models import AskRequest, AskResponse
from app.services.sql_safety import is_safe_sql
from app.services.sql import clean_sql
from app.services.llm import generate_sql
from app.utils.prompts import load_prompt
from app.db.duckdb_client import run_query


app = FastAPI(title="Weather Text-to-SQL API")

SYSTEM_PROMPT = (
    load_prompt("text2sql_system.txt")
    + "\n\nСхема данных:\n"
    + load_prompt("text2sql_schema.txt")
)

@app.post("/ask", response_model=AskResponse)
async def ask(request: AskRequest):
    raw = await generate_sql(SYSTEM_PROMPT, request.question)
    sql = clean_sql(raw)

    if not is_safe_sql(sql):
        return AskResponse(
            sql=sql,
            data=[],
            error="Сгенерированный SQL не является безопасным (только SELECT/WITH).",
        )

    try:
        data = await to_thread(run_query, sql)
    except Exception as e:
        return AskResponse(sql=sql, data=[], error=f"Ошибка выполнения SQL: {e}")

    return AskResponse(sql=sql, data=data)

@app.get("/health")
def health():
    return {"status": "ok"}