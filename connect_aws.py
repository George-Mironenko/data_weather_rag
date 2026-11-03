from asyncio import to_thread
from fastapi import FastAPI, HTTPException

from Asks_models import AskRequest, AskResponse
from is_safe_sql import is_safe_sql
from load_prompt import load_prompt

from llm import generate_sql
from sql_utils import clean_sql
from db import _run_query

app = FastAPI(title="Weather Text-to-SQL API")
SYSTEM_PROMPT = f"""Ты — эксперт по DuckDB SQL. Схема данных:
{load_prompt('prompts/text2sql_shema.txt')}

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
        data = await to_thread(_run_query, sql)
    except Exception as e:
        return AskResponse(sql=sql, data=[], error=f"Ошибка выполнения SQL: {e}")

    return AskResponse(sql=sql, data=data)

@app.get("/health")
def health():
    return {"status": "ok"}