import os
from asyncio import to_thread

import duckdb
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from openai import AsyncOpenAI, APIConnectionError, APITimeoutError, APIStatusError

app = FastAPI(title="Weather Text-to-SQL API")

# --- Настройки ---
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434/v1")
MODEL_NAME = os.getenv("MODEL_NAME", "distil-qwen3-4b-text2sql")

# S3 настройки (лучше вынести в переменные окружения на сервере)
S3_KEY = os.getenv("S3_KEY")
S3_SECRET = os.getenv("S3_SECRET")

# --- Клиент для Ollama ---
aclient = AsyncOpenAI(
    base_url=OLLAMA_BASE_URL,
    api_key="ollama",
    timeout=60.0,
)

# --- DuckDB подключение (создаём один раз при старте) ---
con = duckdb.connect(":memory:")
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"""
    CREATE OR REPLACE SECRET selectel (
        TYPE S3,
        KEY_ID '{S3_KEY}',
        SECRET '{S3_SECRET}',
        REGION 'ru-1',
        ENDPOINT 's3.ru-1.storage.selcloud.ru',
        URL_STYLE 'path',
        USE_SSL true
    );
""")

# --- Схема для промпта ---
SCHEMA = """
Таблица находится в S3:
read_parquet('s3://weather-data/raw/open-meteo/city=*/data.parquet', filename=true)

Колонки в файлах:
- date (DATE)
- temperature_2m_max (DOUBLE)
- temperature_2m_min (DOUBLE)
- precipitation_sum (DOUBLE)
- wind_speed_10m_max (DOUBLE)

Колонка city извлекается из пути (Hive partition), её можно использовать в WHERE.
"""

SYSTEM_PROMPT = f"""Ты — эксперт по DuckDB SQL. Схема данных:
{SCHEMA}

Сгенерируй ТОЛЬКО SQL-запрос для DuckDB без объяснений и без markdown-разметки.
Запрос должен быть безопасным (только SELECT или WITH).
"""

class AskRequest(BaseModel):
    question: str

class AskResponse(BaseModel):
    sql: str
    data: list[dict]
    error: str | None = None


def is_safe_sql(sql: str) -> bool:
    """Разрешаем только SELECT и WITH."""
    sql_upper = sql.strip().upper()
    return sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")


@app.post("/ask", response_model=AskResponse)
async def ask(request: AskRequest):
    # 1. Генерируем SQL через Ollama
    try:
        response = await aclient.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": request.question},
            ],
            temperature=0,
        )
        sql = response.choices[0].message.content.strip()
    except APIConnectionError:
        # Ollama не запущена / сеть недоступна → 503
        raise HTTPException(503, "Ollama недоступна")
    except APITimeoutError:
        # Тайм-аут → 504
        raise HTTPException(504, "Тайм-аут модели")
    except APIStatusError as e:
        # Ollama вернула ошибку (например, модель не найдена) → 502
        raise HTTPException(502, f"Ошибка Ollama: {e}")

    # Убираем возможные markdown-обёртки
    if sql.startswith("```"):
        sql = sql.split("\n", 1)[1] if "\n" in sql else sql
        sql = sql.rsplit("```", 1)[0].strip()

    # 2. Проверка безопасности
    if not is_safe_sql(sql):
        return AskResponse(sql=sql, data=[], error="Сгенерированный SQL не является безопасным (только SELECT/WITH).")

    # 3. Выполняем через DuckDB
    try:
        result = await to_thread(con.execute(sql).fetchdf())
        # Преобразуем DataFrame в список словарей для JSON-ответа
        data = await to_thread(result.to_dict(orient="records"))
        return AskResponse(sql=sql, data=data)
    except Exception as e:
        return AskResponse(sql=sql, data=[], error=f"Ошибка выполнения SQL: {e}")


@app.get("/health")
def health():
    return {"status": "ok"}