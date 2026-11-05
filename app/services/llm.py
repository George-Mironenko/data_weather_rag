from os import getenv

from openai import AsyncOpenAI, APIConnectionError, APITimeoutError, APIStatusError
from fastapi import HTTPException


aclient = AsyncOpenAI(
    base_url=getenv("OLLAMA_BASE_URL", "http://ollama:11434/v1"),
    api_key="ollama",
    timeout=60.0,
)

MODEL_NAME = getenv("MODEL_NAME", "distil-qwen3-4b-text2sql")


async def generate_sql(system_prompt: str, question: str) -> str:
    try:
        response = await aclient.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": question},
            ],
            temperature=0,
            max_tokens=512,
        )
    except APIConnectionError:
        raise HTTPException(503, "Ollama недоступна")
    except APITimeoutError:
        raise HTTPException(504, "Тайм-аут модели")
    except APIStatusError as e:
        raise HTTPException(502, f"Ошибка Ollama: {e}")

    content = response.choices[0].message.content or ""
    if not content.strip():
        raise HTTPException(502, "Модель вернула пустой ответ")
    return content