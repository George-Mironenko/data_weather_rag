from typing import Literal

from pydantic import BaseModel, Field


class AskRequest(BaseModel):
    question: str = Field(
        min_length=1,
        max_length=2000,
        description="Вопрос на естественном языке для генерации SQL",
        examples=["Какая максимальная температура была в Москве в январе 2024?"],
    )


class AskResponse(BaseModel):
    sql: str | None = Field(
        default=None,
        description="Сгенерированный SQL (None, если генерация не удалась)",
    )
    data: list[dict] = Field(
        default_factory=list,
        description="Результат выполнения SQL",
    )
    error: str | None = Field(
        default=None,
        description="Текст ошибки для пользователя",
    )
    error_code: Literal["unsafe_sql", "sql_execution", "llm_error"] | None = Field(
        default=None,
        description="Машиночитаемый код ошибки",
    )
