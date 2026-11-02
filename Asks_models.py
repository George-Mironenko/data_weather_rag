from pydantic import BaseModel


class AskRequest(BaseModel):
    question: str


class AskResponse(BaseModel):
    sql: str
    data: list[dict]
    error: str | None = None
