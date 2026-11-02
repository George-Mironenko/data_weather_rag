FROM python:3.11-slim

RUN pip install duckdb fastapi uvicorn duckdb openai

RUN python -`command` "import duckdb; duckdb.connect().execute('INSTALL httpfs;')"

WORKDIR /app

COPY main.py .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]