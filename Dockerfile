FROM python:3.11-slim

WORKDIR /app

# Системные зависимости (нужны для сборки некоторых колёс)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Зависимости отдельным слоем — кэшируются между сборками
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Предзагрузка httpfs, чтобы первый запрос не висел
RUN python -c "import duckdb; duckdb.connect().execute('INSTALL httpfs;')"

# Код приложения — весь пакет, а не один файл
COPY app/ ./app/
COPY prompts/ ./prompts/

# Важно: app.main:app, а не main:app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]