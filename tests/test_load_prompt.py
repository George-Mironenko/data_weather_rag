import pytest

from app.utils.prompts import load_prompt, _PROMPTS_DIR


@pytest.fixture
def prompts_dir(tmp_path, monkeypatch):
    """
    Подменяет _PROMPTS_DIR на tmp_path, чтобы тесты не зависели
    от реальных файлов в проекте и не писали в репозиторий.
    """
    monkeypatch.setattr("app.utils.prompts._PROMPTS_DIR", tmp_path)
    return tmp_path



def test_loads_file_without_kwargs(prompts_dir):
    """Файл читается как есть, без .format()."""
    (prompts_dir / "plain.txt").write_text("SELECT 1", encoding="utf-8")

    result = load_prompt("plain.txt")

    assert result == "SELECT 1"


def test_strips_trailing_whitespace(prompts_dir):
    """Trailing newline и пробелы от редактора убираются."""
    (prompts_dir / "trailing.txt").write_text("SELECT 1\n\n  \n", encoding="utf-8")

    result = load_prompt("trailing.txt")

    assert result == "SELECT 1"
    assert not result.endswith("\n")



def test_curly_braces_preserved_when_no_kwargs(prompts_dir):
    """
    Ключевой кейс: SQL-пример с { } в промпте не должен падать
    с KeyError, если kwargs не переданы.
    """
    sql_with_braces = "SELECT STRUCT{'a': 1} FROM read_parquet('s3://x')"
    (prompts_dir / "braces.txt").write_text(sql_with_braces, encoding="utf-8")

    result = load_prompt("braces.txt")

    assert result == sql_with_braces


def test_real_prompt_with_read_parquet_not_broken(prompts_dir):
    """
    Пример из задачи: промпт содержит read_parquet('...city=*/data.parquet').
    Без kwargs фигурные скобки не должны интерпретироваться.
    """
    prompt = (
        "Используй read_parquet('s3://weather-data/raw/open-meteo/"
        "city=*/data.parquet', hive_partitioning=true) как источник."
    )
    (prompts_dir / "text2sql_system.txt").write_text(prompt, encoding="utf-8")

    result = load_prompt("text2sql_system.txt")

    assert result == prompt
    assert "{*}" in result or "city=*/" in result


# ---------- 3. Подстановка kwargs ----------

def test_formats_with_kwargs(prompts_dir):
    """При наличии kwargs применяется .format()."""
    (prompts_dir / "templated.txt").write_text(
        "Схема: {schema}\nСегодня: {today}", encoding="utf-8"
    )

    result = load_prompt("templated.txt", schema="date, city", today="2026-11-03")

    assert result == "Схема: date, city\nСегодня: 2026-11-03"


def test_missing_kwarg_raises_key_error(prompts_dir):
    """
    Если в шаблоне есть {schema}, а kwargs не передан —
    .format() бросит KeyError. Это ожидаемое поведение,
    тест фиксирует контракт.
    """
    (prompts_dir / "templated.txt").write_text("Схема: {schema}", encoding="utf-8")

    with pytest.raises(KeyError):
        load_prompt("templated.txt", other="x")


def test_extra_kwargs_are_ignored(prompts_dir):
    """Лишние kwargs не ломают .format() — Python их игнорирует."""
    (prompts_dir / "templated.txt").write_text("Схема: {schema}", encoding="utf-8")

    result = load_prompt("templated.txt", schema="date", unused="x")

    assert result == "Схема: date"


# ---------- 4. Ошибки ----------

def test_missing_file_raises_file_not_found(prompts_dir):
    """Отсутствующий файл → FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_prompt("does_not_exist.txt")


def test_error_message_contains_full_path(prompts_dir):
    """
    Сообщение об ошибке содержит полный путь — это критично
    для отладки в Docker, где CWD непредсказуем.
    """
    with pytest.raises(FileNotFoundError) as exc_info:
        load_prompt("missing.txt")

    assert "missing.txt" in str(exc_info.value)
    assert str(prompts_dir) in str(exc_info.value)


# ---------- 5. Пустой файл ----------

def test_empty_file_returns_empty_string(prompts_dir):
    """Пустой файл — валидный случай, возвращается пустая строка."""
    (prompts_dir / "empty.txt").write_text("", encoding="utf-8")

    result = load_prompt("empty.txt")

    assert result == ""


def test_whitespace_only_file_returns_empty_string(prompts_dir):
    """.strip() превращает файл из пробелов в пустую строку."""
    (prompts_dir / "spaces.txt").write_text("   \n\t\n", encoding="utf-8")

    result = load_prompt("spaces.txt")

    assert result == ""


# ---------- 6. Путь относительно модуля, а не CWD ----------

def test_path_is_relative_to_module_not_cwd(prompts_dir, monkeypatch, tmp_path_factory):
    """
    Ключевой контракт: даже если CWD другой, функция находит файл
    в _PROMPTS_DIR (который привязан к файлу модуля).
    """
    (prompts_dir / "x.txt").write_text("ok", encoding="utf-8")

    other_cwd = tmp_path_factory.mktemp("elsewhere")
    monkeypatch.chdir(other_cwd)

    result = load_prompt("x.txt")

    assert result == "ok"