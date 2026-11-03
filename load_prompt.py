from pathlib import Path


_PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"


def load_prompt(filename: str, **kwargs) -> str:
    """
    Загружает промпт из каталога prompts/.

    Если переданы kwargs — применяет .format(**kwargs).
    Если нет — возвращает текст как есть (фигурные скобки в SQL не ломаются).
    """
    prompt_path = _PROMPTS_DIR / filename
    if not prompt_path.is_file():
        raise FileNotFoundError(f"Prompt file not found: {prompt_path}")

    template = prompt_path.read_text(encoding="utf-8").strip()

    if kwargs:
        return template.format(**kwargs)
    return template