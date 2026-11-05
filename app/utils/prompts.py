from pathlib import Path

# app/utils/prompts.py -> app/utils -> app -> корень проекта
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_PROMPTS_DIR = _PROJECT_ROOT / "prompts"


def load_prompt(filename: str, **kwargs) -> str:
    prompt_path = _PROMPTS_DIR / filename
    if not prompt_path.is_file():
        raise FileNotFoundError(f"Prompt file not found: {prompt_path}")
    template = prompt_path.read_text(encoding="utf-8").strip()
    if kwargs:
        return template.format(**kwargs)
    return template


