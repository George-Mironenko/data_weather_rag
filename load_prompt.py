from pathlib import Path


def load_prompt(filename: str, **kwargs) -> str:
    prompt_path = Path("prompts") / filename
    with open(prompt_path, "r", encoding="utf-8") as f:
        template = f.read()
    return template.format(**kwargs)
