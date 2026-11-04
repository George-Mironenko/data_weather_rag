import re


def clean_sql(raw: str) -> str:
    text = raw.strip()
    text = re.sub(r"^```(?:sql)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    return text.strip().rstrip(";").strip()