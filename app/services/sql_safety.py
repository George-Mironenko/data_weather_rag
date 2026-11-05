def is_safe_sql(sql: str) -> bool:
    """Разрешаем только SELECT и WITH."""
    sql_upper = sql.strip().upper()
    return sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")
