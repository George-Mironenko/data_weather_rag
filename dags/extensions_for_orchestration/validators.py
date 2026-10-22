import math
from decimal import Decimal
from typing import Any

import logging

logger = logging.getLogger(__name__)


def validate_cities(
    records: tuple[tuple[Any, ...], ...] | None,
) -> list[dict] | None:
    """
    Преобразует список кортежей (name, lat, lon) в список словарей.

    Проверки для каждой записи:
      1. Запись — кортеж из 3 элементов.
      2. name — непустая строка.
      3. lat/lon — числа (int, float, Decimal).
      4. lat/lon — не NaN и не inf.
      5. lat в диапазоне [-90, 90], lon — [-180, 180].

    :param records: Список кортежей (name, lat, lon) из БД
    :return: Список словарей [{"city": ..., "lat": ..., "lon": ...}]
             или None, если ни одна запись не прошла валидацию
    """
    if not records:
        logger.error("Ошибка: в функцию передан пустой список")
        return None

    def write_log(idx: int, warning: str):
        logger.warning(f"Запись #{idx} пропущена: {warning}")

    cities_list = []

    for idx, record in enumerate(records, 1):
        if not isinstance(record, tuple):
            write_log(idx, "ожидался tuple")
            continue

        if len(record) != 3:
            write_log(idx, f"ожидалось 3 элемента, получено {len(record)}")
            continue

        name, lat, lon = record

        if not isinstance(name, str):
            write_log(idx, "название города не строка")
            continue

        if not name.strip():
            write_log(idx, "пустая строка или пробелы в названии")
            continue

        # Postgres может вернуть Decimal, поэтому проверяем и его
        if not isinstance(lat, (int, float, Decimal)):
            write_log(idx, f"широта не число: {type(lat).__name__}")
            continue

        if not isinstance(lon, (int, float, Decimal)):
            write_log(idx, f"долгота не число: {type(lon).__name__}")
            continue

        lat_f = float(lat)
        lon_f = float(lon)

        if math.isnan(lat_f) or math.isinf(lat_f):
            write_log(idx, f"широта NaN или inf: {lat}")
            continue

        if math.isnan(lon_f) or math.isinf(lon_f):
            write_log(idx, f"долгота NaN или inf: {lon}")
            continue

        if not (-90.0 <= lat_f <= 90.0):
            write_log(idx, f"широта вне диапазона [-90, 90]: {lat_f}")
            continue

        if not (-180.0 <= lon_f <= 180.0):
            write_log(idx, f"долгота вне диапазона [-180, 180]: {lon_f}")
            continue

        cities_list.append({
            "city": name.strip(),
            "lat": lat_f,
            "lon": lon_f,
        })

    logger.debug("Успешно список преобразован")

    if not cities_list:
        logger.error("Ни одна запись не прошла валидацию.")
        return None

    logger.info(f"Прошли валидацию {len(cities_list)} из {len(records)} записей")
    return cities_list