import math
from decimal import Decimal

import pytest

from dags.extensions_for_orchestration.validators import validate_cities


def test_empty_input_returns_none():
    assert validate_cities(None) is None
    assert validate_cities(()) is None


def test_valid_record():
    result = validate_cities((("Moscow", 55.7558, 37.6173),))
    assert result == [{"city": "Moscow", "lat": 55.7558, "lon": 37.6173}]


def test_decimal_coordinates_are_converted():
    result = validate_cities(
        (("Moscow", Decimal("55.7558"), Decimal("37.6173")),)
    )
    assert result is not None
    assert isinstance(result[0]["lat"], float)


def test_invalid_name_skipped():
    result = validate_cities((
        ("", 55.75, 37.61),          # пустое имя
        ("   ", 55.75, 37.61),       # пробелы
        (123, 55.75, 37.61),         # не строка
        ("Spb", 59.93, 30.31),       # валидный
    ))
    assert result == [{"city": "Spb", "lat": 59.93, "lon": 30.31}]


def test_out_of_range_lat():
    result = validate_cities((
        ("Bad", 91.0, 37.61),
        ("Bad2", -91.0, 37.61),
    ))
    assert result is None


def test_out_of_range_lon():
    result = validate_cities((
        ("Bad", 55.75, 181.0),
        ("Bad2", 55.75, -181.0),
    ))
    assert result is None


def test_nan_and_inf_skipped():
    result = validate_cities((
        ("NaN", float("nan"), 37.61),
        ("Inf", 55.75, float("inf")),
        ("Ok", 55.75, 37.61),
    ))
    assert result == [{"city": "Ok", "lat": 55.75, "lon": 37.61}]


def test_wrong_tuple_length():
    result = validate_cities((
        ("TooShort", 55.75),
        ("TooLong", 55.75, 37.61, "extra"),
        ("Ok", 55.75, 37.61),
    ))
    assert result == [{"city": "Ok", "lat": 55.75, "lon": 37.61}]


def test_wrong_type_not_tuple():
    result = validate_cities((
        ["Moscow", 55.75, 37.61],   # list, а не tuple
        ("Ok", 55.75, 37.61),
    ))
    assert result == [{"city": "Ok", "lat": 55.75, "lon": 37.61}]

