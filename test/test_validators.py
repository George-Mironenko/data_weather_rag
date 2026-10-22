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


def test_single_valid_city():
    result = validate_cities((("Moscow", 55.7558, 37.6173),))
    assert result == [{"city": "Moscow", "lat": 55.7558, "lon": 37.6173}]

def test_multiple_valid_cities_preserve_order():
    data = (
        ("Moscow", 55.7558, 37.6173),
        ("London", 51.5074, -0.1278),
        ("Sydney", -33.8688, 151.2093),
    )
    result = validate_cities(data)
    assert [c["city"] for c in result] == ["Moscow", "London", "Sydney"]
    assert len(result) == 3

def test_empty_input_returns_empty_list():
    assert validate_cities(()) == []
    assert validate_cities([]) == []

def test_boundary_coordinates_are_valid():
    data = (
        ("NorthPole", 90.0, 0.0),
        ("SouthPole", -90.0, 0.0),
        ("DateLineEast", 0.0, 180.0),
        ("DateLineWest", 0.0, -180.0),
        ("Zero", 0.0, 0.0),
    )
    result = validate_cities(data)
    assert len(result) == 5

def test_latitude_out_of_range_raises():
    with pytest.raises((ValueError, AssertionError)):
        validate_cities((("Bad", 91.0, 0.0),))

def test_longitude_out_of_range_raises():
    with pytest.raises((ValueError, AssertionError)):
        validate_cities((("Bad", 0.0, 181.0),))

def test_empty_city_name_raises():
    with pytest.raises((ValueError, AssertionError)):
        validate_cities((("", 55.0, 37.0),))

def test_non_numeric_coordinates_raise():
    with pytest.raises((TypeError, ValueError, AssertionError)):
        validate_cities((("Bad", "55.0", 37.0),))

def test_wrong_tuple_length_raises():
    with pytest.raises((ValueError, TypeError, AssertionError)):
        validate_cities((("Moscow", 55.7),))          # не хватает lon
    with pytest.raises((ValueError, TypeError, AssertionError)):
        validate_cities((("Moscow", 55.7, 37.6, 1),)) # лишний элемент

def test_coordinates_as_int_accepted():
    result = validate_cities((("Zero", 0, 0),))
    assert result == [{"city": "Zero", "lat": 0, "lon": 0}]

def test_one_invalid_among_valid_raises_whole_call():
    data = (
        ("Moscow", 55.7558, 37.6173),
        ("Broken", 200.0, 37.0),   # невалидная широта
    )
    with pytest.raises((ValueError, AssertionError)):
        validate_cities(data)

def test_duplicate_cities_all_returned():
    data = (("Moscow", 55.7558, 37.6173), ("Moscow", 55.7558, 37.6173))
    result = validate_cities(data)
    assert len(result) == 2

def test_input_not_mutated():
    data = [["Moscow", 55.7558, 37.6173]]
    snapshot = [list(x) for x in data]
    validate_cities(data)
    assert data == snapshot

def test_result_item_has_exact_keys():
    result = validate_cities((("Moscow", 55.7558, 37.6173),))
    assert set(result[0].keys()) == {"city", "lat", "lon"}

def test_result_is_list():
    assert isinstance(validate_cities((("Moscow", 55.7, 37.6),)), list)