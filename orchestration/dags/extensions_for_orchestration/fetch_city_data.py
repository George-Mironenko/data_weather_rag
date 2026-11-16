import requests
import pandas as pd


def fetch_city_data(
        city: str,
        lat: float,
        lon: float,
        start_date: str,
        end_date: str,
        daily_variables: tuple[str]
) -> pd.DataFrame:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": daily_variables,
        "timezone": "auto",
    }
    resp = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params=params,
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()

    df = pd.DataFrame(data["daily"])
    df["city"] = city
    return df

def fetch_only_check_city_data(
        start_date: str,
        end_date: str,
) -> bool:
    """
    Эта фукнция только проверяет, что данные в api есть.
    :param start_date:
    :param end_date:
    :return:
    """
    params = {
        "latitude": 55.755855,
        "longitude": 37.6173,
        "start_date": start_date,
        "end_date": end_date,
        "daily": 'temperature_2m_max',
        "timezone": "auto",
    }

    try:
        resp = requests.get(
            "https://archive-api.open-meteo.com/v1/archive",
            params=params,
            timeout=60,
        )
        resp.raise_for_status()
    except requests.RequestException:
        return False

    daily = resp.json().get("daily", {})
    values = daily.get("temperature_2m_max") or []
    return any(v is not None for v in values)
