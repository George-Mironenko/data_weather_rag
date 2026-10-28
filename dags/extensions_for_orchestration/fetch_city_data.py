import pandas as pd
import requests


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
