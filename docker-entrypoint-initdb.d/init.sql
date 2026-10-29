-- 1. Справочник городов: DAG читает отсюда name, lat, lon
CREATE TABLE IF NOT EXISTS cities (
    id      SERIAL PRIMARY KEY,
    name    TEXT NOT NULL UNIQUE,
    lat     DOUBLE PRECISION NOT NULL,
    lon     DOUBLE PRECISION NOT NULL
);


CREATE INDEX IF NOT EXISTS idx_cities_weather_daily_date
    ON cities (date);


CREATE TABLE IF NOT EXISTS daily_variables (
    variables TEXT NOT NULL UNIQUE
);

INSERT INTO daily_variables (variables) VALUES
    ('temperature_2m_max'),
    ('temperature_2m_min'),
    ('precipitation_sum'),
    ('wind_speed_10m_max')
ON CONFLICT (variables) DO NOTHING;

-- 3. Наполнение справочника (те же 11 городов, что в CITIES)
insert into cities (name, lat, lon) values
    ('Moscow',55.7558,37.6173),
    ('Saint Petersburg',59.9343,30.3351),
    ('Yekaterinburg',56.8389,60.6057),
    ('Vladivostok',43.1332,131.9113),
    ('Novosibirsk',55.0084,82.9357),
    ('Krasnodar',45.0355,38.9753),
    ('Chelyabinsk',55.1644,61.4368),
    ('Kaliningrad',54.7104,20.4522),
    ('Murmansk',68.9585,33.0827),
    ('Magadan',59.5638,150.8035),
    ('Omsk',54.9885,73.3242),
    ('Sochi',43.6028,39.7342),
    ('Yakutsk',62.0355,129.6755),
    ('Astrakhan',46.3497,48.0408)
on conflict (name) do nothing;

