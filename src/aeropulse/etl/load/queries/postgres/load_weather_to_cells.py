from sqlalchemy.orm import Session
from aeropulse.models.weather_res6 import WeatherRes6


def seed_weather_cells(session: Session, h3_cells: list[str]) -> int:
    count = 0
    for cell in h3_cells:
        session.merge(WeatherRes6(h3_res6=cell))
        count += 1
    session.commit()
    return count
