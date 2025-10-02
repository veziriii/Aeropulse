from sqlalchemy.orm import Session
from aeropulse.models.weather_res6 import WeatherRes6


def insert_weather_cells(session: Session, h3_cells):
    objs = [WeatherRes6(h3_res6=cell) for cell in h3_cells]
    for obj in objs:
        session.merge(obj)  # upsert
    session.commit()
