from sqlalchemy import Column, String, DateTime, JSON
from aeropulse.models.base import Base


class WeatherRes6(Base):
    __tablename__ = "weather_res6"
    h3_res6 = Column(String(16), primary_key=True)
    last_updated = Column(DateTime)
    weather = Column(JSON)
