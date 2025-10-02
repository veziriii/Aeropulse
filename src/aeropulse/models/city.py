from sqlalchemy import Column, Integer, String, Float, BigInteger
from aeropulse.models.base import (
    Base,
)  # we’ll define a shared Base if you don’t have one yet


class City(Base):
    __tablename__ = "cities_us"

    city_id = Column(BigInteger, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    state = Column(String)
    country = Column(String, nullable=False)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    h3_res6 = Column(String(16), index=True)
