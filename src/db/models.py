from sqlalchemy import Column, Integer, Float, String, DateTime, Boolean, func
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Generation(Base):
    __tablename__ = "generation"

    _id = Column(Integer, primary_key=True)
    DATETIME = Column(DateTime, nullable=False)

    GAS = Column(Float)
    COAL = Column(Float)
    NUCLEAR = Column(Float)
    WIND = Column(Float)
    WIND_EMB = Column(Float)
    HYDRO = Column(Float)
    IMPORTS = Column(Float)
    BIOMASS = Column(Float)
    OTHER = Column(Float)
    SOLAR = Column(Float)
    STORAGE = Column(Float)
    GENERATION = Column(Float)
    CARBON_INTENSITY = Column(Float)
    LOW_CARBON = Column(Float)
    ZERO_CARBON = Column(Float)
    RENEWABLE = Column(Float)
    FOSSIL = Column(Float)

    GAS_perc = Column(Float)
    COAL_perc = Column(Float)
    NUCLEAR_perc = Column(Float)
    WIND_perc = Column(Float)
    WIND_EMB_perc = Column(Float)
    HYDRO_perc = Column(Float)
    IMPORTS_perc = Column(Float)
    BIOMASS_perc = Column(Float)
    OTHER_perc = Column(Float)
    SOLAR_perc = Column(Float)
    STORAGE_perc = Column(Float)
    GENERATION_perc = Column(Float)
    LOW_CARBON_perc = Column(Float)
    ZERO_CARBON_perc = Column(Float)
    RENEWABLE_perc = Column(Float)
    FOSSIL_perc = Column(Float)


class PipelineRunHistory(Base):
    __tablename__ = "pipeline_run_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_start = Column(DateTime(timezone=True), default=func.now(), nullable=False)
    run_stop = Column(DateTime(timezone=True), default=func.now())
    last_fetched_id = Column(Integer)
    total_fetched = Column(Integer, default=0)
    valid_records = Column(Integer, default=0)
    success = Column(Boolean, default=False)
    error_message = Column(String)