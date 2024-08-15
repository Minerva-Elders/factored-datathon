# -*- coding: utf-8 -*-
from sqlalchemy import Boolean, Column, Float, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()
EVENTS_TABLE_NAME = "events"
GKG_TABLE_NAME = "gkg"


class Events(Base):
    __tablename__ = EVENTS_TABLE_NAME
    __table_args__ = {"schema": "bronze"}

    GlobalEventID = Column(Integer, primary_key=True)
    Day = Column(Integer)
    MonthYear = Column(Integer)
    Year = Column(Integer)
    FractionDate = Column(Float)
    Actor1Code = Column(String)
    Actor1Name = Column(String)
    Actor1CountryCode = Column(String)
    Actor1KnownGroupCode = Column(String)
    Actor1EthnicCode = Column(String)
    Actor1Religion1Code = Column(String)
    Actor1Religion2Code = Column(String)
    Actor1Type1Code = Column(String)
    Actor1Type2Code = Column(String)
    Actor1Type3Code = Column(String)
    Actor2Code = Column(String)
    Actor2Name = Column(String)
    Actor2CountryCode = Column(String)
    Actor2KnownGroupCode = Column(String)
    Actor2EthnicCode = Column(String)
    Actor2Religion1Code = Column(String)
    Actor2Religion2Code = Column(String)
    Actor2Type1Code = Column(String)
    Actor2Type2Code = Column(String)
    Actor2Type3Code = Column(String)
    IsRootEvent = Column(Boolean)
    EventCode = Column(String)
    EventBaseCode = Column(String)
    EventRootCode = Column(String)
    QuadClass = Column(Integer)
    GoldsteinScale = Column(Float)
    NumMentions = Column(Integer)
    NumSources = Column(Integer)
    NumArticles = Column(Integer)
    AvgTone = Column(Float)
    Actor1Geo_Type = Column(Integer)
    Actor1Geo_FullName = Column(String)
    Actor1Geo_CountryCode = Column(String)
    Actor1Geo_ADM1Code = Column(String)
    Actor1Geo_Lat = Column(Float)
    Actor1Geo_Long = Column(Float)
    Actor1Geo_FeatureID = Column(String)
    Actor2Geo_Type = Column(Integer)
    Actor2Geo_FullName = Column(String)
    Actor2Geo_CountryCode = Column(String)
    Actor2Geo_ADM1Code = Column(String)
    Actor2Geo_Lat = Column(Float)
    Actor2Geo_Long = Column(Float)
    Actor2Geo_FeatureID = Column(String)
    ActionGeo_Type = Column(Integer)
    ActionGeo_FullName = Column(String)
    ActionGeo_CountryCode = Column(String)
    ActionGeo_ADM1Code = Column(String)
    ActionGeo_Lat = Column(Float)
    ActionGeo_Long = Column(Float)
    ActionGeo_FeatureID = Column(String)
    DATEADDED = Column(Integer)
    SOURCEURL = Column(String)


class GKG(Base):
    __tablename__ = GKG_TABLE_NAME
    __table_args__ = {"schema": "bronze"}

    UUID = Column(String, primary_key=True)
    DATE = Column(Integer)
    NUMARTS = Column(Integer)
    COUNTS = Column(String)
    THEMES = Column(String)
    LOCATIONS = Column(String)
    PERSONS = Column(String)
    ORGANIZATIONS = Column(String)
    TONE = Column(String)
    CAMEOEVENTIDS = Column(String)
    SOURCES = Column(String)
    SOURCEURLS = Column(String)
