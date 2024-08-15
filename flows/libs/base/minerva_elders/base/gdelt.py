# -*- coding: utf-8 -*-
import asyncio
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Tuple
from uuid import uuid4

import pandas as pd
from minerva_elders.base.db.utils import load_dataframes_to_bronze
from minerva_elders.base.io import clear_directory, download_file, unzip_file


class GDELTFileType(str, Enum):
    """
    Enum that represents the different types of GDELT files.
    """

    EVENTS = "events"
    GKG = "gkg"


GDELT_FILE_TYPE_COLUMNS = {
    GDELTFileType.EVENTS: {
        "GlobalEventID": "Int32",
        "Day": "Int32",
        "MonthYear": "Int32",
        "Year": "Int32",
        "FractionDate": "Float64",
        "Actor1Code": str,
        "Actor1Name": str,
        "Actor1CountryCode": str,
        "Actor1KnownGroupCode": str,
        "Actor1EthnicCode": str,
        "Actor1Religion1Code": str,
        "Actor1Religion2Code": str,
        "Actor1Type1Code": str,
        "Actor1Type2Code": str,
        "Actor1Type3Code": str,
        "Actor2Code": str,
        "Actor2Name": str,
        "Actor2CountryCode": str,
        "Actor2KnownGroupCode": str,
        "Actor2EthnicCode": str,
        "Actor2Religion1Code": str,
        "Actor2Religion2Code": str,
        "Actor2Type1Code": str,
        "Actor2Type2Code": str,
        "Actor2Type3Code": str,
        "IsRootEvent": bool,
        "EventCode": str,
        "EventBaseCode": str,
        "EventRootCode": str,
        "QuadClass": "Int32",
        "GoldsteinScale": "Float64",
        "NumMentions": "Int32",
        "NumSources": "Int32",
        "NumArticles": "Int32",
        "AvgTone": "Float64",
        "Actor1Geo_Type": "Int32",
        "Actor1Geo_FullName": str,
        "Actor1Geo_CountryCode": str,
        "Actor1Geo_ADM1Code": str,
        "Actor1Geo_Lat": "Float64",
        "Actor1Geo_Long": "Float64",
        "Actor1Geo_FeatureID": str,
        "Actor2Geo_Type": "Int32",
        "Actor2Geo_FullName": str,
        "Actor2Geo_CountryCode": str,
        "Actor2Geo_ADM1Code": str,
        "Actor2Geo_Lat": "Float64",
        "Actor2Geo_Long": "Float64",
        "Actor2Geo_FeatureID": str,
        "ActionGeo_Type": "Int32",
        "ActionGeo_FullName": str,
        "ActionGeo_CountryCode": str,
        "ActionGeo_ADM1Code": str,
        "ActionGeo_Lat": "Float64",
        "ActionGeo_Long": "Float64",
        "ActionGeo_FeatureID": str,
        "DATEADDED": "Int32",
        "SOURCEURL": str,
    },
    GDELTFileType.GKG: {
        "DATE": "Int32",
        "NUMARTS": "Int32",
        "COUNTS": str,
        "THEMES": str,
        "LOCATIONS": str,
        "PERSONS": str,
        "ORGANIZATIONS": str,
        "TONE": str,
        "CAMEOEVENTIDS": str,
        "SOURCES": str,
        "SOURCEURLS": str,
    },
}


def get_gdelt_file_url(date: datetime, type_: GDELTFileType) -> str:
    """
    Function that returns the URL of a GDELT file given a date and a type.

    Args:
        date (datetime): The date of the file.
        type (GDELTFileType): The type of the file.

    Returns:
        str: The URL of the file.
    """
    date_str = date.strftime("%Y%m%d")
    if type_ == GDELTFileType.EVENTS:
        return f"http://data.gdeltproject.org/events/{date_str}.export.CSV.zip"
    elif type_ == GDELTFileType.GKG:
        return f"http://data.gdeltproject.org/gkg/{date_str}.gkg.csv.zip"
    raise ValueError(f"Invalid GDELT file type: {type_}")


async def load_gdelt_file(date: datetime, type_: GDELTFileType, clear: bool = True) -> pd.DataFrame:
    """
    Function that loads a GDELT file into a DataFrame.

    Args:
        date (datetime): The date of the file.
        type (GDELTFileType): The type of the file.
        clear (bool): Whether to clear the temporary files after loading the data.

    Returns:
        pd.DataFrame: The DataFrame containing the file data.
    """
    # Create a temporary directory
    tmp_dir = Path("/tmp") / uuid4().hex
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # Download the file
    url = get_gdelt_file_url(date=date, type_=type_)
    zip_path = tmp_dir / f"{date.strftime('%Y%m%d')}-{type_}.zip"
    await download_file(url=url, path=zip_path)

    # Unzip it
    extract_to = tmp_dir / f"{date.strftime('%Y%m%d')}-{type_}"
    await unzip_file(zip_path=zip_path, extract_to=extract_to)

    # Get the CSV file
    csv_files = list(Path(extract_to).glob("*.csv")) + list(Path(extract_to).glob("*.CSV"))
    if len(csv_files) == 1:
        csv_path = csv_files[0]
    elif len(csv_files) > 1:
        raise ValueError("Multiple CSV files extracted in the directory.")
    else:
        raise FileNotFoundError("No CSV files extracted in the directory.")

    # Load the CSV file into a DataFrame
    # If it's GKG, the first row is a header
    if type_ == GDELTFileType.GKG:
        df = pd.read_csv(csv_path, sep="\t", header=0)
        df["UUID"] = [str(uuid4()) for _ in range(len(df))]
    # If it's Events, there are no column names. We need to specify them.
    elif type_ == GDELTFileType.EVENTS:
        df = pd.read_csv(csv_path, sep="\t", header=None)
        df.columns = list(GDELT_FILE_TYPE_COLUMNS[type_].keys())
    else:
        raise ValueError(f"Invalid GDELT file type: {type_}")

    # Fix column types
    df = df.astype(GDELT_FILE_TYPE_COLUMNS[type_])

    # Clear the temporary files if needed
    if clear:
        await clear_directory(tmp_dir)

    return df


async def load_gdelt_files(date: datetime, clear: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load GDELT files for a specific date.

    Args:
        date (datetime): The date to load the files for.
        clear (bool): Whether to clear the temporary files after loading the data.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the DataFrames for the Events and GKG
        files, respectively.
    """
    df_events_task = load_gdelt_file(date=date, type_=GDELTFileType.EVENTS)
    df_gkg_task = load_gdelt_file(date=date, type_=GDELTFileType.GKG)

    df_events, df_gkg = await asyncio.gather(df_events_task, df_gkg_task)

    return df_events, df_gkg


async def process_date(date: datetime):
    """
    Process and load GDELT data for a specific date.

    Args:
        date (datetime): The date to load and process the data for.
    """
    try:
        df_events, df_gkg = await load_gdelt_files(date)
        await load_dataframes_to_bronze(df_events, df_gkg)
        print(f"Successfully processed data for {date.strftime('%Y-%m-%d')}")
    except Exception as e:
        print(f"Failed to process data for {date.strftime('%Y-%m-%d')}: {e}")
