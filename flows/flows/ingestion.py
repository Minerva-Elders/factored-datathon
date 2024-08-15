# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from os import getenv
from pathlib import Path
from typing import List, Tuple

import pandas as pd
from minerva_elders.base.db import bronze
from minerva_elders.base.db.utils import (
    create_schema_if_not_exists,
    create_tables_if_not_exist,
    load_dataframes_to_bronze,
)
from minerva_elders.base.gdelt import load_gdelt_files
from prefect import flow, task


@task(retries=3, retry_delay_seconds=10)
async def setup_bronze_schema(database_url: str) -> None:
    """
    Task that sets up the bronze schema in the PostgreSQL database.

    Args:
        database_url (str): The URL of the PostgreSQL database.
    """
    print("Setting up bronze schema in the database")
    await create_schema_if_not_exists(database_url=database_url, schema_name="bronze")
    await create_tables_if_not_exist(
        database_url=database_url, declarative_base=bronze.Base
    )
    print("Bronze schema set up in the database")


@task
def generate_date_list(start_date: datetime, end_date: datetime) -> List[datetime]:
    """
    Task that generates a list of dates between the start and end dates.

    Args:
        start_date (datetime): The start date.
        end_date (datetime): The end date (inclusive).
    """
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date)
        current_date += timedelta(days=1)
    return date_list


@task(
    retries=3,
    retry_delay_seconds=10,
    tags=["data-fetching"],
    cache_result_in_memory=False,
)
async def get_raw_dataframes(date: datetime) -> Tuple[str, str]:
    """
    Task that loads GDELT files for a single date and returns the DataFrames.

    Args:
        date (datetime): The date to process.

    Returns:
        Tuple[str, str]: Paths to the DataFrames containing the GDELT data.
    """
    print(f"Loading GDELT files for date: {date}")
    df_events, df_gkg = await load_gdelt_files(date=date)
    print(f"Loaded GDELT files for date: {date}")
    output_dir = Path(f"/tmp/gdelt/{date.strftime('%Y%m%d')}")
    output_dir.mkdir(parents=True, exist_ok=True)
    df_events.to_csv(output_dir / "events.csv", index=False)
    df_gkg.to_csv(output_dir / "gkg.csv", index=False)
    del df_events, df_gkg
    return str(output_dir / "events.csv"), str(output_dir / "gkg.csv")


@task(
    retries=3,
    retry_delay_seconds=10,
    tags=["database-operations"],
    cache_result_in_memory=False,
)
async def upload_to_bronze(
    dataframes: Tuple[str, str], database_url: str, chunksize: int = 100
) -> None:
    """
    Task that uploads the GDELT DataFrames to the PostgreSQL database.

    Args:
        dataframes (Tuple[str]): Paths for the DataFrames to upload.
        database_url (str): The URL of the PostgreSQL database.
    """
    path_events, path_gkg = dataframes
    df_events_reader = pd.read_csv(
        path_events,
        chunksize=chunksize,
    )
    df_gkg_reader = pd.read_csv(
        path_gkg,
        chunksize=chunksize,
    )
    print("Uploading DataFrames to the database")
    await load_dataframes_to_bronze(
        df_events_reader=df_events_reader,
        df_gkg_reader=df_gkg_reader,
        database_url=database_url,
    )
    print("DataFrames uploaded to the database")


@flow
def gdelt_ingestion_flow(
    database_url: str,
    start_date: datetime = None,
    end_date: datetime = None,
    upload_chunk_size: int = 100,
) -> None:
    """
    Flow that processes GDELT files for a range of dates and stores them in a PostgreSQL database.

    Args:
        database_url (str): The URL of the PostgreSQL database.
        start_date (datetime): The start date. If not provided, defaults to yesterday.
        end_date (datetime): The end date (inclusive). If not provided, defaults to yesterday.
    """
    # Generate the list of dates to process
    start_date = start_date or datetime.now() - timedelta(days=1)
    end_date = end_date or start_date
    date_list = generate_date_list(start_date=start_date, end_date=end_date)

    # Set up the bronze schema
    setup_bronze_schema(database_url=database_url)

    # Load data for each date
    raw_dataframes = get_raw_dataframes.map(date=date_list)

    # Upload the data to the database
    upload_to_bronze.map(
        dataframes=raw_dataframes,
        database_url=database_url,
        chunksize=upload_chunk_size,
    )


if __name__ == "__main__":
    # This is just for local execution
    # Get input values
    required_envs = ["START_DATE", "END_DATE", "DATABASE_URL"]
    missing_envs = [env for env in required_envs if not getenv(env)]
    if missing_envs:
        raise ValueError(f"Missing required environment variables: {missing_envs}")
    env_values = {env: getenv(env) for env in required_envs}

    # Parse the dates
    try:
        start_date = datetime.strptime(env_values["START_DATE"], "%Y-%m-%d")
        end_date = datetime.strptime(env_values["END_DATE"], "%Y-%m-%d")
    except ValueError:
        raise ValueError("Invalid date format. Use YYYY-MM-DD.")

    # Run the flow
    database_url = env_values["DATABASE_URL"]
    gdelt_ingestion_flow(
        database_url=database_url, start_date=start_date, end_date=end_date
    )
