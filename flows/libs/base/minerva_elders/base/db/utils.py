# -*- coding: utf-8 -*-
import asyncio

from pandas.io.parsers.readers import TextFileReader
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from .bronze import EVENTS_TABLE_NAME, GKG_TABLE_NAME


async def create_schema_if_not_exists(database_url: str, schema_name: str):
    """
    Asynchronously creates a schema in a PostgreSQL database if it does not already exist.

    Args:
        database_url (str): The URL of the PostgreSQL database.
        schema_name (str): The name of the schema to create.
    """
    # Create the SQLAlchemy engine
    engine = create_async_engine(database_url, echo=False)

    # Create a session for async operations
    async with engine.begin() as conn:
        # Create the schema if it does not exist
        await conn.run_sync(
            lambda sync_conn: sync_conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        )

    # Close the engine
    await engine.dispose()


async def create_tables_if_not_exist(database_url: str, declarative_base: object):
    """
    Asynchronously creates tables in a PostgreSQL database if they do not already exist.

    Args:
        database_url (str): The URL of the PostgreSQL database.
        declarative_base (object): The declarative base object containing the table definitions.
    """
    # Create the SQLAlchemy engine
    engine = create_async_engine(database_url, echo=False)

    # Create a session for async operations
    async with engine.begin() as conn:
        # Create the tables if they do not exist
        await conn.run_sync(declarative_base.metadata.create_all)

    # Close the engine
    await engine.dispose()


async def df_to_postgres(
    df_reader: TextFileReader,
    table_name: str,
    database_url: str,
    schema_name: str = "public",
    if_exists: str = "append",
):
    """
    Asynchronously uploads a DataFrame to a PostgreSQL table.

    Args:
        df_reader (TextFileReader): The reader for the DataFrame to upload.
        table_name (str): The name of the table to upload the data to.
        database_url (str): The URL of the PostgreSQL database.
        schema_name (str): The name of the schema containing the table.
        if_exists (str): Behavior when the table already exists: 'replace', 'append', 'fail'.
    """
    # Create the SQLAlchemy engine
    engine = create_async_engine(database_url, echo=False)

    # Create a session for async operations
    async with engine.begin() as conn:
        for chunk_df in df_reader:
            # Append the data to the existing table
            await conn.run_sync(
                lambda sync_conn: chunk_df.to_sql(
                    name=table_name,
                    con=sync_conn,
                    if_exists=if_exists,
                    index=False,
                    schema=schema_name,
                )
            )

    # Close the engine
    await engine.dispose()


async def load_dataframes_to_bronze(
    df_events_reader: TextFileReader, df_gkg_reader: TextFileReader, database_url: str
):
    """
    Asynchronously loads the DataFrames into bronze tables in the PostgreSQL database.

    Args:
        df_events_reader (TextFileReader): The reader for the events DataFrame.
        df_gkg_reader (TextFileReader): The reader for the GKG DataFrame.
        database_url (str): The URL of the PostgreSQL database.
    """
    df_events_task = df_to_postgres(
        df_reader=df_events_reader,
        table_name=EVENTS_TABLE_NAME,
        database_url=database_url,
        schema_name="bronze",
    )
    df_gkg_task = df_to_postgres(
        df_reader=df_gkg_reader,
        table_name=GKG_TABLE_NAME,
        database_url=database_url,
        schema_name="bronze",
    )

    await asyncio.gather(df_events_task, df_gkg_task)
