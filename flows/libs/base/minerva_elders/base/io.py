# -*- coding: utf-8 -*-
import asyncio
import zipfile
from pathlib import Path

import aiofiles
import aiofiles.os
from aiohttp import ClientSession


async def clear_directory(directory: str | Path) -> None:
    """
    Asynchronously clears a directory by removing all files and subdirectories.

    Args:
        directory (str | Path): The path to the directory to clear.
    """
    directory = Path(directory)

    # Ensure the directory exists
    if not directory.exists() or not directory.is_dir():
        raise ValueError(f"{directory} is not a valid directory.")

    # Recursively remove files and directories
    for item in directory.iterdir():
        if item.is_dir():
            await clear_directory(item)  # Recursively clear subdirectory
            await aiofiles.os.rmdir(item)  # Remove the now-empty directory
        else:
            await aiofiles.os.remove(item)  # Remove the file


async def download_file(url: str, path: str | Path) -> None:
    """
    Function that downloads a file from a URL and saves it to a path.

    Args:
        url (str): The URL of the file.
        path (str | Path): The path where the file will be saved.
    """
    path = Path(path)
    # Ensure that the path is not a directory
    if path.exists() and path.is_dir():
        raise ValueError("The provided path is a directory, not a file path.")

    # Create the directory if it doesn't exist
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)

    async with ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            async with aiofiles.open(path, "wb") as file:
                await file.write(await response.read())


async def unzip_file(zip_path: str | Path, extract_to: str | Path) -> None:
    """
    Function that unzips a file.

    Args:
        zip_path (str | Path): The path to the zip file.
        extract_to (str | Path): The path where the file will be extracted.
    """
    zip_path = Path(zip_path)
    extract_to = Path(extract_to)

    # Ensure that the zip file exists
    if not zip_path.exists():
        raise FileNotFoundError(f"Zip file {zip_path} not found.")

    # Create the target directory if it doesn't exist
    if not extract_to.exists():
        extract_to.mkdir(parents=True, exist_ok=True)

    # Define a blocking function to unzip the file
    def _unzip():
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)

    # Run the blocking unzip operation in a separate thread
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _unzip)
