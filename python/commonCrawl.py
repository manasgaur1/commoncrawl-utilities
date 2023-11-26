import argparse
import logging
import os
import requests
import gzip
import json
import shutil
from typing import Generator, Dict, Any

# Configure the logger
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=os.environ.get("LOGLEVEL", "DEBUG").upper(),
)
logger = logging.getLogger(__name__)

class DownloadError(Exception):
    pass

def download_file(url: str, local_file_path: str) -> None:
    """
    Download a file from a URL and save it to a local path.

    Args:
        url (str): The URL of the file to download.
        local_file_path (str): The local file path to save the downloaded file.

    Raises:
        DownloadError: If the file download fails.
    """
    response = requests.get(url, stream=True)
    
    if response.status_code == 200:
        with open(local_file_path, "wb") as local_file:
            for chunk in response.iter_content(chunk_size=8192):
                local_file.write(chunk)
        logger.info(f"File downloaded successfully to {local_file_path}")
    else:
        logger.error(f"Failed to download file. Status code: {response.status_code}")
        raise DownloadError(f"Failed to download file from {url}")

def read_file(local_file_path: str) -> Generator[str, None, None]:
    """
    Read a gzipped file line by line.

    Args:
        local_file_path (str): The path to the local gzipped file.

    Yields:
        str: Each line of the file as a string.
    """
    with gzip.open(local_file_path, 'rt', encoding='utf-8') as file:
        for line in file:
            yield line.strip()

record = {'data': ""}

def write_record(record: Dict[str, Any], base_output_folder: str, output_file: str) -> None:
        """
        Write a record to an output JSONL file.

        Args:
            record (Dict[str, Any]): The record to be written.
        """
        language = record.get('WARC-Identified-Content-Language', "eng").replace(',', '_')
        output_folder = os.path.join(base_output_folder, language)
        output_path = os.path.join(output_folder, language + "." + output_file)
        os.makedirs(output_folder, exist_ok=True)
        with open(output_path, 'a', encoding='utf-8') as jsonl_file:
            json.dump(record, jsonl_file, ensure_ascii=False)
            jsonl_file.write('\n')

def process_records(lines_generator: Generator[str, None, None], config: Dict[str, Any], 
                    base_output_folder: str, output_file: str) -> None:
    """
    Process and write records to output JSONL files.

    Args:
        lines_generator (Generator[str, None, None]): Generator yielding lines from the file.
        config (Dict[str, Any]): Configuration settings as a dictionary.
        base_output_folder (str): The base output folder path.
        output_file (str): The name of the output JSONL file.
    """
    global record
    reserved_words = set(config['reserved_words'])

    for line in lines_generator:
        if line != config['record_start']:
            parts = line.split(":")
            if set([parts[0]]) & reserved_words:
                record[parts[0]] = ":".join(parts[1:]).strip()
            else:
                record['data'] += line + '\n'
        else:
            write_record(record, base_output_folder, output_file)
            record.clear()
            record = {'data': ""}

    logger.info(f"record={record}")
    write_record(record, base_output_folder, output_file)

def main() -> None:
    """
    Main function to download and process a file from a URL.
    """
    parser = argparse.ArgumentParser(description="URL for downloading the file")
    parser.add_argument("url", help="URL of the file to download and process")

    args = parser.parse_args()
    data_folder = "data"
    os.makedirs(data_folder, exist_ok=True)

    local_file_path = os.path.join(data_folder, args.url.split("/")[-1])
    download_file(args.url, local_file_path)


    lines_generator = read_file(local_file_path)
    next(lines_generator) # skip first line in the file


    config_file_path = "./config/config.json"
    with open(config_file_path, "r") as json_file:
        config = json.load(json_file)

    base_output_folder = os.path.join(data_folder, "output")
    if os.path.exists(base_output_folder):
        shutil.rmtree(base_output_folder)
    os.makedirs(base_output_folder, exist_ok=True)

    output_file = args.url.split("/")[-1].replace('.wet.gz', '.jsonl')

    process_records(lines_generator, config, base_output_folder, output_file)


if __name__ == "__main__":
    main()
