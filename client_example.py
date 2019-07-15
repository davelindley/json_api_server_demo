import requests
import json
import shutil
import logging
import csv

logging.basicConfig(filename="chunk_example.log", level=logging.INFO)


def add_to_failed_files(filename):
    """
    Helper function for structured logging of filenames in csv.
    For if you want to retry any failed files.
    :param filename:
    :return:
    """
    with open("file_failures.csv", "a+") as output_file:
        writer = csv.writer(output_file)
        writer.writerow([filename])


for file in json.loads(requests.get("http://127.0.0.1:5000/files").text)["filenames"]:
    with requests.get(
        "http://127.0.0.1:5000/get_data", params={"file": file}, stream=True
    ) as request_stream:
        with open(file, "wb+") as file_to_write:
            # copyfileobj is iterative and chunked by default to avoid memory over-consumption
            try:
                shutil.copyfileobj(request_stream.raw, file_to_write)
                logging.info(f"{file} was written")
            except Exception as e:
                logging.critical(f"{file} NOT ABLE TO BE RETRIEVED with exception {e}")
                add_to_failed_files(file)
