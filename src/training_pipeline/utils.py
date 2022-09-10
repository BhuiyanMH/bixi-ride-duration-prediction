import requests
import shutil
import os
import pathlib


DATASET_URLS ={
    "2022-04-01": "https://sitewebbixi.s3.amazonaws.com/uploads/docs/20220104-stations-f82036.zip",
    "2022-05-01": "https://sitewebbixi.s3.amazonaws.com/uploads/docs/20220105-donnees-ouvertes-0d544b.zip",
    "2022-06-01":"https://sitewebbixi.s3.amazonaws.com/uploads/docs/20220106-donnees-ouvertes-f45195.zip",
    "2022-07-01":"https://sitewebbixi.s3.amazonaws.com/uploads/docs/20220107-donnees-ouvertes-8aa623.zip"
}

def download_data(url:str, dest_path:str):
    # download the compressed data file
    with open(dest_path, 'wb') as data_file:
        content = requests.get(url, stream=True).content
        data_file.write(content)

    # unzip it into a data folder
    data_folder = dest_path.split(".")[0]
    shutil.unpack_archive(dest_path, data_folder)

    # remove the compressed data file
    compressed_file = pathlib.Path(dest_path)
    compressed_file.unlink(missing_ok=True)

def download_all_data():
    for key, value in DATASET_URLS.items():
        dest_path = f"data/{key}.zip"
        download_data(value, dest_path)

    