import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from data_fetcher import DataFetcher
from data_processor import DataProcessor
from datetime import datetime

def main():
    path = os.getenv("MY_PATH")
    path = '/mnt/c/Users/se1irar/Downloads/Archive/'
    process = DataProcessor(path)
    fetcher = DataFetcher(path)
    filenames = os.listdir(path)

    start_number = 320
    end_number = 400
    includes = ["656"]
    includes2 = ["657"]
    excludes = ["TEST", "EXTRA", "BUFF", "SAT", "SIP", "516", "MIN", "CIP", "654", "GRF", "GRT", "ALF", "ALT", ]

    filtered_files_656 = fetcher.fetcher(filenames, start_number, end_number, includes, excludes)
    filtered_files_657 = fetcher.fetcher(filenames, start_number, end_number, includes2, excludes)

    with ThreadPoolExecutor() as executor:
        batch_numbers_656 = list(executor.map(process.extract_batch_number, filtered_files_656))

    with ThreadPoolExecutor() as executor:
        batch_numbers_657 = list(executor.map(process.extract_batch_number, filtered_files_657))

    filtered_files_656 += filtered_files_657
    batch_numbers_656 += batch_numbers_657
    print(len(batch_numbers_656))

    namespaces = {'ns': 'SIMATIC_BATCH_V8_1_0'}
    xpath_VBH = {
            "VBH": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='31' and @termid='4']/@timestamp",
    }

    result_time = list(map(lambda file: process.process(file, xpath_VBH, namespaces, 'full'), filtered_files_656))
    print(result_time)

if __name__ == "__main__":
    main()
