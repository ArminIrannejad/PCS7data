import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from data_fetcher import DataFetcher
from data_processor import DataProcessor
from datetime import datetime

def main():
    path = os.getenv("MY_PATH")
    path = '/mnt/c/Users/se1irar/Downloads/Archive/'
    processor = DataProcessor(path)
    fetcher = DataFetcher(path)
    filenames = os.listdir(path)

    start_number = 351
    end_number = 351
    includes = ["656"]
    includes2 = ["657"]
    excludes = ["TEST", "EXTRA", "BUFF", "SAT", "SIP", "516", "MIN", "CIP", "654", "GRF", "GRT", "ALF", "ALT", ]

    filtered_files_656 = fetcher.fetcher(filenames, start_number, end_number, includes, excludes)
    filtered_files_657 = fetcher.fetcher(filenames, start_number, end_number, includes2, excludes)

    with ThreadPoolExecutor() as executor:
        batch_numbers_656 = list(executor.map(processor.extract_batch_number, filtered_files_656))

    with ThreadPoolExecutor() as executor:
        batch_numbers_657 = list(executor.map(processor.extract_batch_number, filtered_files_657))

    filtered_files_656 += filtered_files_657
    batch_numbers_656 += batch_numbers_657
    print(len(batch_numbers_656))

    namespaces = {'ns': 'SIMATIC_BATCH_V8_1_0'}
    xpath_VBH = {
            "VBH": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='31' and @termid='4']/@timestamp",
    }

    with ThreadPoolExecutor() as executor:
        result_time = list(executor.map(lambda file: processor.process(file, xpath_VBH, namespaces, 'full'), filtered_files_656))
    
    time_data = [[processor.time_difference(timestamps) if timestamps is not None else None for timestamps in row] for row in result_time]
    difference = [timestamps[0] if timestamps is not None else None for row in time_data for timestamps in row]
    start_times = [timestamps[1].strftime("%Y-%m-%dT%H:%M:%S.%fZ") if timestamps is not None else None for row in time_data for timestamps in row]
    end_times = [timestamps[2].strftime("%Y-%m-%dT%H:%M:%S.%fZ") if timestamps is not None else None for row in time_data for timestamps in row]

    df = pd.DataFrame({
        'Batch_Number': batch_numbers_656,
        'Start_time': start_times,
        'End_time': end_times,
        'Difference': difference,
        'Filename': filtered_files_656,
    })

    df = df.dropna()
    print(df)

if __name__ == "__main__":
    main()
