import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from data_fetcher import DataFetcher
from data_processor import DataProcessor
from datetime import datetime

def process_time_data_in_chunks(files, processor, xpath_VBH, namespaces, chunk_size=10):
    time_data = []
    
    for i in range(0, len(files), chunk_size):
        chunk = files[i:i + chunk_size]
        
        with ThreadPoolExecutor() as executor:
            result_time = list(executor.map(lambda file: processor.process(file, xpath_VBH, namespaces, 'full'), chunk))
        
        chunk_time_data = [[processor.time_difference(timestamps) if timestamps is not None else None for timestamps in row] for row in result_time]
        time_data.extend(chunk_time_data)
    
    return time_data

def main():
    path = "C:/Users/se1irar/Downloads/Archive/"
    path = "Z:/Production/ALBFRII/BatchArchives_XML/"
    output_path = "C:/Users/se1irar/Downloads/Data/Albumin/VBH_Tider.csv"
    output_path1 = "Z:/Public/Temp/se1irar/Data/Albumin/VBH_Tider.csv"

    processor = DataProcessor(path)
    fetcher = DataFetcher(path)
    filenames = os.listdir(path)

    start_number = 149
    end_number = 450
    includes = ["656"]
    includes2 = ["657"]
    excludes = ["TEST", "EXTRA", "BUFF", "SAT", "SIP", "516", "MIN", "CIP", "654", "GRF", "GRT", "ALF", "ALT"]

    filtered_files_656 = fetcher.fetcher(filenames, start_number, end_number, includes, excludes)
    filtered_files_657 = fetcher.fetcher(filenames, start_number, end_number, includes2, excludes)

    filtered_files_656 += filtered_files_657

    if os.path.exists(output_path):
        existing_df = pd.read_csv(output_path)
        existing_filenames = set(existing_df['Filename'].tolist())
    else:
        existing_filenames = set()

    new_files = [file for file in filtered_files_656 if file not in existing_filenames]

    batch_numbers_656 = list(map(processor.extract_batch_number, new_files))

    namespaces = {'ns': 'SIMATIC_BATCH_V8_1_0'}
    xpath_VBH = {
            "VBH": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='31' and @termid='4']/@timestamp",
    }

    chunk_size = 20  
    time_data = process_time_data_in_chunks(new_files, processor, xpath_VBH, namespaces, chunk_size)

    difference = [timestamps[0] if timestamps is not None else None for row in time_data for timestamps in row]
    start_times = [timestamps[1].strftime("%Y-%m-%dT%H:%M:%S.%fZ") if timestamps is not None else None for row in time_data for timestamps in row]
    end_times = [timestamps[2].strftime("%Y-%m-%dT%H:%M:%S.%fZ") if timestamps is not None else None for row in time_data for timestamps in row]

    df = pd.DataFrame({
        'Batch_Number': batch_numbers_656,
        'Start_time': start_times,
        'End_time': end_times,
        'Difference': difference,
        'Filename': new_files,
    })

    df = df.dropna()
    print(df)

    if os.path.exists(output_path):
        df.to_csv(output_path, mode='a', header=False, index=False)
        df.to_csv(output_path1, mode='a', header=False, index=False)
    else:
        df.to_csv(output_path, index=False)
        df.to_csv(output_path1, index=False)

    df_sort = pd.read_csv(output_path1)

    df_sort['Start_time'] = pd.to_datetime(df_sort['Start_time'])
    df_sort = df_sort.sort_values(by='Start_time', ascending=False)

    df_sort.to_csv(output_path1, index=False)

if __name__ == "__main__":
    main()
