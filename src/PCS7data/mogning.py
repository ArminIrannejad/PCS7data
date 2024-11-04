import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from data_fetcher import DataFetcher
from data_processor import DataProcessor
from datetime import datetime

def process_chunks(files, processor, xpath_mog, namespaces, chunk_size=10):
    time_data = []
    
    for i in range(0, len(files), chunk_size):
        chunk = files[i:i + chunk_size]
        
        with ThreadPoolExecutor() as executor:
            result_time = list(executor.map(lambda file: processor.process(file, xpath_mog, namespaces, 'full'), chunk))

        time_data.extend(result_time)
    return time_data

def main():
    if os.name == 'nt':
        path = "C:/Users/se1irar/Downloads/Archive/"
        path = "Z:/Production/ALBFRII/BatchArchives_XML/"
        output_path = "C:/Users/se1irar/Downloads/Data/FrII/mogning.csv"
        output_path1 = "Z:/Public/Temp/se1irar/Data/FrII/mogning.csv"
    else:
        path = os.getenv("MY_PATH")
        path = "/mnt/c/Users/se1irar/Downloads/Archive/"
        output_path = "/mnt/c/Users/se1irar/Downloads/Data/FrII/mogning.csv"
        output_path1 = "/mnt/z/Public/Temp/se1irar/Data/FrII/mogning.csv"


    processor = DataProcessor(path)
    fetcher = DataFetcher(path)
    filenames = os.listdir(path)

    start_number = 400
    end_number = 404
    includes = ["516"]
    excludes = ["TEST", "EXTRA", "BUFF", "SAT", "SIP", "65", "MIN", "CIP", "654", "ALF", "ALT"]

    filtered_files = fetcher.fetcher(filenames, start_number, end_number, includes, excludes)


    if os.path.exists(output_path):
        existing_df = pd.read_csv(output_path)
        existing_filenames = set(existing_df['Filename'].tolist())
    else:
        existing_filenames = set()

    new_files = [file for file in filtered_files if file not in existing_filenames]

    batch_numbers = list(map(processor.extract_batch_number, new_files))

    namespaces = {'ns': 'SIMATIC_BATCH_V8_1_0'}
    xpath_mog = {
            "start_mog": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='1' and @termid='12']/@timestamp",
            "end_mog": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventsub[@contid='25' and @termid='0']/@timestamp"
    }

    chunk_size = 10  
    time_data = process_chunks(new_files, processor, xpath_mog, namespaces, chunk_size)

    for i in time_data:
        print(i)

#   difference = [processor.time_difference(times) if times[0] and times[1] else None for times in comb_times_str]

#    df = pd.DataFrame({
#        'Batch_Number': batch_numbers,
#        'Start_time': start_times,
#        'End_time': end_times,
#        'Duration': difference,
#        'Filename': new_files,
#    })
#
#    df = df.dropna()
#
#    if os.path.exists(output_path):
#        df.to_csv(output_path, mode='a', header=False, index=False)
#        df.to_csv(output_path1, mode='a', header=False, index=False)
#    else:
#        df.to_csv(output_path, index=False)
#        df.to_csv(output_path1, index=False)
#
#    df_sort = pd.read_csv(output_path1)
#
#    df_sort['Start_time'] = pd.to_datetime(df_sort['Start_time'], format='mixed')
#    df_sort['End_time'] = pd.to_datetime(df_sort['End_time'], format='mixed')
#    df_sort = df_sort.sort_values(by='Start_time', ascending=False)
#
#    df_sort['Start_time'] = df_sort['Start_time'].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
#    df_sort['End_time'] = df_sort['End_time'].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
#
#    df_sort.to_csv(output_path1, index=False)

if __name__ == "__main__":
    main()
