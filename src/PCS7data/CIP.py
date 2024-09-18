import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from data_fetcher import DataFetcher
from data_processor import DataProcessor
from datetime import datetime

def process_chunks(files, processor, xpath, namespaces, chunk_size=10):
    time_data = []
    for i in range(0, len(files), chunk_size):
        chunk = files[i:i + chunk_size]
        result_time = list(map(lambda file: processor.process(file, xpath, namespaces, 'full'), chunk))
        time_data.extend(result_time)
    return time_data

def main():
    if os.name == 'nt':
        path = "C:/Users/se1irar/Downloads/Archive/"
        path = "Z:/Production/ALBFRII/BatchArchives_XML/"
    else:
        path = os.getenv("MY_PATH")
        path = "/mnt/c/Users/se1irar/Downloads/Archive/"


    processor = DataProcessor(path)
    fetcher = DataFetcher(path)
    filenames = os.listdir(path)

    start_number = 409
    end_number = 410
    includes = ["CIP", "516"]
    excludes = ["TEST", "EXTRA", "BUFF", "SAT", "SIP", "MIN", "654", "ALF", "ALT", "656",]

    filtered_files = fetcher.fetcher(filenames, start_number, end_number, includes, excludes)

    batch_numbers = list(map(processor.extract_batch_number, filtered_files))
    print(len(batch_numbers))

    namespaces = {'ns': 'SIMATIC_BATCH_V8_1_0'}
    xpath_batch = {
            "data": "/ns:Archivebatch/ns:Cr/ns:Rp/ns:Sfcdesccltn/ns:Sfcdescflat/ns:Sfcand/ns:Sfcseq/ns:Sfcstep",
    }

    chunk_size = 10
    result = process_chunks(filtered_files, processor, xpath_batch, namespaces, chunk_size)
    # /Archivebatch/Cr/Rp/Sfcdesccltn/Sfcdescflat
    paired = []
    

    for sfcstep in result:
        for element in sfcstep[0]:
            name = element.get('name')
            contid = element.get('contid')
            termid = element.get('termid')
            paired.append((name, contid, termid))

    for i, (name, contid, termid) in enumerate(paired):
        print(f"Index {i}: Name: {name}, ContID: {contid}, TermID: {termid}")
    
if __name__ == "__main__":
    main()
