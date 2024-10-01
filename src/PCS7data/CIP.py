import os
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from data_fetcher import DataFetcher
from data_processor import DataProcessor

def process_chunks(files, processor, xpaths, namespaces, chunk_size=10):
    all_sfcands_data = []
    for i in range(0, len(files), chunk_size):
        chunk = files[i:i + chunk_size]
        results = list(map(lambda file: processor.recipe_block_id(file, xpaths, namespaces), chunk))
        all_sfcands_data.extend(results)
    return all_sfcands_data

def main():
    if os.name == 'nt':
        path = "C:/Users/se1irar/Downloads/Archive/"
        path = "Z:/Production/ALBFRII/BatchArchives_XML/"
    else:
        path = "/mnt/c/Users/se1irar/Downloads/Archive/"
        path = os.getenv("MY_PATH")

    processor = DataProcessor(path)
    fetcher = DataFetcher(path)
    filenames = os.listdir(path)

    start_number = 400
    end_number = 405
    includes = ["CIP", "516",]
    excludes = ["TEST", "EXTRA", "BUFF", "SAT", "SIP", "MIN", "654", "ALF", "ALT", "656"]

    filtered_files = fetcher.fetcher(filenames, start_number, end_number, includes, excludes)

    batch_numbers = list(map(processor.extract_batch_number, filtered_files))
    print(len(batch_numbers))

    namespaces = {'ns': 'SIMATIC_BATCH_V8_1_0'}
    xpath_batch = {
        "data": "/ns:Archivebatch/ns:Cr/ns:Rp/ns:Sfcdesccltn/ns:Sfcdescflat/ns:Sfcand",
    }

    chunk_size = 10
    result = process_chunks(filtered_files, processor, xpath_batch, namespaces, chunk_size)
    columns_grfp = ['Filename', 'CIPSats_Kallvatten', 'CIP_Dranering', 'CIPSats_NaOH', 'CIP_Dranering2', 'CIPSats_Kallvatten2', 'CIP_Dranering3', 'CIPSats_HWFI', 'CIP_Blasning']
    columns = ['Filename', 'CIPSats_Kallvatten', 'CIP_Dranering', 'CIPSats_NaOH', 'CIP_Dranering2', 'CIPSats_HWFI', 'CIP_Dranering3', 'CIP_Blasning', 'HygSta']
    
    df = pd.DataFrame(result, columns=columns)
    df_grfp = df[df['Filename'].str.contains('GRFP', case=False)]
    df_grfp.columns = columns_grfp 
    print(df)
    print(df_grfp)

if __name__ == "__main__":
    main()

