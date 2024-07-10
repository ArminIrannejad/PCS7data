import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from data_fetcher import DataFetcher
from data_processor import DataProcessor


def main():
    path = os.getenv("MY_PATH")
    #path = '/mnt/c/Users/se1irar/Downloads/Archive/'
    processor = DataProcessor(path)
    fetcher = DataFetcher(path)
    filenames = os.listdir(path)

    start_number = 000
    end_number = 500 
    includes = ["654"]
    excludes = ["TEST", "EXTRA", "BUFF", "SAT", "SIP", "516", "MIN", "CIP", "656", "GRF", "GRT", "ALF", "ALT",]

    filtered_files_654 = fetcher.fetcher(filenames, start_number, end_number, includes, excludes)

    with ThreadPoolExecutor() as executor:
        batch_numbers_654 = list(executor.map(processor.extract_batch_number, filtered_files_654))
    print(len(batch_numbers_654))

    namespaces = {'ns': 'SIMATIC_BATCH_V8_1_0'}
    xpaths_654 = {
            "ing_batch": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='0' and @termid='48']/ns:Parvalcltn/ns:Parvalstring[@id='30']/@actval",
    }

    with ThreadPoolExecutor() as executor:
        results_654 = list(executor.map(lambda file: processor.process(file, xpaths_654, namespaces), filtered_files_654))

    ingaende_batch = [result[1] for result in results_654]

    includes = ["656"]
    excludes = ["TEST", "EXTRA", "BUFF", "SAT", "SIP", "516", "MIN", "CIP", "654", "GRF", "GRT", "ALF", "ALT", ]

    filtered_files_656 = fetcher.fetcher(filenames, start_number, end_number, includes, excludes)

    with ThreadPoolExecutor() as executor:
        batch_numbers_656 = list(executor.map(processor.extract_batch_number, filtered_files_656))

    ing_df = pd.DataFrame(ingaende_batch, columns=['batch_number'])
    ing_df['batch_654'] = batch_numbers_654
    d = {
            'filenames': filtered_files_656,
            'batch_number': batch_numbers_656,
    }
    df = pd.DataFrame(d)

    merged_df = pd.merge(ing_df, df, on='batch_number', how='inner')
    print(merged_df)

    files_654_656 = merged_df['filenames'].tolist()
    batch_number_ing = merged_df['batch_number'].tolist()
    batch_number = merged_df['batch_654'].tolist()

if __name__ == "__main__":
    main()
