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

    start_number = 000
    end_number = 999 
    includes = ["654"]
    excludes = ["TEST", "EXTRA", "BUFF", "SAT", "SIP", "516", "MIN", "CIP", "656", "GRF", "GRT", "ALF", "ALT",]

    filtered_files_654 = fetcher.fetcher(filenames, start_number, end_number, includes, excludes)
    filtered_files_new = fetcher.fetcher(filenames, 150, 500, includes, excludes)

    with ThreadPoolExecutor() as executor:
        batch_numbers_654 = list(executor.map(processor.extract_batch_number, filtered_files_654))
    print(len(batch_numbers_654))

    namespaces = {'ns': 'SIMATIC_BATCH_V8_1_0'}
    xpaths_654 = {
            "ing_batch": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='0' and @termid='48']/ns:Parvalcltn/ns:Parvalstring[@id='30']/@actval",
    }
    
    xpath_654_old = {
            "ing_batch": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='0' and @termid='3']/ns:Parvalcltn/ns:Parvalstring[@id='30']/@actval",
    }

    with ThreadPoolExecutor() as executor:
        ingaende_batch_old = list(executor.map(lambda file: processor.process(file, xpath_654_old, namespaces, 'last'), filtered_files_654))
    with ThreadPoolExecutor() as executor:
        ingaende_batch = list(executor.map(lambda file: processor.process(file, xpaths_654, namespaces, 'last'), filtered_files_654))

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

    ing_df = pd.DataFrame(ingaende_batch, columns=['batch_number'])
    ing_df['batch_654'] = batch_numbers_654
    ing_df['filenames_654'] = filtered_files_654

    ing_df_old = pd.DataFrame(ingaende_batch_old, columns=['batch_number'])
    ing_df_old['batch_654'] = batch_numbers_654
    ing_df_old['filenames_654'] = filtered_files_654

    combined_ing_df = pd.concat([ing_df, ing_df_old]).drop_duplicates(subset=['batch_number', 'batch_654'])

    d = {
            'filenames': filtered_files_656,
            'batch_number': batch_numbers_656,
    }
    df_temp = pd.DataFrame(d)

    merged_df = pd.merge(combined_ing_df, df_temp, on='batch_number', how='inner')
    print(merged_df)

    files_merge_654 = merged_df['filenames_654'].tolist()
    files_ing_batches_656 = merged_df['filenames'].tolist()
    batch_number_ing = merged_df['batch_number'].tolist()
    batch_number = merged_df['batch_654'].tolist()
    files_merge_654_new = [i for i in filtered_files_new if i in files_merge_654]

    xpath_656ing = {
            "For_Konc_Innan_WT1": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='6' and @termid='4']/ns:Parvalcltn/ns:Parvalfloat[@id='32' and @actval > '2']/@actval",
            "For_Konc_Innan_TT1": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='6' and @termid='4']/ns:Parvalcltn/ns:Parvalfloat[@id='34' and @actval > '2']/@actval",
            "For_Konc_Efter_WT1": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='6' and @termid='5']/ns:Parvalcltn/ns:Parvalfloat[@id='32' and @actval > '2']/@actval",
            "For_Konc_Efter_TT1": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='6' and @termid='5']/ns:Parvalcltn/ns:Parvalfloat[@id='34' and @actval > '2']/@actval",
            "For_Konc_Abs": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='6' and @termid='1']/ns:Parvalcltn/ns:Parvalfloat[@id='44' and not (number(@actval) = 0)]/@actval",
            "Dia_NaCl_Innan_WT1": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='13' and @termid='9']/ns:Parvalcltn/ns:Parvalfloat[@id='32' and @actval > '2']/@actval",
            "Dia_NaCl_Innan_TT1": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='13' and @termid='9']/ns:Parvalcltn/ns:Parvalfloat[@id='34' and @actval > '2']/@actval",
            "Dia_NaCl_Efter_WT1": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='13' and @termid='15']/ns:Parvalcltn/ns:Parvalfloat[@id='32' and @actval > '2']/@actval",
            "Dia_NaCl_Efter_TT1": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='13' and @termid='15']/ns:Parvalcltn/ns:Parvalfloat[@id='34' and @actval > '2']/@actval",
            "Dia_NaCl_Sats_Vol": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='13' and @termid='4']/ns:Parvalcltn/ns:Parvalfloat[@id='34' and @actval > '2']/@actval",
            "Dia_NaCl_Abs": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='13' and @termid='8']/ns:Parvalcltn/ns:Parvalfloat[@id='7' and not (number(@actval) = 0)]/@actval",
            "Dia_WFI_Innan_WT1": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='12' and @termid='14']/ns:Parvalcltn/ns:Parvalfloat[@id='32' and @actval > '2']/@actval",
            "Dia_WFI_Innan_TT1": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='12' and @termid='14']/ns:Parvalcltn/ns:Parvalfloat[@id='34' and @actval > '2']/@actval",
            "Dia_WFI_Efter_WT1": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='12' and @termid='16']/ns:Parvalcltn/ns:Parvalfloat[@id='32' and @actval > '2']/@actval",
            "Dia_WFI_Efter_TT1": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='12' and @termid='16']/ns:Parvalcltn/ns:Parvalfloat[@id='34' and @actval > '2']/@actval",
            "Dia_WFI_Sats_Vol": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='12' and @termid='4']/ns:Parvalcltn/ns:Parvalfloat[@id='34' and @actval > '2']/@actval",
            "Dia_WFI_Abs": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='12' and @termid='8']/ns:Parvalcltn/ns:Parvalfloat[@id='7' and not (number(@actval) = 0)]/@actval",
            "Konc_Innan_WT1": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='7' and @termid='1']/ns:Parvalcltn/ns:Parvalfloat[@id='32' and @actval > '2']/@actval",
            "Konc_Innan_TT1": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='7' and @termid='1']/ns:Parvalcltn/ns:Parvalfloat[@id='34' and @actval > '2']/@actval",
            "Konc_Efter_WT1": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='7' and @termid='9']/ns:Parvalcltn/ns:Parvalfloat[@id='32' and @actval > '2']/@actval",
            "Konc_Efter_TT1": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='7' and @termid='9']/ns:Parvalcltn/ns:Parvalfloat[@id='34' and @actval > '2']/@actval",
            "Konc_Abs": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='7' and @termid='31']/ns:Parvalcltn/ns:Parvalfloat[@id='44' and not (number(@actval) = 0)]/@actval",
            "Konc_Abs_Eluering": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='7' and @termid='3']/ns:Parvalcltn/ns:Parvalfloat[@id='44' and not (number(@actval) = 0)]/@actval",
            "Just_Slutvikt_Innan_WT1": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='9' and @termid='43']/ns:Parvalcltn/ns:Parvalfloat[@id='32' and @actval > '2']/@actval",
            "Just_Slutvikt_Innan_TT1": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='9' and @termid='43']/ns:Parvalcltn/ns:Parvalfloat[@id='34' and @actval > '2']/@actval",
            "Just_Slutvikt_Efter_WT1": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='9' and @termid='25']/ns:Parvalcltn/ns:Parvalfloat[@id='32' and @actval > '2']/@actval",
            "Just_Slutvikt_Efter_TT1": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='9' and @termid='25']/ns:Parvalcltn/ns:Parvalfloat[@id='34' and @actval > '2']/@actval",
    }

    xpath_654_times = {
            "Omrörning": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='0' and @termid='3']/@timestamp",
            "WFI_Tillsats": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='0' and @termid='14']/@timestamp",
            "NaCl_Tillsats": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='3' and @termid='1']/@timestamp",
            "WFI_till_WDS2": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='0' and @termid='8']/@timestamp",
            "Filtr_NaclBufftill5%": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='0' and @termid='63']/@timestamp",

    }

    xpath_654_filtr_times = {
            "Filtr_10%_Alb_start": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='0' and @termid='58']/@timestamp",
            "Filtr_10%_Alb_end": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='0' and @termid='74']/@timestamp",

    }

    with ThreadPoolExecutor(max_workers=4) as executor:
        results_654_time = list(executor.map(lambda file: processor.process(file, xpath_654_times, namespaces, 'full'), files_merge_654_new))
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        results_656 = list(executor.map(lambda file: processor.process(file, xpath_656ing, namespaces, 'last'), files_ing_batches_656))

    results_654_time_filtr = list(map(lambda file: processor.process(file, xpath_654_filtr_times, namespaces, 'full'), files_merge_654_new))

    difference = [[processor.time_difference(timestamps) if timestamps is not None else None for timestamps in row] for row in results_654_time]
    start_filtr = [timestamps[0] for timestamps in results_654_time_filtr] 
    end_filtr = [timestamps[1] for timestamps in results_654_time_filtr] 
    comb_filtr_times = [
            [
                min([datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ") for ts in start_times]) if start_times else None, 
                max([datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ") for ts in end_times]) if end_times else None
            ]
            for start_times, end_times in zip(start_filtr, end_filtr)
    ]
    comb_filtr_times_str = [
    [
        start.strftime("%Y-%m-%dT%H:%M:%S.%fZ") if start is not None else None,
        end.strftime("%Y-%m-%dT%H:%M:%S.%fZ") if end is not None else None
    ]
    for start, end in comb_filtr_times
    ]
    
    difference_filtr = [processor.time_difference(times) if times is not None else None for times in comb_filtr_times_str]
    column_names = list(xpath_654_times.keys())
    df_time_diffs = pd.DataFrame(difference, columns=column_names)
    df_time_diffs['Filename_654'] = files_merge_654_new
    df_time_diffs['Filtr_10%_Alb'] = difference_filtr
    df_time_diffs.dropna()
    print(df_time_diffs)

    column_names = list(xpath_656ing.keys())
    
    df = pd.DataFrame(results_656, columns=column_names)
    df['batch_number'] = batch_number
    df ['batch_number_ingaende'] = batch_number_ing
    df['Filename_654'] = files_merge_654
    df['Filename_ingaende'] = files_ing_batches_656
    df.dropna(subset=['For_Konc_Innan_WT1', 'Dia_NaCl_Innan_TT1'])

    final_df = pd.merge(df, df_time_diffs, on='Filename_654', how='left')
    print(final_df)

    final_df.to_excel('~/Downloads/Anna_Data_fake.xlsx', index=False)

if __name__ == "__main__":
    main()
