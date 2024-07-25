import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from data_fetcher import DataFetcher
from data_processor import DataProcessor


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
        results_654_old = list(executor.map(lambda file: processor.process(file, xpath_654_old, namespaces, 'last'), filtered_files_654))
    with ThreadPoolExecutor() as executor:
        results_654 = list(executor.map(lambda file: processor.process(file, xpaths_654, namespaces, 'last'), filtered_files_654))

    ingaende_batch_old = [result_old[1] for result_old in results_654_old]
    ingaende_batch = [result[1] for result in results_654]

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
    df = pd.DataFrame(d)

    merged_df = pd.merge(combined_ing_df, df, on='batch_number', how='inner')
    print(merged_df)

    files_merge_654 = merged_df['filenames_654'].tolist()
    files_ing_batches_656 = merged_df['filenames'].tolist()
    batch_number_ing = merged_df['batch_number'].tolist()
    batch_number = merged_df['batch_654'].tolist()

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

    with ThreadPoolExecutor(max_workers=4) as executor:
        results_656 = list(executor.map(lambda file: processor.process(file, xpath_656ing, namespaces, 'last'), files_ing_batches_656))


    first_col = 'Filename_656'
    xpath_key = list(xpath_656ing.keys())
    column_names = [first_col] + xpath_key
    
    df = pd.DataFrame(results_656, columns=column_names)
    df['batch_number'] = batch_number
    df ['batch_number_ingaende'] = batch_number_ing
    df['Filename_654'] = files_merge_654
    df.dropna(subset=['For_Konc_Innan_WT1'])

    df.to_excel('~/Downloads/Anna_Data_fake.xlsx', index=False)

if __name__ == "__main__":
    main()
