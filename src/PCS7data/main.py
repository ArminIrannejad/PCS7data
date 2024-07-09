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

    start_number = 400
    end_number = 500 
    includes = ["516"]
    excludes = ["TEST", "EXTRA", "BUFF", "SAT", "SIP", "654", "MIN", "CIP", "656", "GRF", "GRT", ]

    filtered_files = fetcher.fetcher(filenames, start_number, end_number, includes, excludes)
    print(len(filtered_files))

    with ThreadPoolExecutor() as executor:
        batch_numbers = list(executor.map(processor.extract_batch_number, filtered_files))
    print(len(batch_numbers))

    times = processor.get_time(batch_numbers, filtered_files)
    print(times)

    namespaces = {'ns': 'SIMATIC_BATCH_V8_1_0'}
    xpaths = {
    "A_vikt": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='61' and @termid='5']/ns:Parvalcltn/ns:Parvalmaterial[@id='10' and @actval > '2']/@actval",
    "IIISusp_V_Innan_pHjust": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='5' and @termid='48']/ns:Parvalcltn/ns:Parvalfloat[@id='22' and @actval > '2']/@actval",
    "IIISusp_V_Efter_pHjust": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='5' and @termid='31']/ns:Parvalcltn/ns:Parvalfloat[@id='22' and @actval > '2']/@actval",
    "VIExtr_V_add_IIISusp_Efter": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='8' and @termid='1']/ns:Parvalcltn/ns:Parvalfloat[@id='2' and @actval > '2']/@actval",
    "VIExtr_V_efter_pHjust": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='0' and @termid='1']/ns:Parvalcltn/ns:Parvalfloat[@id='22' and @actval > '2']/@actval",
    "VIIIFallning_V_Innan_pHjust": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='1' and @termid='43']/ns:Parvalcltn/ns:Parvalfloat[@id='22' and @actval > '2']/@actval",
    "VIIIFallning_V_Efter_pHjust": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='1' and @termid='45']/ns:Parvalcltn/ns:Parvalfloat[@id='22' and @actval > '2']/@actval",
    "XIVFallning_V_Innan_pHjust": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='30' and @termid='13']/ns:Parvalcltn/ns:Parvalfloat[@id='22' and @actval > '2']/@actval",
    "XIVFallning_V_Innan_pHjust": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='30' and @termid='18']/ns:Parvalcltn/ns:Parvalfloat[@id='22' and @actval > '2']/@actval",
    "IIISusp_pH_Innan_pHjust": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='5' and @termid='9']/ns:Parvalcltn/ns:Parvalfloat[@id='14' and @actval > '2']/@actval",
    "IIISusp_pH_Efter_pHjust": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='5' and @termid='29']/ns:Parvalcltn/ns:Parvalfloat[@id='14' and @actval > '2']/@actval",
    "VIExtr_pH_Innan_pHjust": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='0' and @termid='153']/ns:Parvalcltn/ns:Parvalfloat[@id='14' and @actval > '2']/@actval",
    "VIExtr_pH_Efter_pHjust": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='0' and @termid='79']/ns:Parvalcltn/ns:Parvalfloat[@id='14' and @actval > '2']/@actval",
    "VIIIFallning_pH_Innan_pHjust": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='1' and @termid='39']/ns:Parvalcltn/ns:Parvalfloat[@id='14' and @actval > '2']/@actval",
    "VIIIFallning_pH_Efter_pHjust": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='1' and @termid='6']/ns:Parvalcltn/ns:Parvalfloat[@id='14' and @actval > '2' and @actval < '7']/@actval",
    "XIVFallning_pH_Innan_pHjust": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='30' and @termid='95']/ns:Parvalcltn/ns:Parvalfloat[@id='14' and @actval > '2']/@actval",
    "XIVFallning_pH_Efter_pHjust": "/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid='30' and @termid='25']/ns:Parvalcltn/ns:Parvalfloat[@id='14' and @actval > '2']/@actval"
    }
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(lambda file: processor.process(file, xpaths, namespaces), filtered_files))
    
    first_col = 'File'
    xpath_key = list(xpaths.keys())
    column_names = [first_col] + xpath_key

    df = pd.DataFrame(results, columns=column_names)
    df['Batch_Number'] = batch_numbers
    df = df.dropna(subset=['Batch_Number', 'A_vikt', 'File', ])
    df = df.to_excel('~/Downloads/testanna.xlsx')

if __name__ == "__main__":
    main()
