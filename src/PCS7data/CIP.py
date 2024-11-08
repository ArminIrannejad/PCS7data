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

def process_chunks_time(files, processor, xpath_VBH, namespaces, chunk_size=10):
    time_data = []
    for i in range(0, len(files), chunk_size):
        chunk = files[i:i + chunk_size]
        with ThreadPoolExecutor() as executor:
            result_time = list(executor.map(lambda file: processor.process(file, xpath_VBH, namespaces, 'full'), chunk))
        chunk_time_data = [[processor.time_difference(timestamps) if timestamps is not None else None for timestamps in row] for row in result_time]
        time_data.extend(chunk_time_data)
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

    start_number = 400
    end_number = 402
    includes = ["CIP", "516",]
    excludes = ["TEST", "EXTRA", "BUFF", "SAT", "SIP", "MIN", "654", "ALF", "ALT", "656"]

    filtered_files = fetcher.fetcher(filenames, start_number, end_number, includes, excludes)

    batch_numbers = list(map(processor.extract_batch_number, filtered_files))
    print(len(batch_numbers))

    namespaces = {'ns': 'SIMATIC_BATCH_V8_1_0'}
    xpath_batch = {
        "data": "/ns:Archivebatch/ns:Cr/ns:Rp/ns:Sfcdesccltn/ns:Sfcdescflat/ns:Sfcand",
    }

    chunk_size = 15
    result = process_chunks(filtered_files, processor, xpath_batch, namespaces, chunk_size)
    columns_grfp = ['Filename', 'CIPSats_Kallvatten', 'CIP_Dranering', 'CIPSats_NaOH', 'CIP_Dranering2', 'CIPSats_Kallvatten2', 'CIP_Dranering3', 'CIPSats_HWFI', 'CIP_Blasning']
    columns = ['Filename', 'CIPSats_Kallvatten', 'CIP_Dranering', 'CIPSats_NaOH', 'CIP_Dranering2', 'CIPSats_HWFI', 'CIP_Dranering3', 'CIP_Blasning', 'HygSta']
    
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    df = pd.DataFrame(result, columns=columns) # pyright: ignore should be fixed in 2.3.* pandas
    df_grt11 = df[df['Filename'].str.contains('GRT11', case=False) & ~df['Filename'].str.contains('GRFP', case=False)]
    df_grt12 = df[df['Filename'].str.contains('GRT12', case=False) & ~df['Filename'].str.contains('GRFP', case=False)]
    df_grt13 = df[df['Filename'].str.contains('GRT13', case=False) & ~df['Filename'].str.contains('GRFP', case=False)]
    df_grt14 = df[df['Filename'].str.contains('GRT14', case=False) & ~df['Filename'].str.contains('GRFP', case=False)]
    df_grt15 = df[df['Filename'].str.contains('GRT15', case=False) & ~df['Filename'].str.contains('GRFP', case=False)]
    df_grt16 = df[df['Filename'].str.contains('GRT15', case=False) & ~df['Filename'].str.contains('GRFP', case=False)]
    df_grfp1_grt11 = df[df['Filename'].str.contains('GRFP1', case=False) & df['Filename'].str.contains('GRT11', case=False)]
    df_grfp1_grt13 = df[df['Filename'].str.contains('GRFP1', case=False) & df['Filename'].str.contains('GRT13', case=False)]
    df_grfp1_grt15 = df[df['Filename'].str.contains('GRFP1', case=False) & df['Filename'].str.contains('GRT15', case=False)]
    df_grfp2_grt12 = df[df['Filename'].str.contains('GRFP2', case=False) & df['Filename'].str.contains('GRT12', case=False)]
    df_grfp2_grt14 = df[df['Filename'].str.contains('GRFP2', case=False) & df['Filename'].str.contains('GRT14', case=False)]
    df_grfp2_grt16 = df[df['Filename'].str.contains('GRFP2', case=False) & df['Filename'].str.contains('GRT16', case=False)]
    df_grfp3_grt12 = df[df['Filename'].str.contains('GRFP3', case=False) & df['Filename'].str.contains('GRT12', case=False)]
    df_grfp3_grt14 = df[df['Filename'].str.contains('GRFP3', case=False) & df['Filename'].str.contains('GRT14', case=False)]
    df_grfp3_grt16 = df[df['Filename'].str.contains('GRFP3', case=False) & df['Filename'].str.contains('GRT16', case=False)]
    dfs_filter = [df_grfp1_grt11, df_grfp1_grt13, df_grfp1_grt15, df_grfp2_grt12, df_grfp2_grt14, df_grfp2_grt16, df_grfp3_grt12, df_grfp3_grt14, df_grfp3_grt16]
    for df in dfs_filter:
        df.columns = columns_grfp

    aoneuth = df_grt16, df_grt12, df_grt13, df_grt11, df_grt15, df_grt14

    tuples_list = []
    for _, row in df_grt14.iterrows():
        tuples_list.append(row['CIPSats_Kallvatten'])

    result_dict_grt14_sfc1 = {t[0]: f"/ns:Archivebatch/ns:Cr/ns:Eventcltn/ns:Eventrph[@contid={t[1]} and @termid={t[2]}]/@timestamp" for t in tuples_list[0]}

    time_data = process_chunks_time(df_grt14['Filename'].tolist(), processor, result_dict_grt14_sfc1, namespaces, chunk_size)
    difference = [timestamps[0] if timestamps is not None else None for row in time_data for timestamps in row]
    start_times = [timestamps[1].strftime("%Y-%m-%dT%H:%M:%S.%fZ") if timestamps is not None else None for row in time_data for timestamps in row]
    end_times = [timestamps[2].strftime("%Y-%m-%dT%H:%M:%S.%fZ") if timestamps is not None else None for row in time_data for timestamps in row]
    
    df_grt14_final = pd.DataFrame({
        'Start_time': start_times,
        'End_time': end_times,
        'Difference': difference,
    })

    print(len(df_grt14))
    print(df_grt14_final)

if __name__ == "__main__":
    main()
