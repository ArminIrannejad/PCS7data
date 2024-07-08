import os
import re
import pandas as pd
from lxml import etree
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

class DataFetcher:
    def __init__(self, path):
        self.path = path
    def file_filter(self, file, start_number, end_number, includes, excludes):
        m_pattern = re.compile(r'M(\d{3})')
        match = m_pattern.search(file)
        if match:
            number = int(match.group(1))
            if start_number <= number <= end_number:
                if all(include.lower() in file.lower() for include in includes) and not any(exclude.lower() in file.lower() for exclude in excludes):
                    return file 
        return None 

    def fetcher(self, filenames, start_number, end_number, includes, excludes):
        with ThreadPoolExecutor() as executor:
            filtered_files = list(executor.map(lambda file: self.file_filter(file, start_number, end_number, includes, excludes), filenames))
        return list(filter(None, filtered_files))

class DataProcessor:
    def __init__(self, path):
        self.path = path

    def extract_batch_number(self, filename):
        ignore = 'SB8_34633-5543-83_'
        if filename.startswith(ignore):
            filename = filename[len(ignore):]
        start_index = filename.find('M')
        if start_index != -1:
            result = filename[start_index: start_index + 10]
            return result
        return None

    def process(self, file, xpaths, namespaces):
        results = [file]
        file_path = fr'{self.path}/{file}'
        tree = etree.parse(file_path)
        root = tree.getroot()
        for xpath in xpaths.values():
            parvalfloats = root.xpath(xpath, namespaces=namespaces)
            if parvalfloats:
                lst = [parvalfloat for parvalfloat in parvalfloats]
                results.append(str(list(set(lst))[-1] if len(set(lst)) < 3 else None))
            else:
                results.append(None)
        return results

    def get_time_(self, file):
        namespaces = {'ns': 'SIMATIC_BATCH_V8_1_0'}
        xpath = "//ns:Archivebatch/ns:Cr"
        file_path = fr'{self.path}/{file}'
        tree = etree.parse(file_path)
        root = tree.getroot()
        parvalfloats = root.xpath(xpath, namespaces=namespaces)
        if not parvalfloats:
            return None
        lst = [
        f"{parvalfloat.get('actstart')} {parvalfloat.get('actend')}"
        for parvalfloat in parvalfloats
        ]
        return str(list(set(lst))[-1]).split() if len(set(lst)) == 1 else None    

    def time_diff_(self, row):
        if pd.isnull(row['Start_time']) or pd.isnull(row['End_time']):
            return None
        else:
            start_dt = datetime.strptime(row['Start_time'], "%Y-%m-%dT%H:%M:%S.%fZ")
            end_dt = datetime.strptime(row['End_time'], "%Y-%m-%dT%H:%M:%S.%fZ")
            difference = end_dt - start_dt
            total_seconds = int(difference.total_seconds())
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            diff_str = f"{hours} h {minutes} min {seconds} s"
            return pd.Series([diff_str, total_seconds])

    def get_time(self, batch_numbers, files):
        with ThreadPoolExecutor() as executor:
            timestamps = list(executor.map(self.get_time_, files))
        start_times = [timestamp[0] if timestamp is not None else None for timestamp in timestamps]
        end_times = [timestamp[1] if timestamp is not None else None for timestamp in timestamps]
        dict_time = {
                'Batch_number': batch_numbers,
                'Start_time': start_times,
                'End_time': end_times, 
                'File': files,
        }
        df = pd.DataFrame(dict_time)
        df[['Difference', 'Total_Seconds']] = df.apply(self.time_diff_, axis=1)
        df = df.dropna(subset=['Start_time', 'End_time', 'Batch_number'])
        df = df[['Batch_number', 'Difference', 'Start_time', 'End_time', 'File', 'Total_Seconds']]  
        return df

def main():
    path = os.getenv("MY_PATH")
    path = '/mnt/c/Users/se1irar/Downloads/Archive/'
    processor = DataProcessor(path)
    fetcher = DataFetcher(path)
    filenames = os.listdir(path)

    start_number = 400
    end_number = 500 
    includes = ["516"]
    excludes = ["TEST", "EXTRA", "BUFF", "SAT", "SIP", "654", "MIN", "CIP", "656", "GRF"]

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
    print(df)

if __name__ == "__main__":
    main()
