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
        file_path = f'{self.path}/{file}'
        tree = etree.parse(file_path)
        root = tree.getroot()
        for xpath in xpaths.values():
            parvalfloats = root.xpath(xpath, namespaces=namespaces)
            if parvalfloats:
                lst = [parvalfloat for parvalfloat in parvalfloats]
                results.append(str(list(set(lst))[-1] if len(set(lst)) < 2 else None))
            else:
                results.append(None)
        return results

    def get_time_(self, file):
        namespaces = {'ns': 'SIMATIC_BATCH_V8_1_0'}
        xpath = "//ns:Archivebatch/ns:Cr"
        if '"' in file:
            return None
        file_path = f'{self.path}/{file}'
        tree = etree.parse(file_path)
        root = tree.getroot()
        parvalfloats = root.xpath(xpath, namespaces=namespaces)
        if not parvalfloats:
            return None
        lst = [
        f"{parvalfloat.get('actstart')} {parvalfloat.get('actend')}"
        for parvalfloat in parvalfloats
        ]
        return str(list(set(lst))[-1]).split() if len(set(lst)) < 2 else None    

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
                'Batch_Numbers': batch_numbers,
                'Start_time': start_times,
                'End_time': end_times, 
                'File': files,
        }
        df = pd.DataFrame(dict_time)
        df[['Difference', 'Total_Seconds']] = df.apply(self.time_diff_, axis=1)
        df = df.dropna(subset=['Start_time', 'End_time', 'Batch_Numbers'])
        df = df[['Batch_Numbers', 'Difference', 'Start_time', 'End_time', 'File', 'Total_Seconds']]  
        return df

def main():
    path = os.getenv("MY_PATH")
    processor = DataProcessor(path)
    fetcher = DataFetcher(path)
    filenames = os.listdir(path)

    start_number = 102
    end_number = 500 
    includes = ["654"]
    excludes = ["TEST", "EXTRA", "BUFF", "SAT", "SIP", "516", "MIN", "CIP", "656",]

    filtered_files = fetcher.fetcher(filenames, start_number, end_number, includes, excludes)
    print(len(filtered_files))

    with ThreadPoolExecutor() as executor:
        batch_numbers = list(executor.map(processor.extract_batch_number, filtered_files))
    print(len(batch_numbers))

    times = processor.get_time(batch_numbers, filtered_files)
    print(times)
if __name__ == "__main__":
    main()
