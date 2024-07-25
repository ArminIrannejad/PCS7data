import os
import pandas as pd
from lxml import etree
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

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

    def process(self, file, xpaths, namespaces, result_type = 'full'):
        results = [file]
        file_path = os.path.join(self.path, file)       
        tree = etree.parse(file_path)
        root = tree.getroot()
        for xpath in xpaths.values():
            parvalfloats = root.xpath(xpath, namespaces=namespaces)
            if parvalfloats:
                lst = [parvalfloat for parvalfloat in parvalfloats]
                unique_lst = list(set(lst))
                if result_type == 'first':
                    results.append(str(unique_lst[0]))
                elif result_type == 'last':
                    results.append(str(unique_lst[-1]))
                else:
                    results.append(str(unique_lst))
            else:
                results.append(None)
        return results

    def _get_time(self, file):
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

    def _time_diff(self, row):
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
            timestamps = list(executor.map(self._get_time, files))
        start_times = [timestamp[0] if timestamp is not None else None for timestamp in timestamps]
        end_times = [timestamp[1] if timestamp is not None else None for timestamp in timestamps]
        dict_time = {
                'Batch_number': batch_numbers,
                'Start_time': start_times,
                'End_time': end_times, 
                'File': files,
        }
        df = pd.DataFrame(dict_time)
        df[['Difference', 'Total_Seconds']] = df.apply(self._time_diff, axis=1)
        df = df.dropna(subset=['Start_time', 'End_time', 'Batch_number'])
        df = df[['Batch_number', 'Difference', 'Start_time', 'End_time', 'File', 'Total_Seconds']]  
        return df

    def time_for_block(self, xpath):
        pass

