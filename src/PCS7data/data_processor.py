import os
import numpy as np
import pandas as pd
from lxml import etree
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

class DataProcessor:
    """
    Processes data from XML BatchArchives.     
    """
    def __init__(self, path):
        self.path = path

    def extract_batch_number(self, filename):
        """
        
        """
        ignore = 'SB8_34633-5543-83_'
        if filename.startswith(ignore):
            filename = filename[len(ignore):]
        start_index = filename.find('M')
        if start_index != -1:
            result = filename[start_index: start_index + 10]
            return result
        return None

    def recipe_block_id(self, file, xpaths, namespaces):
        """

        """
        results = [file]
        file_path = os.path.join(self.path, file)
        tree = etree.parse(file_path)
        for xpath in xpaths.values():
            sfcands = tree.xpath(xpath, namespaces=namespaces)
            for sfcand in sfcands:
                sfcsteps = sfcand.xpath("./ns:Sfcseq/ns:Sfcstep", namespaces=namespaces)
                sfcsteps_data = []
                for sfcstep in sfcsteps:
                    contid = sfcstep.get("contid")
                    termid = sfcstep.get("termid")
                    name = sfcstep.get("name")
                    sfcsteps_data.append((name, contid, termid))
                
                if sfcsteps_data:
                        results.append(sfcsteps_data)
                else:
                    results.append(None)
        return results

    def process(self, file, xpaths, namespaces, result_type = 'full'):
        """
        
        """
        results = []
        file_path = os.path.join(self.path, file)       
        tree = etree.parse(file_path)
        for xpath in xpaths.values():
            parvalfloats = tree.xpath(xpath, namespaces=namespaces)
            if parvalfloats:
                unique_lst = list(set(parvalfloats))
                if result_type == 'first':
                    results.append(str(unique_lst[0]))
                elif result_type == 'last':
                    results.append(str(unique_lst[-1]))
                else:
                    results.append((unique_lst))
            else:
                results.append(None)
        return results

    def _get_time(self, file):
        """
        Deprecated
        """
        namespaces = {'ns': 'SIMATIC_BATCH_V8_1_0'}
        xpath = "//ns:Archivebatch/ns:Cr"
        file_path = fr'{self.path}/{file}'
        tree = etree.parse(file_path)
        root = tree.getroot()
        parvalfloats = root.xpath(xpath, namespaces=namespaces)
        if not parvalfloats:
            return None
        lst = [f"{parvalfloat.get('actstart')} {parvalfloat.get('actend')}" for parvalfloat in parvalfloats]
        return str(list(set(lst))[-1]).split() if len(set(lst)) == 1 else None    

    def _time_diff(self, row):
        """
        Deprecated
        """
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
        """
        Deprecated
        """
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

    def time_difference(self, timestamp_lst):
        """

        """
        if None in timestamp_lst:
            return None
        elif timestamp_lst is None:
            return None
        dt_obj = [datetime.fromisoformat(ts) for ts in timestamp_lst]
        start = min(dt_obj)
        end = max(dt_obj)
        diff = end - start
        total_seconds = int(diff.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        diff_str = f"{hours:02}:{minutes:02}:{seconds:02}"
        return diff_str, start, end
