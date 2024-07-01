import os
import re
import pandas as pd
from lxml import etree
from concurrent.futures import ThreadPoolExecutor

class DataFetcher:
    def __init__(self):
        self.path = os.getenv("MY_PATH")

    def file_filter(self, file, start_number, end_number, includes, excludes):
        m_pattern = re.compile(r'M(\d{3})')
        match = m_pattern.search(file)
        if match:
            number = int(match.group(1))
            if start_number <= number <= end_number:
                if any(include.lower() in file.lower() for include in includes) and not any(exclude.lower() in file.lower() for exclude in excludes):
                    return file 
        return None 

    def fetcher(self, filenames, start_number, end_number, includes, excludes):
        with ThreadPoolExecutor() as executor:
            filtered_files = list(executor.map(lambda file: self.file_filter(file, start_number, end_number, includes, excludes), filenames))
        return list(filter(None, filtered_files))

class DataProcessor:
    def __init__(self):
        self.path = os.getenv("MY_PATH")

    def extract_batch_number(self, filename):
        ignore = 'SB8_34633-5543-83_'
        if filename.startswith(ignore):
            filename = filename[len(ignore):]
        start_index = filename.find('M')
        if start_index != -1:
            result = filename[start_index: start_index + 10]
            return result
        return None

    def process(self, file, xpath, namespace):
        results = [file]
        file_path = f'{self.path}/{file}'
        tree = etree.parse(file_path)
        root = tree.getroot()
        for xpath in xpath.values():
            parvalfloats = root.xpath(xpath, namespace=namespace)
            if parvalfloats:
                lst = [parvalfloat for parvalfloat in parvalfloats]
                results.append(str(list(set(lst))[0] if len(set(lst)) == 1 else None))
            else:
                results.append(None)
        return results

def main():
    processor = DataProcessor()
    fetcher = DataFetcher()
    path = fetcher.path
    filenames = os.listdir(path)

    start_number = 234
    end_number = 460
    includes = ["CIP", "516"]
    excludes = ["TEST", "AL", "EXTRA", "BUFF", "SAT", "SIP",]

    filtered_files = fetcher.fetcher(filenames, start_number, end_number, includes, excludes)
    print(len(filtered_files))

if __name__ == "__main__":
    main()
