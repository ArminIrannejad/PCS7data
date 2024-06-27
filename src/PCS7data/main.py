import pandas as pd
import os
import re
from lxml import etree
from concurrent.futures import ThreadPoolExecutor

class DataFetcher:
    def __init__(self):
        self.path = os.getenv("MY_PATH")

    def fetcher(self):
        pass    

class DataProcessor:
    def __init__(self, path):
        self.path = path

    def file_filter(self, file, start_number, end_number, includes):
        m_pattern = re.compile(r'M(\d{3})')
        match = m_pattern.search(file)
        if match:
            number = int(match.group(1))
            if start_number <= number <= end_number:
                if any(include in file for include in includes):
                    return True
        return False 

    def extract_batch_nummer(self, filename):
        ignore = 'SB8_34633-5543-83_'
        if filename.startswith(ignore):
            filename = filename[len(ignore):]
        start_index = filename.find('M')
        if start_index != -1:
            result = filename[start_index: start_index + 10]
            return result

    def process(self, file):
        if '"' in file:
            return None
        file_path = fr'{self.path}\{file}'
        tree = etree.parse(file_path)
        root = tree.getroot()

        pass

def main():
    pass

if __name__ == "__main__":
    main()
