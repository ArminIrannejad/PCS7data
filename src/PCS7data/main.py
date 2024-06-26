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
        m_pattern = re.compile(r'M(d\{3})')
        match = m_pattern.search(file)
        if match:
            number = int(match.group(1))
            if start_number <= number <= end_number:
                if any(include in file for include in includes):
                    return True
        return False 
    def process(self):
        pass

def main():
    pass

if __name__ == "__main__":
    main()
