import re
from concurrent.futures import ThreadPoolExecutor

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


