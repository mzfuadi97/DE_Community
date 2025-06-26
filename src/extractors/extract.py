import json
import logging
from typing import List, Dict

class Extract:
    def __init__(self, source_type: str, path: str):
        self.source_type = source_type
        self.path = path
    
    def extract_data(self) -> List[Dict]:
        data = []
        try:
            with open(self.path, 'r') as file:
                for line in file:
                    record = json.loads(line.strip())  # Mengubah setiap baris JSON menjadi dictionary
                    data.append(record)  # Pastikan data adalah list of dictionaries
                    print(f"Extracted record: {record}")  # Debugging: log data yang diambil
        except Exception as e:
            logging.error(f"Error extracting data from {self.path}: {e}")
        return data  # Mengembalikan data dalam bentuk list of dictionaries
