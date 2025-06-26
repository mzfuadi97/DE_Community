from datetime import datetime
import random
import string
from typing import List, Dict, Callable

class Transform:
    def __init__(self, transforms: List[Callable]):
        self.transforms = transforms
    
    def apply_transforms(self, data):
        # Jika data adalah list, terapkan transformasi ke setiap item
        if isinstance(data, list):
            print(f"Data before transform (list): {data}")
            for transform in self.transforms:
                # Untuk fungsi join, biasanya menerima dua list, jadi skip jika bukan dict
                if hasattr(transform, '__name__') and transform.__name__ == 'join_data':
                    # join_data harus dipanggil secara eksplisit di pipeline
                    data = transform(*data) if isinstance(data, tuple) else transform(data)
                else:
                    data = [transform(item) for item in data]
            print(f"Data after transform (list): {data}")
            return data
        # Jika data adalah dict, proses seperti sebelumnya
        elif isinstance(data, dict):
            print(f"Data before transform (dict): {data}")
            for transform in self.transforms:
                data = transform(data)
            print(f"Data after transform (dict): {data}")
            return data
        else:
            print(f"Data before transform is not a dict or list: {type(data)}")
            return data


# Fungsi transformasi: parsing timestamp
def parse_timestamp(data: Dict) -> Dict:
    if 'timestamp' in data:
        try:
            # Mengonversi timestamp dari string ke datetime
            data['timestamp'] = datetime.strptime(data['timestamp'], "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError as e:
            print(f"Error parsing timestamp: {e}")
    return data

# Fungsi transformasi: anonymize user_id
def anonymize_user_id(data: Dict) -> Dict:
    if 'user_id' in data:
        # Mengganti user_id dengan ID acak
        data['user_id'] = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
    return data

# Fungsi transformasi: menambah response category
def add_response_category(data: Dict) -> Dict:
    if 'status_code' in data:
        if data['status_code'] >= 200 and data['status_code'] < 300:
            data['response_category'] = 'Success'
        elif data['status_code'] >= 400 and data['status_code'] < 500:
            data['response_category'] = 'Client Error'
        else:
            data['response_category'] = 'Server Error'
    return data

# Fungsi transformasi: join data user_activities dan api_logs berdasarkan user_id

def join_data(user_activities: list, api_logs: list) -> list:
    """
    Melakukan join (merge) dua list of dict berdasarkan key 'user_id'.
    Hasilnya adalah list of dict, setiap dict merupakan hasil merge dari kedua sumber data.
    Jika user_id tidak ditemukan di salah satu sumber, hanya data yang ada yang di-merge (inner join).
    """
    # Buat dict lookup untuk api_logs berdasarkan user_id
    api_logs_dict = {item['user_id']: item for item in api_logs if 'user_id' in item}
    
    merged = []
    for activity in user_activities:
        user_id = activity.get('user_id')
        if user_id in api_logs_dict:
            merged_item = {**activity, **api_logs_dict[user_id]}
            merged.append(merged_item)
    return merged
