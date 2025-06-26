import pandas as pd
import json
import os
from config.config import Config
from src.loaders.load import Load

def main():
    # Load konfigurasi
    config = Config(config_path='config/config.yaml')
    data_sources = config.get('data_sources') or []
    if len(data_sources) < 2:
        raise ValueError("Config must contain at least two data sources.")

    user_activities_path = data_sources[1].get('path')
    api_logs_path = data_sources[0].get('path')

    # Membaca data ke dalam DataFrame
    activities_df = pd.read_json(user_activities_path, lines=True)
    logs_df = pd.read_json(api_logs_path, lines=True)

    # Join berdasarkan user_id
    merged_df = pd.merge(activities_df, logs_df, on='user_id', how='inner')

    # Simpan hasil join ke file
    merged_df.to_json('output_data.json', orient='records', lines=False, indent=2)

    # Agregasi
    action_counts = merged_df['action'].value_counts().to_dict() if 'action' in merged_df else {}
    page_visit_counts = merged_df['page_url'].value_counts().to_dict() if 'page_url' in merged_df else {}
    device_counts = merged_df['device_type'].value_counts().to_dict() if 'device_type' in merged_df else {}
    merged_df['timestamp_x'] = pd.to_datetime(merged_df['timestamp_x'])
    merged_df['timestamp_y'] = pd.to_datetime(merged_df['timestamp_y'])
    # Hanya isi time_diff jika kolom timestamp_x ada
    if 'timestamp_x' in merged_df:
        merged_df['time_diff'] = merged_df.groupby('user_id')['timestamp_x'].diff()
        merged_df['time_diff'].fillna('0 days 00:00:00', inplace=True)
        avg_time_diff_per_user = merged_df.groupby('user_id')['time_diff'].apply(lambda x: pd.to_timedelta(x).mean()).astype(str).to_dict()
    else:
        avg_time_diff_per_user = {}
    status_code_counts = merged_df['status_code'].value_counts().to_dict() if 'status_code' in merged_df else {}
    avg_response_time_per_endpoint = merged_df.groupby('endpoint')['response_time'].mean().to_dict() if 'endpoint' in merged_df else {}
    request_counts_per_user = merged_df['user_id'].value_counts().to_dict()

    # Simpan hasil agregat ke file
    output_files = [
        ('action_counts.json', action_counts),
        ('page_visit_counts.json', page_visit_counts),
        ('device_counts.json', device_counts),
        ('avg_time_diff_per_user.json', avg_time_diff_per_user),
        ('status_code_counts.json', status_code_counts),
        ('avg_response_time_per_endpoint.json', avg_response_time_per_endpoint),
        ('request_counts_per_user.json', request_counts_per_user),
    ]
    for fname, data in output_files:
        with open(fname, 'w') as f:
            json.dump(data, f, indent=2)

    # Upload semua file output ke S3
    loader = Load(destination='both', bucket='belajarde', region='ap-southeast-2')
    # Upload output_data.json
    with open('output_data.json', 'r') as f:
        loader.load_data(json.load(f), s3_key='output_data.json')
    # Upload agregat
    for fname, _ in output_files:
        with open(fname, 'r') as f:
            loader.load_data(json.load(f), s3_key=fname)

if __name__ == "__main__":
    main()
