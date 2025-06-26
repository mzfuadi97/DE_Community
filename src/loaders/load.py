import boto3  # type: ignore
import json
import logging
import os

class Load:
    def __init__(self, destination: str, bucket: str, region: str):
        self.destination = destination
        self.bucket = bucket
        self.region = region
        # Pastikan menggunakan region yang benar
        self.s3_client = boto3.client('s3', region_name=self.region)
    
    def load_data(self, data, s3_key=None, local_file_name=None):
        try:
            data_json = json.dumps(data)
            # Gunakan nama file custom jika diberikan
            file_name = local_file_name if local_file_name else 'output_data.json'
            s3_file_name = s3_key if s3_key else file_name

            # Simpan ke lokal jika diminta
            if self.destination in ['local', 'both']:
                with open(file_name, 'w') as f:
                    f.write(data_json)
                print(f"Data saved locally as {file_name}")

            # Upload ke S3 jika diminta
            if self.destination in ['s3', 'both']:
                try:
                    self.s3_client.put_object(
                        Bucket=self.bucket,
                        Key=s3_file_name,
                        Body=data_json,
                        ContentType='application/json'
                    )
                    print(f"Data loaded to s3://{self.bucket}/{s3_file_name} in region {self.region}")
                except Exception as s3_error:
                    print(f"Error uploading to S3: {s3_error}")
                    logging.error(f"Error uploading to S3: {s3_error}")
        except Exception as e:
            print(f"General error in load_data: {e}")
            logging.error(f"Error loading data: {e}")
