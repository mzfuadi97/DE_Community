from etl.extract import Extract
from etl.transform import Transform, join_data
from etl.load import Load

class DataPipeline:
    def __init__(self, config):
        self.config = config
        self.extractor = None
        self.transformer = None
        self.loader = None

    def register_source(self, source_type: str, path: str):
        self.extractor = Extract(source_type, path)

    def add_transform(self, transform_func):
        if not self.transformer:
            self.transformer = Transform([])
        self.transformer.transforms.append(transform_func)

    def set_loader(self, destination: str, bucket: str, region: str):
        self.loader = Load(destination, bucket, region)

    def execute(self):
        # Extract
        data = self.extractor.extract_data()

        # Debug: Cek apakah data sudah dalam format list
        print(f"Data after extraction: {data}")

        # Pastikan data adalah list. Jika hanya satu dictionary, bungkus menjadi list
        if isinstance(data, dict):
            data = [data]  # Bungkus menjadi list jika data adalah dict

        print(f"Data after conversion to list: {data}")  # Debug: log data setelah pembungkus

        # Transform
        transformed_data = self.transformer.apply_transforms(data) if self.transformer else data

        print(f"Data after transformation: {transformed_data}")  # Debug: log data setelah transformasi

        # Load
        self.loader.load_data(transformed_data)
