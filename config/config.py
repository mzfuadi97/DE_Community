# config/config.py
import yaml
import os
from typing import Dict

class Config:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self.load_config()
        
    def load_config(self) -> Dict:
        with open(self.config_path, 'r') as file:
            config = yaml.safe_load(file)
        
        # Check for environment variables and override config if present
        self.override_with_env_vars(config)
        return config
    
    def override_with_env_vars(self, config: Dict):
        if 'env' in os.environ:
            config['env'] = os.environ['env']
        
    def get(self, key: str):
        return self.config.get(key)
