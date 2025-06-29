import pandas as pd
import json
import os
import yaml
import logging
from config.config import Config
from src.loaders.load import Load
from src.transformers.enrichment import DataEnrichment
from src.transformers.validation import DataValidator

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_api_config(config_path: str = 'config/api_config.yaml') -> dict:
    """Load konfigurasi API dari file YAML"""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Replace environment variables
        for api_name, api_config in config.items():
            if isinstance(api_config, dict) and 'api_key' in api_config:
                if isinstance(api_config['api_key'], str) and api_config['api_key'].startswith('${'):
                    env_var = api_config['api_key'][2:-1]  # Remove ${ and }
                    api_config['api_key'] = os.getenv(env_var)
        
        return config
    except Exception as e:
        logger.error(f"Failed to load API config: {e}")
        return {}

def main():
    # Load konfigurasi
    config = Config(config_path='config/config.yaml')
    data_sources = config.get('data_sources') or []
    if len(data_sources) < 2:
        raise ValueError("Config must contain at least two data sources.")

    user_activities_path = data_sources[1].get('path')
    api_logs_path = data_sources[0].get('path')

    # Load API configuration
    api_config = load_api_config()
    if not api_config:
        logger.warning("No API configuration found. Running without external API enrichment.")
        api_config = {}

    # Membaca data ke dalam DataFrame
    logger.info("Reading data files...")
    activities_df = pd.read_json(user_activities_path, lines=True)
    logs_df = pd.read_json(api_logs_path, lines=True)

    # Data Validation
    logger.info("Validating data...")
    validator = DataValidator()
    
    # Validate user activities
    activities_validation = validator.validate_schema(activities_df, "user_activities")
    activities_business = validator.validate_business_rules(activities_df)
    
    # Validate API logs
    logs_validation = validator.validate_schema(logs_df, "api_logs")
    logs_business = validator.validate_business_rules(logs_df)
    
    # Generate validation report
    validation_report = validator.generate_report()
    
    # Save validation report
    with open('validation_report.json', 'w') as f:
        json.dump(validation_report, f, indent=2)
    
    logger.info(f"Validation completed. Errors: {validation_report['validation_summary']['total_errors']}, Warnings: {validation_report['validation_summary']['total_warnings']}")

    # Join berdasarkan user_id
    logger.info("Joining data...")
    merged_df = pd.merge(activities_df, logs_df, on='user_id', how='inner')

    # Enrich data dengan external APIs
    if api_config:
        logger.info("Enriching data with external APIs...")
        try:
            enrichment = DataEnrichment(api_config)
            enriched_df = enrichment.enrich_user_data(merged_df)
            enrichment.close()
            logger.info("Data enrichment completed successfully")
        except Exception as e:
            logger.error(f"Data enrichment failed: {e}")
            enriched_df = merged_df
    else:
        enriched_df = merged_df

    # Simpan hasil join ke file
    enriched_df.to_json('output_data.json', orient='records', lines=False, indent=2)

    # Agregasi dengan data yang sudah di-enrich
    logger.info("Generating aggregated reports...")
    
    # Basic aggregations
    action_counts = enriched_df['action'].value_counts().to_dict() if 'action' in enriched_df else {}
    page_visit_counts = enriched_df['page_url'].value_counts().to_dict() if 'page_url' in enriched_df else {}
    device_counts = enriched_df['device_type'].value_counts().to_dict() if 'device_type' in enriched_df else {}
    
    # Time-based aggregations
    enriched_df['timestamp_x'] = pd.to_datetime(enriched_df['timestamp_x'])
    enriched_df['timestamp_y'] = pd.to_datetime(enriched_df['timestamp_y'])
    
    if 'timestamp_x' in enriched_df:
        enriched_df['time_diff'] = enriched_df.groupby('user_id')['timestamp_x'].diff()
        enriched_df['time_diff'].fillna('0 days 00:00:00', inplace=True)
        avg_time_diff_per_user = enriched_df.groupby('user_id')['time_diff'].apply(lambda x: pd.to_timedelta(x).mean()).astype(str).to_dict()
    else:
        avg_time_diff_per_user = {}
    
    # API-related aggregations
    status_code_counts = enriched_df['status_code'].value_counts().to_dict() if 'status_code' in enriched_df else {}
    avg_response_time_per_endpoint = enriched_df.groupby('endpoint')['response_time'].mean().to_dict() if 'endpoint' in enriched_df else {}
    request_counts_per_user = enriched_df['user_id'].value_counts().to_dict()

    # New enriched aggregations
    enriched_aggregations = {}
    
    # User profile based aggregations
    if 'user_age' in enriched_df:
        age_distribution = enriched_df['user_age'].value_counts().to_dict()
        enriched_aggregations['age_distribution'] = age_distribution
    
    if 'user_gender' in enriched_df:
        gender_distribution = enriched_df['user_gender'].value_counts().to_dict()
        enriched_aggregations['gender_distribution'] = gender_distribution
    
    if 'user_premium' in enriched_df:
        premium_user_stats = enriched_df['user_premium'].value_counts().to_dict()
        enriched_aggregations['premium_user_stats'] = premium_user_stats
    
    # Location based aggregations
    if 'country' in enriched_df:
        country_distribution = enriched_df['country'].value_counts().to_dict()
        enriched_aggregations['country_distribution'] = country_distribution
    
    if 'city' in enriched_df:
        city_distribution = enriched_df['city'].value_counts().to_dict()
        enriched_aggregations['city_distribution'] = city_distribution
    
    # Weather based aggregations
    if 'weather_condition' in enriched_df:
        weather_distribution = enriched_df['weather_condition'].value_counts().to_dict()
        enriched_aggregations['weather_distribution'] = weather_distribution
    
    if 'temperature' in enriched_df:
        temp_stats = {
            'avg_temperature': enriched_df['temperature'].mean(),
            'min_temperature': enriched_df['temperature'].min(),
            'max_temperature': enriched_df['temperature'].max()
        }
        enriched_aggregations['temperature_stats'] = temp_stats

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
    
    # Add enriched aggregations
    for name, data in enriched_aggregations.items():
        output_files.append((f'{name}.json', data))
    
    for fname, data in output_files:
        with open(fname, 'w') as f:
            json.dump(data, f, indent=2)

    # Upload semua file output ke S3
    logger.info("Uploading files to S3...")
    loader = Load(destination='both', bucket='belajarde', region='ap-southeast-2')
    
    # Upload main data
    with open('output_data.json', 'r') as f:
        loader.load_data(json.load(f), s3_key='output_data.json')
    
    # Upload validation report
    with open('validation_report.json', 'r') as f:
        loader.load_data(json.load(f), s3_key='validation_report.json')
    
    # Upload aggregations
    for fname, _ in output_files:
        with open(fname, 'r') as f:
            loader.load_data(json.load(f), s3_key=fname)
    
    logger.info("Pipeline completed successfully!")

if __name__ == "__main__":
    main()
