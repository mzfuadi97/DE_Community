import pandas as pd
import logging
from typing import Dict, List, Optional, Any
from src.extractors.api_extractor import APIExtractor
import time

class DataEnrichment:
    def __init__(self, api_config: Dict[str, Any]):
        """
        Initialize Data Enrichment dengan konfigurasi API
        
        Args:
            api_config: Dictionary berisi konfigurasi untuk berbagai API
        """
        self.api_config = api_config
        self.api_extractors = {}
        self._initialize_api_extractors()
        
    def _initialize_api_extractors(self):
        """Initialize API extractors berdasarkan konfigurasi"""
        for api_name, config in self.api_config.items():
            # Skip global_settings dan API yang tidak punya base_url
            if api_name == 'global_settings' or 'base_url' not in config:
                continue
                
            # Skip API yang disabled
            if config.get('enabled', True) == False:
                continue
                
            try:
                self.api_extractors[api_name] = APIExtractor(
                    base_url=config['base_url'],
                    api_key=config.get('api_key'),
                    rate_limit_per_minute=config.get('rate_limit_per_minute', 60),
                    timeout=config.get('timeout', 30)
                )
                logging.info(f"Initialized API extractor for {api_name}")
            except Exception as e:
                logging.error(f"Failed to initialize API extractor for {api_name}: {e}")
    
    def enrich_user_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich user data dengan informasi dari external APIs
        
        Args:
            df: DataFrame dengan data user activities dan API logs
            
        Returns:
            DataFrame yang sudah di-enrich
        """
        enriched_df = df.copy()
        
        # Enrich dengan user profile data
        if 'user_profile_api' in self.api_extractors:
            enriched_df = self._enrich_user_profiles(enriched_df)
        
        # Skip geolocation enrichment karena ip-api.com tidak stabil
        # if 'geolocation_api' in self.api_extractors and 'ip_address' in enriched_df.columns:
        #     enriched_df = self._enrich_geolocation(enriched_df)
        
        # Enrich dengan weather data (jika ada koordinat)
        if 'weather_api' in self.api_extractors and 'latitude' in enriched_df.columns:
            enriched_df = self._enrich_weather_data(enriched_df)
        
        return enriched_df
    
    def _enrich_user_profiles(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich data dengan user profile dari external API"""
        unique_users = df['user_id'].unique()
        user_profiles = {}
        
        # Batasi jumlah user yang di-enrich untuk menghindari rate limit
        max_users_to_enrich = min(10, len(unique_users))  # Max 10 user saja
        users_to_enrich = unique_users[:max_users_to_enrich]
        
        for user_id in users_to_enrich:
            try:
                profile = self.api_extractors['user_profile_api'].get_user_profile(user_id)
                if 'error' not in profile:
                    user_profiles[user_id] = profile
                time.sleep(0.5)  # Increase delay untuk menghindari rate limit
            except Exception as e:
                logging.error(f"Failed to get profile for user {user_id}: {e}")
        
        # Add profile data ke DataFrame
        profile_data = []
        for user_id in df['user_id']:
            profile = user_profiles.get(user_id, {})
            profile_data.append({
                'user_age': profile.get('age'),
                'user_gender': profile.get('gender'),
                'user_premium': profile.get('is_premium', False),
                'user_join_date': profile.get('join_date'),
                'user_location': profile.get('location')
            })
        
        profile_df = pd.DataFrame(profile_data)
        return pd.concat([df, profile_df], axis=1)
    
    def _enrich_geolocation(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich data dengan geolocation dari IP address"""
        unique_ips = df['ip_address'].unique()
        geo_data = {}
        
        for ip in unique_ips:
            if pd.notna(ip) and ip != '':
                try:
                    geo = self.api_extractors['geolocation_api'].get_geolocation(ip)
                    if 'error' not in geo:
                        geo_data[ip] = geo
                    time.sleep(0.1)  # Small delay untuk rate limiting
                except Exception as e:
                    logging.error(f"Failed to get geolocation for IP {ip}: {e}")
        
        # Add geolocation data ke DataFrame
        geo_info = []
        for ip in df['ip_address']:
            geo = geo_data.get(ip, {})
            geo_info.append({
                'country': geo.get('country'),
                'city': geo.get('city'),
                'latitude': geo.get('lat'),
                'longitude': geo.get('lon'),
                'timezone': geo.get('timezone')
            })
        
        geo_df = pd.DataFrame(geo_info)
        return pd.concat([df, geo_df], axis=1)
    
    def _enrich_weather_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich data dengan weather information"""
        weather_data = []
        
        for idx, row in df.iterrows():
            latitude = row.get('latitude')
            longitude = row.get('longitude')
            
            if latitude is not None and longitude is not None:
                try:
                    # Convert timestamp ke format yang sesuai untuk weather API
                    timestamp_val = row.get('timestamp_x') or row.get('timestamp')
                    if timestamp_val is not None:
                        try:
                            timestamp = pd.to_datetime(timestamp_val)
                            timestamp_str = str(int(timestamp.timestamp()))
                            
                            weather = self.api_extractors['weather_api'].get_weather_data(
                                lat=latitude,
                                lon=longitude,
                                timestamp=timestamp_str
                            )
                            
                            if 'error' not in weather:
                                weather_data.append({
                                    'temperature': weather.get('main', {}).get('temp'),
                                    'humidity': weather.get('main', {}).get('humidity'),
                                    'weather_condition': weather.get('weather', [{}])[0].get('main'),
                                    'weather_description': weather.get('weather', [{}])[0].get('description')
                                })
                            else:
                                weather_data.append({
                                    'temperature': None,
                                    'humidity': None,
                                    'weather_condition': None,
                                    'weather_description': None
                                })
                        except (ValueError, TypeError):
                            weather_data.append({
                                'temperature': None,
                                'humidity': None,
                                'weather_condition': None,
                                'weather_description': None
                            })
                    else:
                        weather_data.append({
                            'temperature': None,
                            'humidity': None,
                            'weather_condition': None,
                            'weather_description': None
                        })
                    
                    time.sleep(0.1)  # Small delay untuk rate limiting
                    
                except Exception as e:
                    logging.error(f"Failed to get weather data for row {idx}: {e}")
                    weather_data.append({
                        'temperature': None,
                        'humidity': None,
                        'weather_condition': None,
                        'weather_description': None
                    })
            else:
                weather_data.append({
                    'temperature': None,
                    'humidity': None,
                    'weather_condition': None,
                    'weather_description': None
                })
        
        weather_df = pd.DataFrame(weather_data)
        return pd.concat([df, weather_df], axis=1)
    
    def close(self):
        """Close semua API extractors"""
        for extractor in self.api_extractors.values():
            extractor.close() 