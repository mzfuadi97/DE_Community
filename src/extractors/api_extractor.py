import requests
import time
import logging
from typing import Dict, List, Optional, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json

class APIExtractor:
    def __init__(self, base_url: str, api_key: Optional[str] = None, 
                 rate_limit_per_minute: int = 60, timeout: int = 30):
        """
        Initialize API Extractor dengan rate limiting dan retry mechanism
        
        Args:
            base_url: Base URL untuk API
            api_key: API key untuk authentication
            rate_limit_per_minute: Rate limit per menit
            timeout: Timeout untuk request dalam detik
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.rate_limit_per_minute = rate_limit_per_minute
        self.timeout = timeout
        self.last_request_time = 0
        self.request_count = 0
        self.session = self._create_session()
        
    def _create_session(self) -> requests.Session:
        """Create session dengan retry mechanism"""
        session = requests.Session()
        
        # Retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _handle_rate_limiting(self):
        """Handle rate limiting dengan delay jika diperlukan"""
        current_time = time.time()
        time_diff = current_time - self.last_request_time
        
        # Reset counter jika sudah lebih dari 1 menit
        if time_diff >= 60:
            self.request_count = 0
            self.last_request_time = current_time
        
        # Check rate limit
        if self.request_count >= self.rate_limit_per_minute:
            sleep_time = 60 - time_diff
            if sleep_time > 0:
                logging.info(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
                self.request_count = 0
                self.last_request_time = time.time()
        
        self.request_count += 1
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None, 
                     headers: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Make HTTP request dengan error handling
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            headers: Request headers
            
        Returns:
            Response data sebagai dictionary
        """
        self._handle_rate_limiting()
        
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        # Default headers
        default_headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'ETL-Pipeline/1.0'
        }
        
        if self.api_key:
            default_headers['Authorization'] = f'Bearer {self.api_key}'
        
        if headers:
            default_headers.update(headers)
        
        try:
            response = self.session.get(
                url, 
                params=params, 
                headers=default_headers, 
                timeout=self.timeout
            )
            
            response.raise_for_status()
            
            # Try to parse JSON response
            try:
                return response.json()
            except json.JSONDecodeError:
                logging.warning(f"Response is not JSON: {response.text[:100]}")
                return {'raw_response': response.text}
                
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed: {e}")
            return {'error': str(e), 'status_code': getattr(e.response, 'status_code', None)}
    
    def get_user_profile(self, user_id: str) -> Dict[str, Any]:
        """
        Get user profile dari randomuser.me API (tanpa user_id, hanya ambil data acak)
        """
        # randomuser.me tidak mendukung pencarian user_id, hanya random
        result = self._make_request('')
        if 'results' in result and len(result['results']) > 0:
            user = result['results'][0]
            # Mapping ke struktur pipeline
            return {
                'age': user.get('dob', {}).get('age'),
                'gender': user.get('gender'),
                'is_premium': False,  # randomuser.me tidak punya info premium
                'join_date': user.get('registered', {}).get('date'),
                'location': user.get('location', {}).get('city')
            }
        return {'error': 'No user data'}
    
    def get_geolocation(self, ip_address: str) -> Dict[str, Any]:
        """
        Get geolocation data dari ip-api.com berdasarkan IP address
        
        Args:
            ip_address: IP address untuk dicari lokasinya
            
        Returns:
            Geolocation data
        """
        params = {'query': ip_address}
        result = self._make_request('', params=params)
        
        # Mapping response ip-api.com ke struktur pipeline
        if result.get('status') == 'success':
            return {
                'country': result.get('country'),
                'city': result.get('city'),
                'lat': result.get('lat'),
                'lon': result.get('lon'),
                'timezone': result.get('timezone'),
                'region': result.get('regionName'),
                'isp': result.get('isp')
            }
        return {'error': 'Geolocation lookup failed'}
    
    def get_weather_data(self, lat: float, lon: float, timestamp: str) -> Dict[str, Any]:
        """
        Get weather data berdasarkan koordinat dan timestamp
        
        Args:
            lat: Latitude
            lon: Longitude
            timestamp: Timestamp untuk data cuaca
            
        Returns:
            Weather data
        """
        params = {
            'lat': lat,
            'lon': lon,
            'dt': timestamp
        }
        return self._make_request('/weather', params=params)
    
    def close(self):
        """Close session"""
        self.session.close() 