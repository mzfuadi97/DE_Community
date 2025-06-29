# Advanced ETL Pipeline Project

## Overview

Project ini adalah sistem ETL (Extract, Transform, Load) yang advanced dengan fitur-fitur production-ready termasuk data validation, external API integration, monitoring, dan alerting.

## Struktur Repository

```
project_root/
├── src/
│   ├── extractors/
│   │   ├── api_extractor.py      # External API integration
│   │   └── extract.py            # Data extraction utilities
│   ├── transformers/
│   │   ├── enrichment.py         # Data enrichment with external APIs
│   │   ├── transform.py          # Data transformation logic
│   │   └── validation.py         # Data validation framework
│   ├── loaders/
│   │   └── load.py               # Data loading to local/S3
│   └── utils/
│       ├── alerting.py           # Error alerting system
│       ├── monitoring.py         # Performance monitoring
│       └── retry.py              # Retry mechanisms
├── config/
│   ├── config.yaml               # Main pipeline configuration
│   ├── api_config.yaml           # External API configuration
│   └── config.py                 # Configuration loader
├── tests/                        # Unit and integration tests
├── docs/                         # Documentation
├── requirements.txt              # Python dependencies
├── main.py                       # Main pipeline execution
└── README.md                     # This file
```

## Key Features

### 1. Data Validation Framework
- **Schema validation** - Validasi tipe data, required fields, dan format
- **Business rules validation** - Custom business logic validation
- **Data quality checks** - Deteksi missing values, outliers, dan duplicates
- **Validation reporting** - Generate laporan validasi dengan metrics dan rekomendasi

### 2. External API Integration
- **User Profile Enrichment** - Menggunakan [randomuser.me](https://randomuser.me/) untuk data user acak
- **Geolocation Data** - Menggunakan [ip-api.com](http://ip-api.com/) untuk lokasi berdasarkan IP
- **Weather Data** - Menggunakan OpenWeatherMap API untuk data cuaca
- **Rate limiting & retry** - Built-in protection untuk API throttling

### 3. Advanced ETL Pipeline
- **Modular architecture** - Extract, Transform, Load components terpisah
- **Data enrichment** - Enrich data dengan informasi dari external APIs
- **Aggregation engine** - Multiple aggregation types (frequency, performance, usage)
- **Dual output** - Save locally dan upload ke S3 simultaneously

### 4. Monitoring & Alerting
- **Performance tracking** - Monitor execution time dan throughput
- **Error handling** - Comprehensive error handling dengan logging
- **Alerting system** - Notifikasi untuk failures dan issues

## External APIs Used

### 1. User Profile API (randomuser.me)
- **URL**: `https://randomuser.me/api/`
- **Authentication**: None (public API)
- **Rate Limit**: ~100 requests/minute
- **Function**: Generate random user profiles (age, gender, location)
- **Response Format**: JSON

### 2. Geolocation API (ip-api.com)
- **URL**: `http://ip-api.com/json`
- **Authentication**: None (public API)
- **Rate Limit**: 45 requests/minute (free tier)
- **Function**: Get location data from IP address
- **Response Format**: JSON

### 3. Weather API (OpenWeatherMap)
- **URL**: `https://api.openweathermap.org/data/2.5`
- **Authentication**: API Key required
- **Rate Limit**: 60 requests/minute (free tier)
- **Function**: Get weather data by coordinates
- **Response Format**: JSON

## Configuration

### Main Configuration (`config/config.yaml`)
```yaml
data_sources:
  - name: "api_logs"
    path: "api_logs.jsonl"
    type: "jsonl"
  - name: "user_activities"
    path: "user_activities.jsonl"
    type: "jsonl"

output:
  destination: "both"  # local, s3, or both
  s3_bucket: "your-bucket-name"
  s3_region: "ap-southeast-2"
```

### API Configuration (`config/api_config.yaml`)
```yaml
user_profile_api:
  base_url: "https://randomuser.me/api/"
  api_key: null
  rate_limit_per_minute: 100
  timeout: 30
  enabled: true

geolocation_api:
  base_url: "http://ip-api.com/json"
  api_key: null
  rate_limit_per_minute: 45
  timeout: 10
  enabled: true

weather_api:
  base_url: "https://api.openweathermap.org/data/2.5"
  api_key: "${OPENWEATHER_API_KEY}"
  rate_limit_per_minute: 60
  timeout: 15
  enabled: true
```

## Installation & Setup

### Prerequisites
- Python 3.8+
- pip
- AWS CLI (untuk S3 upload)
- Virtual environment (recommended)

### Setup Steps

1. **Clone dan setup environment**
   ```bash
   git clone <repository-url>
   cd <project-directory>
   python -m venv belajarde
   ```

2. **Activate virtual environment**
   ```bash
   # Windows
   .\belajarde\Scripts\Activate.ps1
   
   # Linux/Mac
   source belajarde/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install pandas boto3 requests pyyaml
   ```

4. **Configure AWS (untuk S3)**
   ```bash
   aws configure
   ```

5. **Setup environment variables (optional)**
   ```bash
   export OPENWEATHER_API_KEY="your-api-key"
   ```

## Running the Pipeline

### Basic Execution
```bash
python main.py
```

### Pipeline Flow
1. **Data Extraction** - Load user activities dan API logs
2. **Data Validation** - Validate schema dan business rules
3. **Data Join** - Merge data berdasarkan user_id
4. **Data Enrichment** - Enrich dengan external APIs
5. **Aggregation** - Generate multiple aggregation reports
6. **Data Loading** - Save locally dan upload ke S3

### Output Files
- `output_data.json` - Merged and enriched data
- `validation_report.json` - Data validation results
- `action_counts.json` - Action frequency per user
- `page_visit_counts.json` - Page visit statistics
- `device_counts.json` - Device type distribution
- `avg_time_diff_per_user.json` - Time-based analytics
- `status_code_counts.json` - API response statistics
- `avg_response_time_per_endpoint.json` - Performance metrics
- `request_counts_per_user.json` - Request frequency
- `age_distribution.json` - User age demographics (if enriched)
- `gender_distribution.json` - User gender stats (if enriched)
- `country_distribution.json` - Geographic distribution (if enriched)
- `weather_distribution.json` - Weather conditions (if enriched)

## Data Validation

### Schema Validation
- Required fields checking
- Data type validation
- Format validation (datetime, email, etc.)
- Value range validation

### Business Rules Validation
- Response time must be positive
- Status codes must be valid HTTP codes
- User IDs must not be empty
- Custom business logic validation

### Validation Report
```json
{
  "validation_summary": {
    "total_validations": 2,
    "passed_validations": 2,
    "failed_validations": 0,
    "total_errors": 0,
    "total_warnings": 0
  },
  "detailed_results": [...],
  "recommendations": [...]
}
```

## Error Handling

### API Integration Errors
- Network timeout handling
- Rate limit protection
- Retry mechanisms with exponential backoff
- Graceful degradation when APIs fail

### Data Processing Errors
- Missing data handling
- Invalid format recovery
- Partial failure recovery
- Comprehensive error logging

## Monitoring & Performance

### Performance Metrics
- Execution time per stage
- Data processing throughput
- API call success rates
- Memory usage tracking

### Logging
- Structured logging with different levels
- Performance metrics logging
- Error tracking and reporting
- Audit trail for data processing

## Development & Testing

### Project Structure
- Modular design for easy testing
- Separation of concerns
- Configurable components
- Extensible architecture

### Testing Strategy
- Unit tests for individual components
- Integration tests for API calls
- End-to-end pipeline testing
- Performance benchmarking

### Best Practices
- Code documentation
- Type hints
- Error handling
- Logging standards
- Configuration management

## Troubleshooting

### Common Issues
1. **API Rate Limiting** - Pipeline automatically handles rate limits
2. **Network Timeouts** - Retry mechanisms with exponential backoff
3. **Missing Data** - Graceful handling with validation warnings
4. **S3 Upload Failures** - Local backup with retry logic

### Debug Mode
Enable detailed logging by setting log level to DEBUG in main.py

## Future Enhancements

### Planned Features
- Real-time data processing
- Machine learning integration
- Advanced analytics dashboard
- Multi-tenant support
- Cloud-native deployment

### Scalability Improvements
- Parallel processing
- Distributed computing
- Caching mechanisms
- Database integration

---

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## Support

For issues and questions:
- Create an issue in the repository
- Check the documentation in `/docs`
- Review the troubleshooting section 