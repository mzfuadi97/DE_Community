import pandas as pd
import json
from typing import Dict, List, Any
from datetime import datetime

class DataValidator:
    def __init__(self, schema_path: str | None = None):
        """
        Initialize dengan validation schema
        
        Args:
            schema_path: Path ke file schema (opsional)
        """
        self.schema = self.load_schema(schema_path) if schema_path else self.get_default_schema()
        self.validation_results = []
        
    def load_schema(self, schema_path: str) -> Dict[str, Any]:
        """Load schema dari file JSON"""
        try:
            with open(schema_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading schema: {e}")
            return self.get_default_schema()
    
    def get_default_schema(self) -> Dict[str, Any]:
        """Default schema untuk user activities dan API logs"""
        return {
            "user_activities": {
                "required_fields": ["user_id", "action", "timestamp", "page_url"],
                "data_types": {
                    "user_id": "string",
                    "action": "string",
                    "timestamp": "datetime",
                    "page_url": "string",
                    "device_type": "string"
                },
                "valid_actions": ["view", "click", "purchase", "add_to_cart"]
            },
            "api_logs": {
                "required_fields": ["request_id", "user_id", "endpoint", "status_code", "response_time"],
                "data_types": {
                    "request_id": "string",
                    "user_id": "string",
                    "endpoint": "string",
                    "method": "string",
                    "status_code": "integer",
                    "response_time": "float",
                    "timestamp": "datetime"
                },
                "valid_status_codes": [200, 201, 400, 404, 500]
            }
        }
    
    def validate_schema(self, data: pd.DataFrame, data_type: str = "user_activities") -> Dict[str, Any]:
        """
        Validate data against schema
        
        Args:
            data: DataFrame yang akan divalidasi
            data_type: Tipe data ("user_activities" atau "api_logs")
            
        Returns:
            Dictionary berisi hasil validasi
        """
        schema = self.schema.get(data_type, {})
        validation_result = {
            "data_type": data_type,
            "total_records": len(data),
            "validation_time": datetime.now().isoformat(),
            "errors": [],
            "warnings": [],
            "passed": True
        }
        
        # Check required fields
        required_fields = schema.get("required_fields", [])
        for field in required_fields:
            if field not in data.columns:
                validation_result["errors"].append(f"Missing required field: {field}")
                validation_result["passed"] = False
        
        # Check data types
        data_types = schema.get("data_types", {})
        for field, expected_type in data_types.items():
            if field in data.columns:
                try:
                    if expected_type == "string":
                        if not data[field].dtype == 'object':
                            validation_result["warnings"].append(f"Field {field} should be string type")
                    elif expected_type == "integer":
                        if not pd.api.types.is_integer_dtype(data[field]):
                            validation_result["warnings"].append(f"Field {field} should be integer type")
                    elif expected_type == "float":
                        if not pd.api.types.is_float_dtype(data[field]):
                            validation_result["warnings"].append(f"Field {field} should be float type")
                    elif expected_type == "datetime":
                        try:
                            pd.to_datetime(data[field])
                        except:
                            validation_result["errors"].append(f"Field {field} should be datetime format")
                            validation_result["passed"] = False
                except Exception as e:
                    validation_result["warnings"].append(f"Error checking data type for field {field}: {str(e)}")
        
        # Check valid values
        if data_type == "user_activities" and "action" in data.columns:
            try:
                valid_actions = schema.get("valid_actions", [])
                # Filter out NaN values before checking
                valid_data = data["action"].dropna()
                if len(valid_data) > 0:
                    invalid_actions = valid_data[~valid_data.isin(valid_actions)].tolist()
                    if len(invalid_actions) > 0:
                        validation_result["warnings"].append(f"Invalid actions found: {invalid_actions}")
            except Exception as e:
                validation_result["warnings"].append(f"Error checking valid actions: {str(e)}")
        
        if data_type == "api_logs" and "status_code" in data.columns:
            try:
                valid_status_codes = schema.get("valid_status_codes", [])
                # Filter out NaN values before checking
                valid_data = data["status_code"].dropna()
                if len(valid_data) > 0:
                    invalid_status_codes = valid_data[~valid_data.isin(valid_status_codes)].tolist()
                    if len(invalid_status_codes) > 0:
                        validation_result["warnings"].append(f"Invalid status codes found: {invalid_status_codes}")
            except Exception as e:
                validation_result["warnings"].append(f"Error checking valid status codes: {str(e)}")
        
        # Check for missing values
        for field in required_fields:
            if field in data.columns:
                try:
                    missing_count = data[field].isnull().sum()
                    if missing_count > 0:
                        validation_result["warnings"].append(f"Field {field} has {missing_count} missing values")
                except Exception as e:
                    validation_result["warnings"].append(f"Error checking missing values for field {field}: {str(e)}")
        
        self.validation_results.append(validation_result)
        return validation_result
    
    def validate_business_rules(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Apply business validation rules
        
        Args:
            data: DataFrame yang akan divalidasi
            
        Returns:
            Dictionary berisi hasil validasi business rules
        """
        business_result = {
            "business_rules_validation": True,
            "rules_checked": [],
            "violations": []
        }
        
        # Rule 1: Response time should not be negative
        if "response_time" in data.columns:
            business_result["rules_checked"].append("response_time_positive")
            negative_response_times = data[data["response_time"] < 0]
            if len(negative_response_times) > 0:
                business_result["violations"].append(f"Found {len(negative_response_times)} records with negative response time")
                business_result["business_rules_validation"] = False
        
        # Rule 2: Status code should be between 100-599
        if "status_code" in data.columns:
            business_result["rules_checked"].append("status_code_range")
            invalid_status_codes = data[(data["status_code"] < 100) | (data["status_code"] > 599)]
            if len(invalid_status_codes) > 0:
                business_result["violations"].append(f"Found {len(invalid_status_codes)} records with invalid status codes")
                business_result["business_rules_validation"] = False
        
        # Rule 3: User ID should not be empty
        if "user_id" in data.columns:
            business_result["rules_checked"].append("user_id_not_empty")
            empty_user_ids = data[data["user_id"].isnull() | (data["user_id"] == "")]
            if len(empty_user_ids) > 0:
                business_result["violations"].append(f"Found {len(empty_user_ids)} records with empty user_id")
                business_result["business_rules_validation"] = False
        
        return business_result
    
    def generate_report(self) -> Dict[str, Any]:
        """
        Generate validation report
        
        Returns:
            Dictionary berisi laporan validasi lengkap
        """
        if not self.validation_results:
            return {"error": "No validation results available"}
        
        report = {
            "validation_summary": {
                "total_validations": len(self.validation_results),
                "passed_validations": sum(1 for r in self.validation_results if r["passed"]),
                "failed_validations": sum(1 for r in self.validation_results if not r["passed"]),
                "total_errors": sum(len(r["errors"]) for r in self.validation_results),
                "total_warnings": sum(len(r["warnings"]) for r in self.validation_results)
            },
            "detailed_results": self.validation_results,
            "recommendations": self._generate_recommendations(),
            "generated_at": datetime.now().isoformat()
        }
        
        return report
    
    def _generate_recommendations(self) -> List[str]:
        """Generate recommendations berdasarkan hasil validasi"""
        recommendations = []
        
        total_errors = sum(len(r["errors"]) for r in self.validation_results)
        total_warnings = sum(len(r["warnings"]) for r in self.validation_results)
        
        if total_errors > 0:
            recommendations.append("Fix critical errors before proceeding with data processing")
        
        if total_warnings > 0:
            recommendations.append("Review warnings and consider data quality improvements")
        
        for result in self.validation_results:
            if len(result["errors"]) > 0:
                recommendations.append(f"Address {len(result['errors'])} errors in {result['data_type']}")
        
        if not recommendations:
            recommendations.append("Data quality looks good, proceed with processing")
        
        return recommendations 