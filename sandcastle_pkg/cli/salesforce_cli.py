#!/usr/bin/env python3
"""
Salesforce CLI Wrapper

Author: Ken Brill
Version: 1.0
Date: December 24, 2025
License: MIT License
"""

import subprocess
import json
import tempfile
import csv
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List

# Query log file path
QUERY_LOG_FILE = Path(__file__).parent / "logs" / "queries.csv"

def log_query(query: str, org_alias: str = "", cached: bool = False):
    """Log a SOQL query to CSV for duplicate detection and caching analysis"""
    try:
        QUERY_LOG_FILE.parent.mkdir(exist_ok=True)
        file_exists = QUERY_LOG_FILE.exists()
        
        with open(QUERY_LOG_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['Timestamp', 'Org', 'Cached', 'Query'])
            writer.writerow([datetime.now().isoformat(), org_alias, 'YES' if cached else 'NO', query])
    except Exception:
        # Don't let logging failures break the migration
        pass

class SalesforceCLI:
    def __init__(self, target_org: Optional[str] = None):
        """
        Initializes the SalesforceCLI wrapper, optionally targeting a specific Salesforce org.
        """
        self.target_org = target_org
        self._org_info_cache: Dict[str, Any] = {} # Cache org info per target_org
        self._query_cache: Dict[str, Optional[List[Dict[str, Any]]]] = {} # Cache for query results

    def update_record(self, sobject_type: str, record_id: str, data: Dict[str, Any]) -> bool:
        """
        Updates a record in Salesforce using the CLI.
        Returns True if successful, False otherwise.
        """
        # Convert data dict to key=value pairs (space-separated, not comma-separated)
        value_pairs = []
        for key, value in data.items():
            # Handle string values with proper quoting
            if isinstance(value, str):
                clean_value = value.replace('\n', ' ').replace('\r', '').replace("'", "")
                value_pairs.append(f"{key}='{clean_value}'")
            else:
                value_pairs.append(f"{key}='{value}'")
        values_str = ' '.join(value_pairs)
        command_args = [
            'data', 'record', 'update',
            '--sobject', sobject_type,
            '--record-id', record_id,
            '--values', values_str
        ]
        result = self._execute_sf_command(command_args)
        return result is not None

    def _execute_sf_command(self, command_args: list) -> Optional[Dict[str, Any]]:
        """
        Executes a Salesforce CLI command and returns the JSON output.
        Includes --target-org if self.target_org is set.
        This version now extracts detailed error messages from JSON output.
        """
        command_with_args = ['sf'] + command_args
        if self.target_org:
            command_with_args.extend(['--target-org', self.target_org])
        
        full_command = command_with_args + ['--json']
        
        try:
            result = subprocess.run(
                full_command,
                capture_output=True,
                text=True,
                check=False, # Do not raise CalledProcessError automatically
                encoding='utf-8'
            )
            
            json_output = {}
            if result.stdout:
                try:
                    json_output = json.loads(result.stdout)
                except json.JSONDecodeError:
                    # If stdout is not JSON, it might be a general SF CLI error or warning
                    print(f"WARNING: SF CLI output was not JSON for command: {' '.join(full_command)}")
                    print(f"STDOUT: {result.stdout}")

            # Check for error based on return code and JSON status
            if result.returncode != 0 or json_output.get('status') != 0:
                error_message = json_output.get('message', 'Unknown error from Salesforce CLI.')
                # Also include stderr if present, for non-JSON errors
                if result.stderr:
                    error_message += f"\nSTDERR: {result.stderr}"
                
                # Create a custom exception with the full error data
                error = RuntimeError(f"SF CLI command failed: {error_message}")
                error.sf_error_data = json_output  # Attach the full JSON response
                raise error

            return json_output
        except FileNotFoundError:
            raise RuntimeError("Salesforce CLI ('sf') command not found. Please ensure it is installed and in your PATH.")
        except Exception as e:
            # Preserve sf_error_data if it exists on the original exception
            new_error = RuntimeError(f"An unexpected error occurred while running an SF CLI command: {e}")
            if hasattr(e, 'sf_error_data'):
                new_error.sf_error_data = e.sf_error_data
            raise new_error from e
    
    def get_org_info(self) -> Optional[Dict[str, Any]]:
        """
        Retrieves and caches information about the connected Salesforce org.
        Queries the specific org targeted by self.target_org, or the default org if not specified.
        """
        cache_key = self.target_org if self.target_org else 'default'
        if cache_key in self._org_info_cache:
            return self._org_info_cache[cache_key]

        try:
            # sf org display --target-org <alias> is a valid command. 
            result = self._execute_sf_command(['org', 'display'])
            
            if result and result.get('status') == 0:
                self._org_info_cache[cache_key] = result.get('result')
                return self._org_info_cache[cache_key]
            else:
                print(f"Could not get org information. SF command result: {result}")
                return None
        except Exception as e:
            # The exception from _execute_sf_command will be more specific.
            print(f"Failed to execute 'org display' for '{cache_key}'.")
            raise e

    def get_organization_details(self) -> Optional[Dict[str, Any]]:
        """
        Queries the Organization object to get details like IsSandbox.
        """
        try:
            query = "SELECT IsSandbox FROM Organization LIMIT 1"
            records = self.query_records(query)
            if records:
                return records[0]
            return None
        except Exception as e:
            print(f"Could not query Organization details: {e}")
            return None

    def is_sandbox(self) -> bool:
        """
        Checks if the connected org is a sandbox. It first checks the IsSandbox
        field on the Organization object for the most reliable result. If that
        fails, it falls back to checking if the instance URL contains '.sandbox.'.
        """
        # Primary, most reliable method:
        org_details = self.get_organization_details()
        if org_details:
            return org_details.get('IsSandbox', False)

        # Fallback method:
        print("Warning: Could not determine sandbox status from Organization object. Falling back to URL check.")
        org_info = self.get_org_info()
        if org_info and 'instanceUrl' in org_info:
            instance_url = org_info.get('instanceUrl', '').lower()
            return '.sandbox.' in instance_url
        
        return False

    def get_record(self, sobject_type: str, record_id: str) -> Optional[Dict[str, Any]]:
        """
        Gets a single record by its ID using 'sf data get record'.
        """
        try:
            command_args = ['data', 'get', 'record', '--sobject', sobject_type, '--record-id', record_id]
            result = self._execute_sf_command(command_args)
            if result and result.get('status') == 0:
                return result.get('result')
            else:
                print(f"Could not get record for {sobject_type} with ID {record_id}. SF command result: {result}")
                return None
        except Exception as e:
            print(f"Error getting {sobject_type} record with ID {record_id}.")
            raise e

    def get_record_by_name(self, sobject_type: str, name: str) -> Optional[Dict[str, Any]]:
        """
        Queries for a single record by its Name field, with in-memory caching.
        """
        if not hasattr(self, '_get_record_by_name_cache'):
            self._get_record_by_name_cache = {}
        cache_key = (sobject_type, name)
        if cache_key in self._get_record_by_name_cache:
            return self._get_record_by_name_cache[cache_key]
        safe_name = name.replace("'", "'\''")
        query = f"SELECT Id, Name FROM {sobject_type} WHERE Name = '{safe_name}' LIMIT 1"
        try:
            result = self._execute_sf_command(['data', 'query', '--query', query])
            if result and result.get('status') == 0 and result.get('result', {}).get('totalSize', 0) > 0:
                record = result['result']['records'][0]
                self._get_record_by_name_cache[cache_key] = record
                return record
            self._get_record_by_name_cache[cache_key] = None
            return None
        except Exception as e:
            print(f"Error retrieving {sobject_type} record by name '{name}'.")
            raise e

    def get_record_type_info_by_id(self, record_type_id: str) -> Optional[Dict[str, Any]]:
        """
        Gets the DeveloperName for a given RecordType ID, with in-memory caching.
        """
        if not hasattr(self, '_record_type_info_by_id_cache'):
            self._record_type_info_by_id_cache = {}
        if not record_type_id:
            return None
        if record_type_id in self._record_type_info_by_id_cache:
            return self._record_type_info_by_id_cache[record_type_id]
        query = f"SELECT DeveloperName FROM RecordType WHERE Id = '{record_type_id}'"
        try:
            result = self._execute_sf_command(['data', 'query', '--query', query])
            if result and result.get('status') == 0 and result.get('result', {}).get('totalSize', 0) > 0:
                record = result['result']['records'][0]
                self._record_type_info_by_id_cache[record_type_id] = record
                return record
            else:
                print(f"RecordType info not found for ID '{record_type_id}'.")
                self._record_type_info_by_id_cache[record_type_id] = None
                return None
        except Exception as e:
            print(f"Error retrieving RecordType info for ID '{record_type_id}'.")
            raise e

    def get_record_type_id(self, sobject_type: str, developer_name: str) -> Optional[str]:
        """
        Retrieves the RecordTypeId for a given sObject type and DeveloperName, with in-memory caching.
        """
        if not hasattr(self, '_record_type_id_cache'):
            self._record_type_id_cache = {}
        if not developer_name:
            print(f"DeveloperName is missing. Cannot query RecordType Id for {sobject_type}.")
            return None
        cache_key = (sobject_type, developer_name)
        if cache_key in self._record_type_id_cache:
            return self._record_type_id_cache[cache_key]
        query = f"SELECT Id FROM RecordType WHERE SobjectType='{sobject_type}' AND DeveloperName='{developer_name}'"
        try:
            result = self._execute_sf_command(['data', 'query', '--query', query])
            if result and result.get('status') == 0 and result.get('result', {}).get('totalSize', 0) > 0:
                record_id = result['result']['records'][0]['Id']
                self._record_type_id_cache[cache_key] = record_id
                return record_id
            else:
                print(f"RecordType Id not found for {sobject_type} with DeveloperName '{developer_name}'.")
                self._record_type_id_cache[cache_key] = None
                return None
        except Exception as e:
            print(f"Error retrieving RecordType Id for {sobject_type}/{developer_name}.")
            raise e

    def create_record(self, sobject_type: str, data: Dict[str, Any]) -> Optional[str]:
        """
        Creates a new Salesforce record and returns its ID.
        """
        value_pairs = []
        for key, value in data.items():
            # Skip empty strings or single hyphens, unless it's the Name field
            if isinstance(value, str) and (value == '' or value == '-'):
                if key != 'Name':  # Name field might be explicitly set to an empty string or hyphen
                    continue
            
            # Handle different value types for shell consumption
            if isinstance(value, bool):
                # Booleans must be lowercase true/false without quotes
                value_pairs.append(f"{key}={str(value).lower()}")
            elif isinstance(value, str):
                # Remove newlines, carriage returns, and single quotes
                clean_value = value.replace('\n', ' ').replace('\r', '').replace("'", "")
                value_pairs.append(f"{key}='{clean_value}'")
            elif isinstance(value, (int, float)):
                # Numbers without quotes
                value_pairs.append(f"{key}={value}")
            elif value is None:
                # Skip None values
                continue
            else:
                # Other types as-is with quotes
                value_pairs.append(f"{key}='{value}'")
        
        values_str = ' '.join(value_pairs)
        try:
            command_args = ['data', 'create', 'record', '--sobject', sobject_type, '--values', values_str]
            result = self._execute_sf_command(command_args)
            if result and result.get('status') == 0 and result.get('result', {}).get('id'):
                print(f"Successfully created {sobject_type} with ID: {result['result']['id']}")
                return result['result']['id']
            else:
                print(f"Failed to create {sobject_type}. SF command result: {result}")
                print(f"\n--- PROBLEMATIC VALUES STRING ---\n{values_str}\n---------------------------------\n")
                return None
        except Exception as e:
            print(f"Error creating {sobject_type} record: {e}")
            print(f"\n--- PROBLEMATIC VALUES STRING ---\n{values_str}\n---------------------------------\n")
            raise e

    def delete_record(self, sobject_type: str, record_id: str) -> bool:
        """
        Deletes a Salesforce record by its ID.
        """
        try:
            self._execute_sf_command(['data', 'delete', 'record', '--sobject', sobject_type, '--record-id', record_id])
            print(f"Successfully deleted {sobject_type} with ID: {record_id}")
            return True
        except Exception as e:
            print(f"Error deleting {sobject_type} record with ID {record_id}: {e}")
            return False

    def bulk_delete_records(self, sobject_type: str, excluded_ids: set = None) -> dict:
        """
        Bulk deletes all records of the given sObject type using sf force data bulk delete.
        
        Args:
            sobject_type: The Salesforce object type to delete records from
            excluded_ids: Set of record IDs to exclude from deletion
        """
        # Step 1: Query all record IDs
        query = f"SELECT Id FROM {sobject_type}"
        result = self._execute_sf_command(['data', 'query', '--query', query, '--json'])
        if not result or result.get('status') != 0:
            return {'success': False, 'message': f"Failed to query {sobject_type} records."}

        records = result['result']['records']
        
        # Filter out excluded IDs
        if excluded_ids:
            original_count = len(records)
            records = [rec for rec in records if rec['Id'] not in excluded_ids]
            excluded_count = original_count - len(records)
            if excluded_count > 0:
                print(f"  Excluding {excluded_count} protected record(s) from deletion")
        
        if not records:
            return {'success': True, 'message': f"No {sobject_type} records to delete."}

        # Step 2: Write IDs to a temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, newline='', suffix='.csv') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['Id'])
            writer.writeheader()
            for rec in records:
                writer.writerow({'Id': rec['Id']})
            temp_csv_path = csvfile.name

        # Step 3: Run the correct bulk delete command
        try:
            # Use classic SFDX-style command: sf force data bulk delete --sobject <type> --file <csv>
            # Add --wait 10 to ensure deletion completes before continuing
            command = [
                'force', 'data', 'bulk', 'delete',
                '--sobject', sobject_type,
                '--file', temp_csv_path,
                '--wait', '10'  # Wait up to 10 minutes for deletion to complete
            ]
            if self.target_org:
                command.extend(['--target-org', self.target_org])
            
            # Ensure logs directory exists and run command from there
            # This ensures bulk result CSV files are created in logs/
            logs_dir = Path(__file__).parent / 'logs'
            logs_dir.mkdir(exist_ok=True)
            
            # Run command and suppress verbose output (capture stderr only for errors)
            result = subprocess.run(
                ['sf'] + command, 
                text=True, 
                timeout=660,  # 11 minute timeout
                cwd=str(logs_dir),
                stdout=subprocess.DEVNULL,  # Suppress verbose batch status output
                stderr=subprocess.PIPE  # Capture errors only
            )
            os.remove(temp_csv_path)
            if result.returncode == 0:
                return {'success': True, 'message': f"Bulk delete for {sobject_type} completed."}
            else:
                return {'success': False, 'message': f"Bulk delete failed: {result.stderr}"}
        except Exception as e:
            if os.path.exists(temp_csv_path):
                os.remove(temp_csv_path)
            return {'success': False, 'message': str(e)}

    def query_records(self, query: str) -> Optional[List[Dict[str, Any]]]:
        """
        Executes a SOQL query and returns the list of records. Uses an in-memory cache.
        """
        if query in self._query_cache:
            log_query(query, self.target_org or 'default', cached=True)
            return self._query_cache[query]
        
        log_query(query, self.target_org or 'default', cached=False)

        try:
            result = self._execute_sf_command(['data', 'query', '--query', query])
            if result and result.get('status') == 0:
                records = result.get('result', {}).get('records', [])
                self._query_cache[query] = records
                return records
            else:
                error_msg = "Unknown error"
                if result:
                    # Try to extract detailed error message
                    if 'message' in result:
                        error_msg = result['message']
                    elif 'result' in result and isinstance(result['result'], list):
                        # Sometimes errors are in result array
                        error_msg = "; ".join([str(r) for r in result['result']])
                print(f"Query failed: {error_msg}")
                print(f"Full SF command result: {result}")
                self._query_cache[query] = [] # Cache empty result for failed queries too
                return []
        except Exception as e:
            print(f"Error executing query: {e}")
            import traceback
            traceback.print_exc()
            self._query_cache[query] = [] # Cache empty result for errored queries too
            raise e

    def bulk_delete_all_records(self, sobject_type: str, excluded_ids: set = None) -> bool:
        """
        Bulk deletes all records of a given sObject type using the Salesforce CLI bulk delete command.
        
        Args:
            sobject_type: The Salesforce object type to delete records from
            excluded_ids: Set of record IDs to exclude from deletion (e.g., portal users)
        """
        if excluded_ids:
            print(f"Starting bulk delete for {sobject_type} records (excluding {len(excluded_ids)} protected records)...")
        else:
            print(f"Starting bulk delete for all {sobject_type} records using Salesforce CLI bulk delete...")
        result = self.bulk_delete_records(sobject_type, excluded_ids)
        if result.get('success'):
            print(result.get('message', f"Bulk delete for {sobject_type} completed."))
            if 'stdout' in result:
                print(result['stdout'])
            return True
        else:
            print(result.get('message', f"Bulk delete for {sobject_type} failed."))
            if 'stdout' in result:
                print(result['stdout'])
            return False

    def bulk_upsert(self, sobject_type: str, csv_file_path: str, external_id: str = 'Id') -> Optional[Dict[str, Any]]:
        """
        Bulk upsert records from a CSV file using Salesforce CLI.
        
        Args:
            sobject_type: Salesforce object type (e.g., 'Account', 'Contact')
            csv_file_path: Path to CSV file with records to upsert
            external_id: Field to use for matching (default: 'Id' for updates)
        
        Returns:
            Dictionary with result information
        """
        try:
            # sf data upsert bulk --sobject Account --file data.csv --external-id Id --wait 10
            command_args = [
                'data', 'upsert', 'bulk',
                '--sobject', sobject_type,
                '--file', csv_file_path,
                '--external-id', external_id,
                '--wait', '10'  # Wait up to 10 minutes for job completion
            ]
            
            result = self._execute_sf_command(command_args)
            return result
        except Exception as e:
            import logging
            logging.error(f"Bulk upsert failed: {e}")
            return {'status': 1, 'message': str(e)}
