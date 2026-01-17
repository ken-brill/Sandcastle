#!/usr/bin/env python3
"""
Bulk API Utilities

Author: Ken Brill
Version: 1.1.8
Date: December 24, 2025
License: MIT License

Bulk API utilities for efficient record creation.
Instead of creating records one-by-one, batch them and use CSV bulk import.
"""

import csv
import json
import logging
import subprocess
from pathlib import Path
from typing import Dict, List, Any
from rich.console import Console

logger = logging.getLogger(__name__)
console = Console()

class BulkRecordCreator:
    """
    Batches record creation operations and executes them in bulk via CSV import.
    This is 2-5x faster than individual record creation.
    """
    
    def __init__(self, sf_cli_target, batch_size: int = 200):
        """
        Initialize bulk creator.
        
        Args:
            sf_cli_target: Salesforce CLI wrapper for target org
            batch_size: Number of records to batch before auto-flush (default: 200)
        """
        self.sf_cli_target = sf_cli_target
        self.batch_size = batch_size
        self.batches: Dict[str, List[Dict[str, Any]]] = {}
        self.temp_dir = Path(__file__).parent / 'tmp_bulk'
        self.temp_dir.mkdir(exist_ok=True)
    
    def add_record(self, sobject: str, record_data: Dict[str, Any]) -> None:
        """
        Add a record to the batch queue.
        Auto-flushes when batch_size is reached.
        
        Args:
            sobject: Salesforce object type (e.g., 'Account', 'Contact')
            record_data: Dictionary of field values
        """
        if sobject not in self.batches:
            self.batches[sobject] = []
        
        self.batches[sobject].append(record_data)
        
        # Auto-flush if batch is full
        if len(self.batches[sobject]) >= self.batch_size:
            self.flush(sobject)
    
    def flush(self, sobject: str = None) -> Dict[str, List[str]]:
        """
        Flush batched records to Salesforce via bulk CSV import.
        
        Args:
            sobject: Specific object type to flush (None = flush all)
        
        Returns:
            Dict mapping sobject to list of created IDs
        """
        if sobject:
            objects_to_flush = [sobject]
        else:
            objects_to_flush = list(self.batches.keys())
        
        results = {}
        
        for obj in objects_to_flush:
            if obj not in self.batches or not self.batches[obj]:
                continue
            
            records = self.batches[obj]
            logger.info(f"Bulk creating {len(records)} {obj} record(s)")
            
            try:
                created_ids = self._bulk_create(obj, records)
                results[obj] = created_ids
                
                # Clear batch after successful creation
                self.batches[obj] = []
            except Exception as e:
                logger.error(f"Bulk creation failed for {obj}: {e}")
                # Don't clear batch on failure - allow retry
                raise
        
        return results
    
    def _bulk_create(self, sobject: str, records: List[Dict[str, Any]]) -> List[str]:
        """
        Create records using Bulk API 2.0 via CSV import.
        
        Args:
            sobject: Salesforce object type
            records: List of record dictionaries
        
        Returns:
            List of created record IDs
        """
        # Write records to CSV
        csv_file = self.temp_dir / f'bulk_{sobject}.csv'
        
        # Get all unique field names
        all_fields = set()
        for record in records:
            all_fields.update(record.keys())
        
        field_list = sorted(all_fields)
        
        # Sanitize records: remove embedded newlines from field values
        sanitized_records = []
        for record in records:
            sanitized = {}
            for key, value in record.items():
                if isinstance(value, str):
                    # Replace any newline characters with spaces
                    sanitized[key] = value.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ')
                else:
                    sanitized[key] = value
            sanitized_records.append(sanitized)
        
        # Write CSV initially with default line endings
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=field_list)
            writer.writeheader()
            writer.writerows(sanitized_records)
        
        # Read the file and convert line endings to CRLF (Salesforce Bulk API requirement)
        with open(csv_file, 'rb') as f:
            content = f.read()
        
        # Replace LF with CRLF, but avoid double CRLF
        content = content.replace(b'\r\n', b'\n').replace(b'\n', b'\r\n')
        
        with open(csv_file, 'wb') as f:
            f.write(content)
        
        # Execute bulk import via SF CLI using Bulk API 2.0
        # Note: Use 'sf data import bulk' for Bulk API 2.0
        # Try with --line-ending parameter if supported
        command = [
            'sf', 'data', 'import', 'bulk',
            '--sobject', sobject,
            '--file', str(csv_file),
            '--line-ending', 'CRLF',  # Explicitly specify CRLF line endings
            '--target-org', self.sf_cli_target.target_org,
            '--wait', '10',  # Wait up to 10 minutes
            '--json'
        ]
        
        # Ensure logs directory exists and run command from there
        # This ensures bulk result CSV files are created in logs/
        logs_dir = Path(__file__).parent / 'logs'
        logs_dir.mkdir(exist_ok=True)
        
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=600,  # 10 minute timeout
                check=False,
                cwd=str(logs_dir)  # Run from logs directory
            )
        except subprocess.TimeoutExpired as e:
            raise Exception(f"Bulk create timed out after 10 minutes") from e
        
        if result.returncode != 0:
            error_msg = result.stderr or result.stdout
            
            # Try to pretty-print JSON error if possible
            try:
                error_json = json.loads(error_msg)
                console.print("[yellow]⚠ Bulk create failed:[/yellow]")
                console.print_json(data=error_json)
            except (json.JSONDecodeError, TypeError):
                logger.warning(f"Bulk create failed: {error_msg}")
            
            # Try to extract partial results from failed job
            # The error might include a job ID we can query
            try:
                response = json.loads(result.stdout) if result.stdout else {}
                job_id = response.get('data', {}).get('jobId')
                
                if job_id:
                    logger.info(f"Attempting to retrieve partial results from job {job_id}")
                    # Query the job results to get successful IDs
                    results_cmd = [
                        'sf', 'data', 'bulk', 'results',
                        '--job-id', job_id,
                        '--target-org', self.sf_cli_target.target_org,
                        '--json'
                    ]
                    results_run = subprocess.run(
                        results_cmd,
                        capture_output=True,
                        text=True,
                        timeout=60,
                        check=False
                    )
                    
                    logger.info(f"Bulk results command returncode: {results_run.returncode}")
                    if results_run.stdout:
                        try:
                            # Try to parse and pretty-print JSON
                            results_json = json.loads(results_run.stdout)
                            console.print("[dim]Bulk results:[/dim]")
                            console.print_json(data=results_json)
                        except json.JSONDecodeError:
                            # Fallback to truncated string if not JSON
                            logger.info(f"Bulk results stdout: {results_run.stdout[:500]}")
                    if results_run.stderr:
                        logger.info(f"Bulk results stderr: {results_run.stderr[:500]}")
                    
                    if results_run.returncode == 0:
                        results_response = json.loads(results_run.stdout)
                        logger.info(f"Bulk results response status: {results_response.get('status')}")
                        logger.info(f"Bulk results keys: {list(results_response.keys())}")
                        
                        if results_response.get('status') == 0:
                            result_data = results_response.get('result', {})
                            logger.info(f"Result data keys: {list(result_data.keys()) if isinstance(result_data, dict) else 'not a dict'}")
                            
                            # Check if there are successful records
                            successful_count = result_data.get('successfulRecords', 0)
                            success_file = result_data.get('successFilePath')
                            
                            if successful_count > 0 and success_file:
                                logger.info(f"Reading {successful_count} successful ID(s) from {success_file}")
                                # Read the success CSV file to get IDs
                                successful_ids = []
                                try:
                                    with open(success_file, 'r', encoding='utf-8') as f:
                                        reader = csv.DictReader(f)
                                        for row in reader:
                                            # The success file has an 'sf__Id' column with the created record ID
                                            # (Salesforce CLI bulk API v2.0 format)
                                            if 'sf__Id' in row and row['sf__Id']:
                                                successful_ids.append(row['sf__Id'])
                                            elif 'Id' in row and row['Id']:
                                                # Fallback for older format
                                                successful_ids.append(row['Id'])
                                    
                                    if successful_ids:
                                        logger.info(f"Retrieved {len(successful_ids)} successful IDs from {success_file}")
                                        return successful_ids
                                    else:
                                        logger.warning(f"Success file {success_file} was empty or had no IDs")
                                except Exception as e:
                                    logger.warning(f"Failed to read success file {success_file}: {e}")
                            else:
                                logger.warning(f"No successful records in bulk results (successful: {successful_count})")
                        else:
                            logger.warning(f"Bulk results response had non-zero status: {results_response.get('status')}")
                    else:
                        logger.warning(f"Bulk results command failed with returncode {results_run.returncode}")
            except Exception as e:
                logger.warning(f"Could not retrieve partial results: {e}")
            
            return None  # Return None to signal complete failure
        
        # Parse response
        try:
            response = json.loads(result.stdout)
        except json.JSONDecodeError:
            # Fallback to returning empty list if can't parse
            logger.warning(f"Could not parse bulk create response")
            return []
        
        # Extract created IDs from response
        created_ids = []
        if response.get('status') == 0:
            result_data = response.get('result', {})
            job_info = result_data.get('jobInfo', {})
            
            # Different response formats depending on CLI version
            if 'numberRecordsProcessed' in job_info:
                num_created = job_info.get('numberRecordsProcessed', 0)
                logger.info(f"✓ Bulk created {num_created} {sobject} record(s)")
            
            # Try to get IDs from successful results
            if 'successfulResults' in result_data:
                for item in result_data['successfulResults']:
                    if 'id' in item:
                        created_ids.append(item['id'])
        
        return created_ids
    
    def flush_all(self) -> Dict[str, List[str]]:
        """
        Flush all pending batches to Salesforce.
        
        Returns:
            Dict mapping sobject to list of created IDs
        """
        return self.flush(sobject=None)
    
    def get_pending_count(self, sobject: str = None) -> int:
        """
        Get count of pending records in batches.
        
        Args:
            sobject: Specific object type (None = all objects)
        
        Returns:
            Count of pending records
        """
        if sobject:
            return len(self.batches.get(sobject, []))
        else:
            return sum(len(records) for records in self.batches.values())


def bulk_update_records(sf_cli_target, sobject: str, records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Update records in bulk using Bulk API 2.0.
    Each record must have 'Id' field.
    
    Args:
        sf_cli_target: Salesforce CLI wrapper for target org
        sobject: Salesforce object type (e.g., 'Account', 'Contact')
        records: List of dictionaries with 'Id' and fields to update
    
    Returns:
        Dictionary with 'success' boolean and optional 'message'
    """
    if not records:
        return {'success': True, 'message': 'No records to update'}
    
    logger.info(f"Starting bulk update of {len(records)} {sobject} record(s)...")
    
    # Create temp directory for CSV
    temp_dir = Path(__file__).parent / 'tmp_bulk'
    temp_dir.mkdir(exist_ok=True)
    
    # Write records to CSV
    csv_file = temp_dir / f'update_{sobject}_{len(records)}.csv'
    
    try:
        # Get all field names from records
        all_fields = set()
        for record in records:
            all_fields.update(record.keys())
        
        # Id must be first column
        fieldnames = ['Id'] + sorted([f for f in all_fields if f != 'Id'])
        
        # Sanitize records: remove embedded newlines from field values
        sanitized_records = []
        for record in records:
            sanitized = {}
            for key, value in record.items():
                if isinstance(value, str):
                    # Replace any newline characters with spaces
                    sanitized[key] = value.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ')
                else:
                    sanitized[key] = value
            sanitized_records.append(sanitized)
        
        # Write CSV with LF line endings initially
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(sanitized_records)
        
        # Convert line endings to CRLF (Salesforce Bulk API requirement)
        with open(csv_file, 'rb') as f:
            content = f.read()
        
        # Replace LF with CRLF, but avoid double CRLF
        content = content.replace(b'\r\n', b'\n').replace(b'\n', b'\r\n')
        
        with open(csv_file, 'wb') as f:
            f.write(content)
        
        # Execute bulk update via Salesforce CLI
        result = sf_cli_target.bulk_upsert(sobject, str(csv_file), external_id='Id')
        
        # Clean up temp file
        csv_file.unlink()
        
        if result and result.get('status') == 0:
            logger.info(f"✓ Successfully bulk updated {len(records)} {sobject} record(s)")
            return {'success': True, 'records_updated': len(records)}
        else:
            error_msg = result.get('message', 'Unknown error') if result else 'No response from CLI'
            logger.error(f"✗ Bulk update failed: {error_msg}")
            return {'success': False, 'message': error_msg}
    
    except Exception as e:
        logger.error(f"✗ Bulk update error: {e}")
        # Clean up temp file on error
        if csv_file.exists():
            csv_file.unlink()
        return {'success': False, 'message': str(e)}
