"""
CSV utilities for two-phase migration.
Saves production record data to CSV during Phase 1, reads it back during Phase 2.
"""

import csv
import json
#!/usr/bin/env python3
"""
CSV Export Utilities

Author: Ken Brill
Version: 1.1.8
Date: December 24, 2025
License: MIT License
"""

import os

def write_record_to_csv(object_type, prod_id, sandbox_id, record_data, script_dir):
    """
    Writes a record's production data to a CSV file for later lookup population.
    
    Args:
        object_type: Salesforce object type (e.g., 'Account', 'Contact')
        prod_id: Production org record ID
        sandbox_id: Sandbox org record ID
        record_data: Full record data from production (dict)
        script_dir: Script directory path
    """
    csv_dir = os.path.join(script_dir, 'migration_data')
    os.makedirs(csv_dir, exist_ok=True)
    
    csv_path = os.path.join(csv_dir, f'{object_type.lower()}_migration.csv')
    
    # Prepare row data
    row = {
        'production_id': prod_id,
        'sandbox_id': sandbox_id,
        'record_data': json.dumps(record_data)  # Store as JSON string
    }
    
    # Write or append to CSV
    file_exists = os.path.exists(csv_path)
    with open(csv_path, 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['production_id', 'sandbox_id', 'record_data']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        if not file_exists:
            writer.writeheader()
        
        writer.writerow(row)


def read_migration_csv(object_type, script_dir):
    """
    Reads all records from a migration CSV file.
    
    Args:
        object_type: Salesforce object type (e.g., 'Account', 'Contact')
        script_dir: Script directory path
        
    Returns:
        list: List of dicts with keys: production_id, sandbox_id, record_data
    """
    csv_path = os.path.join(script_dir, 'migration_data', f'{object_type.lower()}_migration.csv')
    
    if not os.path.exists(csv_path):
        return []
    
    records = []
    with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            records.append({
                'production_id': row['production_id'],
                'sandbox_id': row['sandbox_id'],
                'record_data': json.loads(row['record_data'])
            })
    
    return records


def clear_migration_csvs(script_dir):
    """
    Clears all migration CSV files to start fresh.
    
    Args:
        script_dir: Script directory path
    """
    csv_dir = os.path.join(script_dir, 'migration_data')
    if os.path.exists(csv_dir):
        for filename in os.listdir(csv_dir):
            if filename.endswith('_migration.csv'):
                filepath = os.path.join(csv_dir, filename)
                os.remove(filepath)
                print(f"  Cleared {filename}")
