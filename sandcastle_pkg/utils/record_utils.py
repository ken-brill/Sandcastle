#!/usr/bin/env python3
"""
Record Utilities

Author: Ken Brill
Version: 1.1.8
Date: December 24, 2025
License: MIT License
"""

import os
import csv
from rich.console import Console
from sandcastle_pkg.utils.picklist_utils import get_valid_picklist_values

console = Console()

# Global cache for record existence checks to avoid repeated queries
_record_existence_cache = {}

# Cache for fallback user ID (queried once per target org)
_fallback_user_cache = {}

def get_fallback_user_id(sf_cli_target):
    """
    Get a fallback user ID for OwnerId when the original user doesn't exist in sandbox.
    Queries for the first active standard user and caches the result.

    Args:
        sf_cli_target: Target Salesforce CLI instance

    Returns:
        str: User ID or None if no users found
    """
    cache_key = sf_cli_target.target_org

    if cache_key in _fallback_user_cache:
        return _fallback_user_cache[cache_key]

    try:
        # Query for an active standard user
        query = "SELECT Id FROM User WHERE IsActive = true AND UserType = 'Standard' LIMIT 1"
        result = sf_cli_target.query_records(query)
        if result and len(result) > 0:
            user_id = result[0]['Id']
            _fallback_user_cache[cache_key] = user_id
            return user_id
    except Exception as e:
        console.print(f"[yellow]Warning: Could not query fallback user: {e}[/yellow]")

    _fallback_user_cache[cache_key] = None
    return None


def check_record_exists(sf_cli, object_type, record_id):
    """
    Check if a record exists in the target org, with caching to avoid repeated queries.

    Args:
        sf_cli: Salesforce CLI instance
        object_type: Salesforce object type (e.g., 'User', 'Account', 'Contact')
        record_id: Record ID to check

    Returns:
        bool: True if record exists, False otherwise
    """
    cache_key = f"{object_type}:{record_id}"
    
    # Check cache first
    if cache_key in _record_existence_cache:
        return _record_existence_cache[cache_key]
    
    # Query the org
    try:
        query = f"SELECT Id FROM {object_type} WHERE Id = '{record_id}' LIMIT 1"
        result = sf_cli.query_records(query)
        exists = result and len(result) > 0
        
        # Cache the result
        _record_existence_cache[cache_key] = exists
        return exists
    except Exception as e:
        print(f"Error checking {object_type} {record_id}: {e}")
        # Cache negative result to avoid repeated failures
        _record_existence_cache[cache_key] = False
        return False

def replace_lookups_with_dummies(record, insertable_fields_info, dummy_records, created_mappings=None, sf_cli_source=None, sf_cli_target=None, sobject_type=None):
    """
    Replaces lookup fields with appropriate values:
    - Use real sandbox IDs if the referenced record was already created
    - Use dummy IDs for required lookups if referenced record doesn't exist yet
    - Remove optional lookups to avoid validation errors (Phase 2 will restore)
    - Map RecordTypeId by DeveloperName (except for Opportunities which use bypass in Phase 1)
    
    Args:
        record: The Salesforce record to modify
        insertable_fields_info: Field metadata dictionary
        dummy_records: Dictionary mapping object types to dummy record IDs
        created_mappings: Optional dict of created_* mappings by object type
        sf_cli_source: Source org CLI (for RecordType mapping)
        sf_cli_target: Target org CLI (for RecordType mapping)
        sobject_type: Object type (e.g., 'Account', 'Opportunity') - used to exclude Opportunity from RecordType mapping
        
    Returns:
        dict: Record with lookups properly set
    """
    modified_record = record.copy()
    created_mappings = created_mappings or {}
    
    for field_name, field_info in insertable_fields_info.items():
        if field_info['type'] == 'reference' and field_info['referenceTo']:
            referenced_object = field_info['referenceTo']
            
            # If the field exists in the record and has a value, replace it
            if field_name in modified_record and modified_record[field_name]:
                prod_lookup_id = modified_record[field_name]
                
                # Skip if it's a dict (relationship field)
                if isinstance(prod_lookup_id, dict):
                    continue
                
                # List of required lookup fields that MUST have a value in Phase 1
                required_lookups = ['AccountId', 'OpportunityId', 'QuoteId', 'OrderId', 
                                  'AccountFromId', 'AccountToId',  # Required for AccountRelationship
                                  'OwnerId']  # Required on most objects
                
                # Special handling for RecordType: Map by DeveloperName (except Opportunities)
                # Opportunities use a bypass RecordTypeId in Phase 1 to avoid triggering flows
                if referenced_object == 'RecordType' and field_name == 'RecordTypeId':
                    # Skip RecordType mapping for Opportunities - they use bypass in Phase 1
                    if sobject_type == 'Opportunity':
                        print(f"  [SKIP] RecordTypeId for Opportunity - will use bypass value, restore in Phase 2")
                        del modified_record[field_name]
                    # For all other objects, map RecordType by DeveloperName
                    elif sf_cli_source and sf_cli_target and sobject_type:
                        try:
                            rt_info = sf_cli_source.get_record_type_info_by_id(prod_lookup_id)
                            if rt_info and 'DeveloperName' in rt_info:
                                dev_name = rt_info['DeveloperName']
                                sandbox_rt_id = sf_cli_target.get_record_type_id(sobject_type, dev_name)
                                if sandbox_rt_id:
                                    modified_record[field_name] = sandbox_rt_id
                                    console.print(f"  [cyan][MAP] RecordType {dev_name}: {prod_lookup_id} → {sandbox_rt_id}[/cyan]")
                                else:
                                    print(f"  [WARN] RecordType '{dev_name}' not found in sandbox, removing field")
                                    del modified_record[field_name]
                            else:
                                print(f"  [WARN] Could not get RecordType info for {prod_lookup_id}, removing field")
                                del modified_record[field_name]
                        except Exception as e:
                            print(f"  [ERROR] RecordType mapping failed: {e}, removing field")
                            del modified_record[field_name]
                    else:
                        # No CLI provided, remove RecordType (will use default)
                        console.print(f"  [yellow][REMOVE] RecordTypeId (no CLI provided), will use default RecordType[/yellow]")
                        del modified_record[field_name]
                    continue
                
                # Special handling for User lookups
                # Keep all User lookups from production (users exist in sandbox with same IDs)
                if referenced_object == 'User':
                    console.print(f"  [green][KEEP] Keeping User lookup {field_name} = {prod_lookup_id} from production (users exist in sandbox)[/green]")
                    # Keep the production User lookup as-is
                # Special handling for OwnerId - keep production value if no mapping available
                # OwnerId can reference User, Group, or other objects - keep as-is from production
                elif field_name == 'OwnerId':
                    console.print(f"  [green][KEEP] Keeping {field_name} = {prod_lookup_id} from production (Owner lookups typically exist in sandbox)[/green]")
                    # Keep the production OwnerId as-is
                # For REQUIRED lookups only, try to use mapping or dummy
                elif field_name in required_lookups:
                    if referenced_object in created_mappings:
                        created_dict = created_mappings[referenced_object]
                        if prod_lookup_id in created_dict:
                            sandbox_lookup_id = created_dict[prod_lookup_id]
                            modified_record[field_name] = sandbox_lookup_id
                            console.print(f"  [cyan][MAP] Using real {field_name}: {prod_lookup_id} → {sandbox_lookup_id}[/cyan]")
                        elif referenced_object in dummy_records:
                            # Required field but record not created yet - use dummy
                            modified_record[field_name] = dummy_records[referenced_object]
                            print(f"  [DUMMY] Replaced {field_name} ({prod_lookup_id}) with dummy {referenced_object}")
                        else:
                            print(f"  [ERROR] Required {field_name} has no mapping or dummy available")
                    elif referenced_object in dummy_records:
                        # Required field without mapping - use dummy
                        modified_record[field_name] = dummy_records[referenced_object]
                        print(f"  [DUMMY] Replaced {field_name} ({prod_lookup_id}) with dummy {referenced_object}")
                    else:
                        print(f"  [ERROR] Required {field_name} has no dummy available")
                # For ALL optional lookups, remove them to avoid lookup filter issues
                # Phase 2 will restore them with real production values
                else:
                    console.print(f"  [yellow][REMOVE] Removing optional lookup {field_name}, will restore in Phase 2[/yellow]")
                    del modified_record[field_name]
            # If the field doesn't exist but is required, add dummy
            elif field_name not in modified_record and referenced_object in dummy_records:
                # Common required lookups
                if field_name in ['AccountId', 'OpportunityId', 'QuoteId', 'OrderId']:
                    modified_record[field_name] = dummy_records[referenced_object]
                    print(f"  [DUMMY] Added required {field_name} with dummy {referenced_object}")
    
    return modified_record

def load_insertable_fields(object_name, script_dir):
    """
    Loads insertable field names, their types, and reference information
    from the generated CSV file.
    Returns a dictionary of {field_name: {'type': field_type, 'referenceTo': reference_object}}.
    """
    field_data_path = os.path.join(script_dir, 'fieldData', f'{object_name.lower()}Fields.csv')
    insertable_fields_info = {}
    if os.path.exists(field_data_path):
        with open(field_data_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                field_name = row['Field Name']
                field_type = row['Field Type']
                reference_to = row.get('Reference To', '')
                insertable_fields_info[field_name] = {
                    'type': field_type,
                    'referenceTo': reference_to
                }
    else:
        print(f"Warning: Field data CSV not found for {object_name} at {field_data_path}.")
    return insertable_fields_info
def filter_record_data(record, insertable_fields_info, sf_cli_target, sobject_type=None):
    """
    Filters a Salesforce record to include only insertable fields and handles special cases.
    For lookup fields, it checks if the referenced record exists in the target sandbox.
    
    Args:
        record: The Salesforce record to filter
        insertable_fields_info: Field metadata dictionary
        sf_cli_target: Target Salesforce CLI instance
        sobject_type: The Salesforce object type (e.g., 'Account', 'Contact'). If not provided, 
                      will try to extract from record attributes.
    """
    # Determine the sobject type
    if not sobject_type:
        sobject_type = record.get('attributes', {}).get('type') or record.get('sobjectType')
    
    # Fields that should never be copied (process/workflow driven fields)
    excluded_fields = {
        'Accept_as_Affiliate__c',  # Requires executive contact - process driven
        'Force_NetSuite_Sync__c',  # Never sync NetSuite integration field to sandbox
    }
    
    # User lookup fields that should be preserved - only OwnerId can be set
    # CreatedById and LastModifiedById are system-managed and cannot be set
    user_lookup_fields = {'OwnerId'}

    # Get fallback user ID dynamically (cached after first query)
    fallback_user_id = get_fallback_user_id(sf_cli_target)

    filtered_data = {}
    for field_name, value in record.items():
        # Preserve OwnerId only if the user exists in sandbox, otherwise use fallback
        if field_name in user_lookup_fields and value and not isinstance(value, dict):
            # Check if the user exists in the sandbox
            if check_record_exists(sf_cli_target, 'User', value):
                filtered_data[field_name] = value
                console.print(f"  [green][PRESERVE] {field_name} = {value} (User exists in sandbox)[/green]")
            elif fallback_user_id:
                filtered_data[field_name] = fallback_user_id
                console.print(f"  [yellow][FALLBACK] {field_name} = {fallback_user_id} (Original user {value} not found in sandbox)[/yellow]")
            else:
                # No fallback available, skip the field and let Salesforce use default
                console.print(f"  [yellow][SKIP] {field_name} - Original user {value} not found and no fallback available[/yellow]")
            continue
        
        # Exclude system fields, relationship fields, process fields, and fields not in our insertable list
        if (field_name == 'attributes' or 
            field_name.endswith('__r') or 
            field_name in excluded_fields or
            field_name not in insertable_fields_info):
            continue
        field_type_info = insertable_fields_info.get(field_name)
        if not field_type_info:
            continue
        field_type = field_type_info['type']

        # Handle lookup fields
        if field_type == 'reference':
            referenced_object = field_type_info['referenceTo']
            if referenced_object and value:
                query =\
                    f"SELECT Id FROM {referenced_object} WHERE Id = '{value}' LIMIT 1"
                existing_referenced_record = sf_cli_target.query_records(query)
                if existing_referenced_record and len(existing_referenced_record) > 0:
                    filtered_data[field_name] = value
                # Skip lookup if referenced record doesn't exist in target sandbox
            continue
        if isinstance(value, dict) and 'Id' in value:
            if field_name == 'RecordTypeId':
                filtered_data[field_name] = value['Id']
            elif field_name == 'OwnerId':
                filtered_data[field_name] = value['Id']
            else:
                filtered_data[field_name] = value['Id']
        elif value is not None:
            # If this is an email field, append '.invalid' to the value
            if 'email' in field_name.lower() and isinstance(value, str) and not value.endswith('.invalid'):
                filtered_data[field_name] = value + '.invalid'
            # Handle picklist fields: check if value is valid, else set to 'Other' or remove
            elif field_type == 'picklist' and isinstance(value, str):
                try:
                    # Try to get valid picklist values for this field
                    valid_values = get_valid_picklist_values(sf_cli_target, sobject_type, field_name) if sobject_type else set()
                    if valid_values and value not in valid_values:
                        # Special handling for required picklist fields
                        if field_name == 'StageName':
                            # StageName is required - use first valid value as default
                            default_stage = next(iter(valid_values)) if valid_values else 'Prospecting'
                            print(f"[PICKLIST REPLACEMENT] Field '{field_name}': '{value}' is not valid. Using default '{default_stage}'.")
                            filtered_data[field_name] = default_stage
                        # Prefer 'Other' if available, else remove field (for non-required fields)
                        elif 'Other' in valid_values:
                            print(f"[PICKLIST REPLACEMENT] Field '{field_name}': '{value}' is not valid. Replacing with 'Other'.")
                            filtered_data[field_name] = 'Other'
                        else:
                            print(f"[PICKLIST REMOVAL] Field '{field_name}': '{value}' is not valid and no 'Other' value available. Removing field from record.")
                            continue
                    elif valid_values:
                        # Value is valid
                        filtered_data[field_name] = value
                    else:
                        # Could not get valid values, remove field to be safe (unless it's StageName)
                        if field_name == 'StageName':
                            # For StageName, use the current value if we can't validate
                            print(f"[PICKLIST PASSTHROUGH] Field '{field_name}': Could not retrieve valid values. Keeping original value '{value}'.")
                            filtered_data[field_name] = value
                        else:
                            # For all other picklists, remove if we can't validate
                            print(f"[PICKLIST REMOVAL] Field '{field_name}': Could not retrieve valid picklist values. Removing field to prevent errors.")
                            continue
                except Exception as e:
                    print(f"[PICKLIST ERROR] Field '{field_name}': Error retrieving picklist values: {str(e)}. Removing field.")
                    continue
            # Handle multi-select picklist fields (semicolon-separated values)
            elif field_type == 'multipicklist' and isinstance(value, str):
                try:
                    valid_values = get_valid_picklist_values(sf_cli_target, sobject_type, field_name) if sobject_type else set()
                    if valid_values:
                        # Split by semicolon, filter valid values
                        selected_values = [v.strip() for v in value.split(';')]
                        valid_selected = [v for v in selected_values if v in valid_values]
                        
                        if valid_selected:
                            result_value = ';'.join(valid_selected)
                            
                            # Check if the result exceeds typical multipicklist field limit (255 chars)
                            if len(result_value) > 255:
                                # Truncate by removing values from the end until it fits
                                truncated_values = []
                                current_length = 0
                                for v in valid_selected:
                                    # Account for semicolon separator
                                    needed_length = len(v) + (1 if truncated_values else 0)
                                    if current_length + needed_length <= 255:
                                        truncated_values.append(v)
                                        current_length += needed_length
                                    else:
                                        break
                                result_value = ';'.join(truncated_values)
                                print(f"[MULTIPICKLIST TRUNCATE] Field '{field_name}': Value too long ({len(';'.join(valid_selected))} chars). Truncated to {len(result_value)} chars. Kept {len(truncated_values)}/{len(valid_selected)} values.")
                            
                            filtered_data[field_name] = result_value
                            invalid_values = [v for v in selected_values if v not in valid_values]
                            if invalid_values:
                                print(f"[MULTIPICKLIST FILTER] Field '{field_name}': Removed invalid values {invalid_values}. Kept: {len(valid_selected)} valid values.")
                        else:
                            print(f"[MULTIPICKLIST REMOVAL] Field '{field_name}': No valid values found in '{value}'. Removing field from record.")
                            continue
                    else:
                        # If we can't get valid values, remove the field to be safe
                        print(f"[MULTIPICKLIST REMOVAL] Field '{field_name}': Could not retrieve valid picklist values. Removing field to prevent errors.")
                        continue
                except Exception as e:
                    print(f"[MULTIPICKLIST ERROR] Field '{field_name}': Error retrieving picklist values: {str(e)}. Removing field.")
                    continue
            # Handle boolean fields - convert string 'True'/'False' to actual booleans
            elif field_type == 'boolean' and isinstance(value, str):
                if value == 'True':
                    filtered_data[field_name] = True
                elif value == 'False':
                    filtered_data[field_name] = False
                else:
                    # Invalid boolean string, skip field
                    print(f"[BOOLEAN ERROR] Field '{field_name}': Invalid boolean string '{value}'. Removing field.")
                    continue
            else:
                filtered_data[field_name] = value
    return filtered_data