"""
Phase 2: Update all records with actual lookup relationships.
Reads CSVs and populates lookups using created_* dictionaries.
OPTIMIZED: Uses Bulk API 2.0 for batch updates instead of individual API calls.
"""
from sandcastle_pkg.utils.csv_utils import read_migration_csv
#!/usr/bin/env python3
"""
Lookup Relationship Updates - Phase 2

Author: Ken Brill
Version: 1.0
Date: December 24, 2025
License: MIT License
"""

import logging
from rich.console import Console

console = Console()


def update_lookups_phase2(sf_cli_source, sf_cli_target, script_dir, insertable_fields_info, created_mappings, object_type, dummy_records):
    """
    Phase 2: Update all records of a given object type with actual lookup values.
    OPTIMIZED: Batches all updates into a single Bulk API 2.0 operation.
    
    Args:
        sf_cli_source: Source production CLI (for RecordType mapping)
        sf_cli_target: Target sandbox CLI
        script_dir: Script directory containing CSVs
        insertable_fields_info: Field metadata for the object
        created_mappings: Dictionary of all created_* dictionaries by object type
        object_type: Salesforce object type (e.g., 'Account', 'Contact')
        dummy_records: Dictionary of dummy record IDs to avoid setting lookups to dummy values
    """
    logging.info(f"\n[PHASE 2] Updating {object_type} lookups from CSV...")
    
    # Fields that cannot be updated after creation (read-only after insert)
    read_only_after_creation = {
        'QuoteLineItem': ['QuoteId', 'PricebookEntryId', 'Product2Id'],
        'OrderItem': ['OrderId', 'PricebookEntryId', 'Product2Id']
    }
    
    read_only_fields = read_only_after_creation.get(object_type, [])
    
    # Read migration CSV
    migrated_records = read_migration_csv(object_type, script_dir)
    if not migrated_records:
        logging.info(f"  No {object_type} records in CSV to update")
        return
    
    logging.info(f"  Found {len(migrated_records)} {object_type} records to update")
    
    # OPTIMIZED: Collect all updates first, then batch them
    bulk_updates = []
    skip_count = 0
    lookup_fields_found = set()
    recordtype_cache = {}  # Cache for RecordType ID mappings by DeveloperName
    
    for record_info in migrated_records:
        sandbox_id = record_info['sandbox_id']
        original_data = record_info['record_data']
        
        # Build update payload with actual lookup IDs
        update_payload = {'Id': sandbox_id}  # Bulk API needs Id in the payload
        
        # Special handling: Restore RecordTypeId for objects that used bypass in Phase 1
        if object_type == 'Opportunity' and 'RecordTypeId' in original_data:
            original_record_type_id = original_data['RecordTypeId']
            if original_record_type_id and not isinstance(original_record_type_id, dict):
                update_payload['RecordTypeId'] = original_record_type_id
        
        for field_name, field_info in insertable_fields_info.items():
            if field_info['type'] != 'reference':
                continue
            
            # Skip read-only fields that cannot be updated after creation
            if field_name in read_only_fields:
                continue
            
            # Skip all User lookups (users exist in sandbox, set correctly in Phase 1)
            referenced_object = field_info['referenceTo']
            if referenced_object == 'User':
                continue
            
            # Get the original production lookup ID
            prod_lookup_id = original_data.get(field_name)
            if not prod_lookup_id or isinstance(prod_lookup_id, dict):
                continue
            
            referenced_object = field_info['referenceTo']
            if not referenced_object:
                continue
            
            # Special handling for RecordType: Map by DeveloperName, not by created records
            if referenced_object == 'RecordType' and prod_lookup_id:
                # Get RecordType info from production to find DeveloperName
                if prod_lookup_id not in recordtype_cache:
                    try:
                        # Query production RecordType to get DeveloperName
                        rt_info = sf_cli_source.get_record_type_info_by_id(prod_lookup_id)
                        if rt_info and 'DeveloperName' in rt_info:
                            dev_name = rt_info['DeveloperName']
                            # Find matching RecordType in sandbox by DeveloperName
                            sandbox_rt_id = sf_cli_target.get_record_type_id(object_type, dev_name)
                            if sandbox_rt_id:
                                recordtype_cache[prod_lookup_id] = sandbox_rt_id
                                logging.info(f"    Mapped RecordType: {dev_name} ({prod_lookup_id} → {sandbox_rt_id})")
                            else:
                                recordtype_cache[prod_lookup_id] = None
                                logging.warning(f"    RecordType '{dev_name}' not found in sandbox")
                        else:
                            recordtype_cache[prod_lookup_id] = None
                    except Exception as e:
                        logging.warning(f"    Could not map RecordType {prod_lookup_id}: {e}")
                        recordtype_cache[prod_lookup_id] = None
                
                # Use cached mapping
                if recordtype_cache.get(prod_lookup_id):
                    update_payload[field_name] = recordtype_cache[prod_lookup_id]
                    lookup_fields_found.add(field_name)
                continue
            
            # Check if we have the referenced object in our created mappings
            if referenced_object in created_mappings:
                created_dict = created_mappings[referenced_object]
                
                # Map production ID to sandbox ID
                if prod_lookup_id in created_dict:
                    sandbox_lookup_id = created_dict[prod_lookup_id]
                    
                    # Don't set lookup to dummy record
                    if referenced_object in dummy_records and sandbox_lookup_id == dummy_records[referenced_object]:
                        continue
                    
                    update_payload[field_name] = sandbox_lookup_id
                    lookup_fields_found.add(field_name)
        
        # Add to bulk updates if we have any lookups to set
        if len(update_payload) > 1:  # More than just Id
            bulk_updates.append(update_payload)
        else:
            skip_count += 1
    
    # OPTIMIZED: Execute bulk update instead of individual updates
    update_count = 0
    error_count = 0
    
    if bulk_updates:
        logging.info(f"  Updating {len(lookup_fields_found)} lookup field(s): {', '.join(sorted(lookup_fields_found))}")
        logging.info(f"  Performing bulk update of {len(bulk_updates)} record(s)...")
        
        try:
            from sandcastle_pkg.utils.bulk_utils import bulk_update_records
            result = bulk_update_records(sf_cli_target, object_type, bulk_updates)
            
            if result and result.get('success'):
                update_count = len(bulk_updates)
                logging.info(f"  ✓ Successfully updated {update_count} {object_type} record(s) via Bulk API")
            else:
                # Bulk failed, fall back to individual updates
                logging.warning(f"  Bulk update failed, falling back to individual updates...")
                for update_data in bulk_updates:
                    sandbox_id = update_data['Id']
                    # Create copy without Id for the update call
                    update_fields = {k: v for k, v in update_data.items() if k != 'Id'}
                    try:
                        sf_cli_target.update_record(object_type, sandbox_id, update_fields)
                        update_count += 1
                    except Exception as e:
                        logging.warning(f"  ✗ Error updating {sandbox_id}: {e}")
                        error_count += 1

                        # Try individual field updates if batch fails
                        if len(update_fields) > 1:
                            for field_name, field_value in update_fields.items():
                                try:
                                    sf_cli_target.update_record(object_type, sandbox_id, {field_name: field_value})
                                except Exception as field_error:
                                    pass
        except ImportError:
            # bulk_utils not available, fall back to individual updates
            logging.warning(f"  Bulk API not available, using individual updates...")
            for update_data in bulk_updates:
                sandbox_id = update_data['Id']
                # Create copy without Id for the update call
                update_fields = {k: v for k, v in update_data.items() if k != 'Id'}
                try:
                    sf_cli_target.update_record(object_type, sandbox_id, update_fields)
                    update_count += 1
                except Exception as e:
                    logging.warning(f"  ✗ Error updating {sandbox_id}: {e}")
                    error_count += 1
    
    # Display summary with green success message
    console.print(f"  [green]✓ {object_type} Phase 2 Summary: {update_count} updated, {skip_count} skipped, {error_count} errors[/green]")
    logging.info(f"  {object_type} Phase 2 Summary: {update_count} updated, {skip_count} skipped, {error_count} errors")
