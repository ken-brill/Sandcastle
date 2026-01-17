#!/usr/bin/env python3
"""
Account Relationship Creation - Phase 1

Author: Ken Brill
Version: 1.0
Date: December 24, 2025
License: MIT License

Phase 1: Creates AccountRelationship with dummy lookups or real Account IDs if available.
AccountRelationship connects two Accounts with a relationship type.
"""
import re
from sandcastle_pkg.utils.record_utils import filter_record_data, replace_lookups_with_dummies, load_insertable_fields
from sandcastle_pkg.utils.csv_utils import write_record_to_csv
from sandcastle_pkg.phase1.create_guest_user_contact import ensure_guest_user_contact

# Global flag to track if Person Accounts are enabled
_person_accounts_enabled = None

def check_person_accounts_enabled(sf_cli):
    """Check once if Person Accounts are enabled in the org"""
    global _person_accounts_enabled
    if _person_accounts_enabled is not None:
        return _person_accounts_enabled
    
    try:
        # Try a simple query with IsPersonAccount
        query = "SELECT IsPersonAccount FROM Account LIMIT 1"
        sf_cli.query_records(query)
        _person_accounts_enabled = True
        print("  [INFO] Person Accounts are enabled in this org")
    except Exception as e:
        if 'IsPersonAccount' in str(e) or 'No such column' in str(e):
            _person_accounts_enabled = False
            print("  [INFO] Person Accounts are not enabled in this org - skipping Person Account checks")
        else:
            # Unknown error, assume not enabled to be safe
            _person_accounts_enabled = False
    
    return _person_accounts_enabled


def create_account_relationship_phase1(prod_relationship_id, created_relationships, relationship_insertable_fields_info,
                                      sf_cli_source, sf_cli_target, dummy_records, script_dir, created_accounts, created_contacts):
    """
    Phase 1: Create AccountRelationship with Account lookups.
    Recursively creates referenced accounts if they don't exist yet.
    Ensures both accounts have guest user contacts required for AccountRelationships.
    
    Args:
        prod_relationship_id: Production AccountRelationship ID
        created_relationships: Dictionary mapping prod_id -> sandbox_id
        relationship_insertable_fields_info: Field metadata
        sf_cli_source: Source org CLI
        sf_cli_target: Target org CLI
        dummy_records: Dictionary of dummy record IDs by object type
        script_dir: Script directory for CSV storage
        created_accounts: Dictionary of created Account mappings
        created_contacts: Dictionary of created Contact mappings
        
    Returns:
        str: Sandbox AccountRelationship ID or None
    """
    # Skip if already created
    if prod_relationship_id in created_relationships:
        print(f"  AccountRelationship {prod_relationship_id} already created as {created_relationships[prod_relationship_id]}")
        return created_relationships[prod_relationship_id]
    
    print(f"\n[PHASE 1] Creating AccountRelationship {prod_relationship_id}")
    
    # Fetch from source
    prod_relationship_record = sf_cli_source.get_record('AccountRelationship', prod_relationship_id)
    if not prod_relationship_record:
        print(f"  ✗ Could not fetch AccountRelationship {prod_relationship_id} from source org")
        return None
    
    # Ensure both AccountFromId and AccountToId exist in sandbox
    from create_account_phase1 import create_account_phase1
    account_fields = load_insertable_fields('Account', script_dir)
    
    account_from_id = prod_relationship_record.get('AccountFromId')
    account_to_id = prod_relationship_record.get('AccountToId')
    
    # Check if Person Accounts are enabled (only once per migration run)
    if check_person_accounts_enabled(sf_cli_source):
        # Check if either account is a Person Account (AccountRelationship doesn't support Person Accounts)
        def is_person_account(account_id, sf_cli):
            """Check if an account is a Person Account."""
            if not account_id:
                return False
            try:
                query = f"SELECT IsPersonAccount FROM Account WHERE Id = '{account_id}'"
                result = sf_cli.query_records(query)
                if result and len(result) > 0:
                    return result[0].get('IsPersonAccount', False)
            except Exception:
                pass  # If we can't check, assume it's not a Person Account
            return False
        
        # Check if AccountFrom is a Person Account in production
        if account_from_id and is_person_account(account_from_id, sf_cli_source):
            print(f"  ⚠ Skipping AccountRelationship {prod_relationship_id}: AccountFrom {account_from_id} is a Person Account")
            print(f"    (AccountRelationship cannot be created with Person Accounts)")
            return None
        
        # Check if AccountTo is a Person Account in production
        if account_to_id and is_person_account(account_to_id, sf_cli_source):
            print(f"  ⚠ Skipping AccountRelationship {prod_relationship_id}: AccountTo {account_to_id} is a Person Account")
            print(f"    (AccountRelationship cannot be created with Person Accounts)")
            return None
    
    if account_from_id and account_from_id not in created_accounts:
        print(f"  → Creating referenced AccountFrom {account_from_id}")
        create_account_phase1(account_from_id, created_accounts, account_fields,
                            sf_cli_source, sf_cli_target, dummy_records, script_dir)
    
    if account_to_id and account_to_id not in created_accounts:
        print(f"  → Creating referenced AccountTo {account_to_id}")
        create_account_phase1(account_to_id, created_accounts, account_fields,
                            sf_cli_source, sf_cli_target, dummy_records, script_dir)
    
    # Ensure both accounts have guest user contacts (TEMPORARILY DISABLED)
    # sandbox_account_from_id = created_accounts.get(account_from_id)
    # sandbox_account_to_id = created_accounts.get(account_to_id)
    # 
    # if sandbox_account_from_id:
    #     ensure_guest_user_contact(sandbox_account_from_id, sf_cli_target, created_contacts, script_dir)
    # 
    # if sandbox_account_to_id:
    #     ensure_guest_user_contact(sandbox_account_to_id, sf_cli_target, created_contacts, script_dir)
    
    # Save original record for CSV (before any modifications)
    original_record = prod_relationship_record.copy()
    
    # Replace lookups with real Account IDs if available, otherwise use dummies
    created_mappings = {
        'Account': created_accounts,
        'AccountRelationship': created_relationships
    }
    record_with_dummies = replace_lookups_with_dummies(
        prod_relationship_record,
        relationship_insertable_fields_info,
        dummy_records,
        created_mappings,
        sf_cli_source,
        sf_cli_target,
        'AccountRelationship'
    )
    
    # Filter to insertable fields
    filtered_data = filter_record_data(
        record_with_dummies,
        relationship_insertable_fields_info,
        sf_cli_target,
        'AccountRelationship'
    )
    filtered_data.pop('Id', None)

    # Check if this relationship already exists in the sandbox
    account_from_sandbox = filtered_data.get('AccountFromId')
    account_to_sandbox = filtered_data.get('AccountToId')
    if account_from_sandbox and account_to_sandbox:
        try:
            check_query = f"SELECT Id FROM AccountRelationship WHERE AccountFromId = '{account_from_sandbox}' AND AccountToId = '{account_to_sandbox}' LIMIT 1"
            existing_rels = sf_cli_target.query_records(check_query)
            if existing_rels and len(existing_rels) > 0:
                existing_id = existing_rels[0]['Id']
                print(f"  ℹ AccountRelationship already exists in sandbox: {existing_id}")
                created_relationships[prod_relationship_id] = existing_id
                write_record_to_csv('AccountRelationship', prod_relationship_id, existing_id, original_record, script_dir)
                return existing_id
        except Exception as check_error:
            print(f"  [WARN] Could not check for existing relationship: {check_error}")
    
    # Create in sandbox
    try:
        sandbox_relationship_id = sf_cli_target.create_record('AccountRelationship', filtered_data)
        if sandbox_relationship_id:
            print(f"  ✓ Created AccountRelationship: {prod_relationship_id} → {sandbox_relationship_id}")
            created_relationships[prod_relationship_id] = sandbox_relationship_id
            
            # Save to CSV for Phase 2
            write_record_to_csv('AccountRelationship', prod_relationship_id, sandbox_relationship_id, original_record, script_dir)
            
            return sandbox_relationship_id
        else:
            print(f"  ✗ Failed to create AccountRelationship {prod_relationship_id}")
            return None
            
    except Exception as e:
        error_msg = str(e)
        print(f"  ✗ Error creating AccountRelationship {prod_relationship_id}: {error_msg}")

        # Check for duplicate error patterns
        if "duplicate value found" in error_msg.lower() or "duplicate" in error_msg.lower():
            # Try to extract existing ID from error message
            match = re.search(r'with id:\s*([a-zA-Z0-9]{15,18})', error_msg)
            if match:
                existing_id = match.group(1)
                # Validate it looks like a Salesforce ID (starts with '0')
                if existing_id.startswith('0'):
                    print(f"  ℹ Found existing AccountRelationship {existing_id}, using it")
                    created_relationships[prod_relationship_id] = existing_id
                    write_record_to_csv('AccountRelationship', prod_relationship_id, existing_id, original_record, script_dir)
                    return existing_id
            # If no valid ID extracted from error, try to find existing relationship by querying
            try:
                account_from = filtered_data.get('AccountFromId')
                account_to = filtered_data.get('AccountToId')
                if account_from and account_to:
                    query = f"SELECT Id FROM AccountRelationship WHERE AccountFromId = '{account_from}' AND AccountToId = '{account_to}' LIMIT 1"
                    existing = sf_cli_target.query_records(query)
                    if existing and len(existing) > 0:
                        existing_id = existing[0]['Id']
                        print(f"  ℹ Found existing AccountRelationship by query: {existing_id}")
                        created_relationships[prod_relationship_id] = existing_id
                        write_record_to_csv('AccountRelationship', prod_relationship_id, existing_id, original_record, script_dir)
                        return existing_id
            except Exception as query_error:
                print(f"  Could not query for existing relationship: {query_error}")
        
        return None
