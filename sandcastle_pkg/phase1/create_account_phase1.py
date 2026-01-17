#!/usr/bin/env python3
"""
Account Creation - Phase 1

Author: Ken Brill
Version: 1.0
Date: December 24, 2025
License: MIT License

Phase 1: Creates Account with dummy lookups.
No dependency resolution - just create and save to CSV.
"""
import os
import re
from rich.console import Console
from rich.panel import Panel
from sandcastle_pkg.utils.record_utils import filter_record_data, replace_lookups_with_dummies
from sandcastle_pkg.utils.csv_utils import write_record_to_csv

console = Console()


def create_account_phase1(prod_account_id, created_accounts, account_insertable_fields_info, 
                          sf_cli_source, sf_cli_target, dummy_records, script_dir, 
                          prefetched_record=None, all_prefetched_accounts=None, progress_index=None, total_count=None):
    """
    Phase 1: Create Account with dummy lookups, save to CSV for Phase 2 update.
    Recursively creates any dependent Accounts (e.g., Primary_Partner__c).
    
    Args:
        prod_account_id: Production Account ID
        created_accounts: Dictionary mapping prod_id -> sandbox_id
        account_insertable_fields_info: Field metadata
        sf_cli_source: Source org CLI
        sf_cli_target: Target org CLI
        dummy_records: Dictionary of dummy record IDs by object type
        script_dir: Script directory for CSV storage
        prefetched_record: Optional pre-fetched account record (to avoid API call)
        all_prefetched_accounts: Optional dict of all prefetched account records for recursive lookups
        
    Returns:
        str: Sandbox Account ID or None
    """
    # Skip if already created
    if prod_account_id in created_accounts:
        console.print(f"  [dim]Account {prod_account_id} already created as {created_accounts[prod_account_id]}[/dim]")
        return created_accounts[prod_account_id]
    
    # Display progress counter if available
    if progress_index is not None and total_count is not None:
        console.rule(f"[bold cyan][PHASE 1] [{progress_index} of {total_count}] Creating Account {prod_account_id}")
    else:
        console.rule(f"[bold cyan][PHASE 1] Creating Account {prod_account_id}")
    
    # Use prefetched record if available, otherwise fetch from source
    if prefetched_record:
        prod_account_record = prefetched_record
        console.print(f"  [dim]Using prefetched account record[/dim]")
    else:
        prod_account_record = sf_cli_source.get_record('Account', prod_account_id)
        if not prod_account_record:
            console.print(f"  [red]✗ Could not fetch Account {prod_account_id} from source org[/red]")
            return None
    
    # Save original record for CSV (before any modifications)
    original_record = prod_account_record.copy()
    
    # RECURSIVE: Create any Account lookups first (e.g., Primary_Partner__c, ParentId)
    for field_name, field_info in account_insertable_fields_info.items():
        if field_info['type'] == 'reference' and field_info['referenceTo'] == 'Account':
            dependent_account_id = original_record.get(field_name)
            if dependent_account_id and not isinstance(dependent_account_id, dict):
                # Recursively create the dependent Account
                if dependent_account_id not in created_accounts:
                    console.print(f"  [yellow][DEPENDENCY] Account {prod_account_id} needs {field_name} → {dependent_account_id}[/yellow]")
                    # Check if we have this account prefetched
                    dependent_prefetched = None
                    if all_prefetched_accounts and dependent_account_id in all_prefetched_accounts:
                        dependent_prefetched = all_prefetched_accounts[dependent_account_id]
                    create_account_phase1(dependent_account_id, created_accounts, account_insertable_fields_info,
                                        sf_cli_source, sf_cli_target, dummy_records, script_dir,
                                        prefetched_record=dependent_prefetched,
                                        all_prefetched_accounts=all_prefetched_accounts)
    
    # Capture processing output
    with console.capture() as capture:
        # Replace lookups with dummy IDs
        # Build created_mappings with just Account since that's all we have at this stage
        created_mappings = {'Account': created_accounts}
        record_with_dummies = replace_lookups_with_dummies(
            prod_account_record, 
            account_insertable_fields_info, 
            dummy_records,
            created_mappings,
            sf_cli_source,
            sf_cli_target,
            'Account'
        )
        
        # Filter to insertable fields and validate picklists
        filtered_data = filter_record_data(
            record_with_dummies, 
            account_insertable_fields_info, 
            sf_cli_target, 
            'Account'
        )
        filtered_data.pop('Id', None)  # Remove production Id
    
    # Display captured output in panel
    captured_text = capture.get().strip()
    if captured_text:
        console.print(Panel(captured_text, title="[dim]Processing Details[/dim]", border_style="dim", padding=(0, 1)))
    
    # Create in sandbox
    try:
        sandbox_account_id = sf_cli_target.create_record('Account', filtered_data)
        if sandbox_account_id:
            console.print(f"[green]✓ Successfully created Account with ID: {sandbox_account_id}[/green]\n")
            created_accounts[prod_account_id] = sandbox_account_id
            
            # Save to CSV for Phase 2
            write_record_to_csv('Account', prod_account_id, sandbox_account_id, original_record, script_dir)
            
            return sandbox_account_id
        else:
            console.print(f"[red]✗ Failed to create Account {prod_account_id}[/red]\n")
            return None
            
    except Exception as e:
        error_msg = str(e)
        console.print(f"[red]✗ Error creating Account {prod_account_id}: {error_msg}[/red]\n")
        
        # Check for duplicate error with existing ID
        if "duplicate value found" in error_msg and "with id:" in error_msg:
            match = re.search(r'with id:\s*([a-zA-Z0-9]{15,18})', error_msg)
            if match:
                existing_id = match.group(1)
                console.print(f"  [blue]ℹ Found existing Account {existing_id}, using it[/blue]")
                created_accounts[prod_account_id] = existing_id
                
                # Still save to CSV for Phase 2 updates
                write_record_to_csv('Account', prod_account_id, existing_id, original_record, script_dir)
                
                return existing_id
        
        return None
