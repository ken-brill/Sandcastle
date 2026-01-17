#!/usr/bin/env python3
"""
Opportunity Creation - Phase 1

Author: Ken Brill
Version: 1.0
Date: December 24, 2025
License: MIT License

Phase 1: Creates Opportunity with dummy lookups.
Uses bypass RecordType to avoid Flow validation, saves actual RecordType for Phase 2.
"""
import re
from rich.console import Console
from rich.panel import Panel
from sandcastle_pkg.utils.record_utils import filter_record_data, replace_lookups_with_dummies
from sandcastle_pkg.utils.csv_utils import write_record_to_csv

console = Console()


def create_opportunity_phase1(prod_opp_id, created_opportunities, opportunity_insertable_fields_info,
                             sf_cli_source, sf_cli_target, dummy_records, script_dir, config, created_accounts=None, created_contacts=None):
    """
    Phase 1: Create Opportunity with dummy lookups and bypass RecordType.
    Saves actual RecordType for Phase 2 restoration.
    
    Args:
        prod_opp_id: Production Opportunity ID
        created_opportunities: Dictionary mapping prod_id -> sandbox_id
        opportunity_insertable_fields_info: Field metadata
        sf_cli_source: Source org CLI
        sf_cli_target: Target org CLI
        dummy_records: Dictionary of dummy record IDs by object type
        script_dir: Script directory for CSV storage
        config: Configuration dict with opportunity_bypass_record_type_id
        
    Returns:
        str: Sandbox Opportunity ID or None
    """
    # Skip if already created
    if prod_opp_id in created_opportunities:
        console.print(f"  [dim]Opportunity {prod_opp_id} already created as {created_opportunities[prod_opp_id]}[/dim]")
        return created_opportunities[prod_opp_id]
    
    console.rule(f"[bold cyan][PHASE 1] Creating Opportunity {prod_opp_id}")
    
    # Fetch from source
    prod_opp_record = sf_cli_source.get_record('Opportunity', prod_opp_id)
    if not prod_opp_record:
        console.print(f"[red]✗ Could not fetch Opportunity {prod_opp_id} from source org[/red]\n")
        return None
    
    # Save original record for CSV (including original RecordTypeId)
    original_record = prod_opp_record.copy()
    
    # Capture processing output
    with console.capture() as capture:
        # Replace lookups with dummy IDs or real IDs if available
        created_mappings = {
            'Account': created_accounts or {},
            'Contact': created_contacts or {},
            'Opportunity': created_opportunities
        }
        record_with_dummies = replace_lookups_with_dummies(
            prod_opp_record,
            opportunity_insertable_fields_info,
            dummy_records,
            created_mappings,
            sf_cli_source,
            sf_cli_target,
            'Opportunity'
        )
        
        # Get bypass RecordType from config (if available)
        # this lets us get past the flow: Opportunity - On create set stage depending on Tracking checkpoint
        bypass_record_type_id = config.get('opportunity_bypass_record_type_id')
        if bypass_record_type_id:
            record_with_dummies['RecordTypeId'] = bypass_record_type_id
            console.print(f"  [yellow][BYPASS] Using bypass RecordType: {bypass_record_type_id}[/yellow]")
        
        # Filter to insertable fields and validate picklists
        filtered_data = filter_record_data(
            record_with_dummies,
            opportunity_insertable_fields_info,
            sf_cli_target,
            'Opportunity'
        )
        filtered_data.pop('Id', None)
    
    # Display captured output in panel
    captured_text = capture.get().strip()
    if captured_text:
        console.print(Panel(captured_text, title="[dim]Processing Details[/dim]", border_style="dim", padding=(0, 1)))
    
    # Create in sandbox
    try:
        sandbox_opp_id = sf_cli_target.create_record('Opportunity', filtered_data)
        if sandbox_opp_id:
            console.print(f"[green]✓ Successfully created Opportunity with ID: {sandbox_opp_id}[/green]\n")
            created_opportunities[prod_opp_id] = sandbox_opp_id
            
            # Save to CSV for Phase 2 (with original RecordTypeId preserved)
            write_record_to_csv('Opportunity', prod_opp_id, sandbox_opp_id, original_record, script_dir)
            
            return sandbox_opp_id
        else:
            console.print(f"[red]✗ Failed to create Opportunity {prod_opp_id}[/red]\n")
            return None
            
    except Exception as e:
        error_msg = str(e)
        console.print(f"[red]✗ Error creating Opportunity {prod_opp_id}: {error_msg}[/red]\n")
        
        # Check for duplicate
        if "duplicate value found" in error_msg and "with id:" in error_msg:
            match = re.search(r'with id:\s*([a-zA-Z0-9]{15,18})', error_msg)
            if match:
                existing_id = match.group(1)
                # Validate it looks like a Salesforce ID (starts with '0')
                if existing_id.startswith('0'):
                    console.print(f"  [blue]ℹ Found existing Opportunity {existing_id}, using it[/blue]")
                    created_opportunities[prod_opp_id] = existing_id
                    # Save to CSV for Phase 2
                    write_record_to_csv('Opportunity', prod_opp_id, existing_id, original_record, script_dir)
                    return existing_id

        return None
