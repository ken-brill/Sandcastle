#!/usr/bin/env python3
"""
SandCastle - Main Entry Point

Author: Ken Brill
Version: 1.0.1
Date: December 24, 2025
License: MIT License
"""

import os
import sys
import json
import argparse
import logging
import time
from pathlib import Path
from datetime import datetime
from glob import glob
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.align import Align

# Import from package structure
from sandcastle_pkg.cli import SalesforceCLI
from sandcastle_pkg.utils import (
    load_insertable_fields,
    clear_migration_csvs
)
from sandcastle_pkg.phase1 import (
    delete_existing_records,
    create_dummy_records,
    create_account_phase1,
    create_contact_phase1,
    create_opportunity_phase1,
    create_quote_phase1,
    create_quote_line_item_phase1,
    create_order_phase1,
    create_order_item_phase1,
    create_case_phase1
)
from sandcastle_pkg.phase2 import update_lookups_phase2

# Get package directory
PACKAGE_DIR = Path(__file__).parent


def show_title_screen():
    """Display fancy title screen with version and support info."""
    from sandcastle_pkg import __version__, __author__
    
    console = Console()
    
    # Create title text
    title = Text()
    title.append("🏰 ", style="yellow")
    title.append("SandCastle", style="bold cyan")
    title.append(" 🏰", style="yellow")
    
    # Create subtitle
    subtitle = Text()
    subtitle.append("Salesforce Sandbox Data Migration Tool", style="dim white")
    
    # Create version and author info
    info = Text()
    info.append(f"Version {__version__}", style="green")
    info.append(" • ", style="dim")
    info.append(f"by {__author__}", style="dim")
    
    # Create support info
    support = Text()
    support.append("📦 GitHub: ", style="dim")
    support.append("https://github.com/ken-brill/Sandcastle", style="blue underline")
    
    # Combine all text
    content = Text()
    content.append(title)
    content.append("\n")
    content.append(subtitle)
    content.append("\n\n")
    content.append(info)
    content.append("\n")
    content.append(support)
    
    # Create panel
    panel = Panel(
        Align.center(content),
        border_style="cyan",
        padding=(1, 2)
    )
    
    console.print()
    console.print(panel)
    console.print()


def create_accounts_phase1(config, account_fields, sf_cli_source, sf_cli_target, dummy_records, script_dir):
    """
    Phase 1: Create all accounts (root + related) using dynamic relationship expansion.
    Returns dictionary mapping production Account IDs to sandbox IDs.
    """
    created_accounts = {}
    
    # OPTIMIZED: Batch fetch all accounts (root + all related accounts) at once
    logging.info(f"\n--- Phase 1: Accounts (Root + All Related) ---")
    
    # Step 1: Build comprehensive query to fetch all related accounts
    root_account_ids = list(config["Accounts"])
    ids_str = "','".join(root_account_ids)
    
    # Build field list for query (account_fields is a dict: field_name -> field_info)
    field_names = [name for name in account_fields.keys() if name not in ['Id']]
    if field_names:
        fields_str = 'Id, ' + ', '.join(field_names)
    else:
        fields_str = 'Id'
    
    # Step 2: Dynamically build WHERE clause for ALL Account lookup/hierarchy fields
    # Find all fields that reference Account (Lookup or Hierarchy type)
    account_lookup_fields = []
    for field_name, field_info in account_fields.items():
        if field_info.get('type') in ['reference', 'hierarchy'] and field_info.get('referenceTo') == 'Account':
            account_lookup_fields.append(field_name)
    
    # Build OR conditions for each Account lookup field
    where_conditions = [f"Id IN ('{ids_str}')"]
    for field_name in account_lookup_fields:
        where_conditions.append(f"{field_name} IN ('{ids_str}')")
    
    where_clause = " OR ".join(where_conditions)
    
    locations_limit = config.get("locations_limit", 10)
    limit_clause = "" if locations_limit == -1 else f" LIMIT {locations_limit * len(root_account_ids) * 10}"
    
    logging.info(f"  Found {len(account_lookup_fields)} Account lookup/hierarchy field(s): {', '.join(account_lookup_fields)}")
    logging.info(f"  Querying all accounts related to {len(root_account_ids)} root account(s)")
    query = f"""SELECT {fields_str} FROM Account 
               WHERE {where_clause}
               {limit_clause}"""
    
    all_account_records = {}
    batch_records = sf_cli_source.query_records(query) or []
    for record in batch_records:
        all_account_records[record['Id']] = record
    
    logging.info(f"  Fetched {len(all_account_records)} account record(s) in one query")
    
    # Step 3: Process root accounts first
    logging.info(f"  Creating {len(root_account_ids)} root account(s)")
    total_accounts = len(all_account_records)
    current_index = 1
    for prod_account_id in root_account_ids:
        if prod_account_id in all_account_records:
            create_account_phase1(prod_account_id, created_accounts, account_fields,
                                sf_cli_source, sf_cli_target, dummy_records, script_dir,
                                prefetched_record=all_account_records[prod_account_id],
                                all_prefetched_accounts=all_account_records,
                                progress_index=current_index, total_count=total_accounts)
            current_index += 1
    
    # Step 4: Process all other related accounts
    related_account_ids = [acc_id for acc_id in all_account_records.keys() if acc_id not in root_account_ids]
    if related_account_ids:
        logging.info(f"  Creating {len(related_account_ids)} related account(s)")
        for prod_account_id in related_account_ids:
            create_account_phase1(prod_account_id, created_accounts, account_fields,
                                sf_cli_source, sf_cli_target, dummy_records, script_dir,
                                prefetched_record=all_account_records[prod_account_id],
                                all_prefetched_accounts=all_account_records,
                                progress_index=current_index, total_count=total_accounts)
            current_index += 1
    
    return created_accounts


def run_pre_migration_setup(config, sf_cli_source, sf_cli_target, script_dir):
    """Run all pre-migration setup tasks"""
    # Step 1: Delete existing records
    delete_existing_records(sf_cli_target, 
                           argparse.Namespace(no_delete=not config.get("delete_existing_records", False)),
                           config.get("target_sandbox_alias"))
    
    # Step 2: Clear migration CSVs
    logging.info("\n--- Clearing Migration CSVs ---")
    clear_migration_csvs(script_dir)
    logging.info("✓ Migration CSVs cleared\n")
    
    # Step 3: Create dummy records
    dummy_records = create_dummy_records(sf_cli_target, config)
    
    # Step 4: Pre-fetch picklist values for validation
    try:
        from sandcastle_pkg.utils import prefetch_picklist_values
        logging.info("\n--- Pre-fetching Picklist Values ---")
        prefetch_picklist_values(sf_cli_target, [
            ('Account', ['Type', 'Industry']),
            ('Contact', ['LeadSource']),
            ('Opportunity', ['StageName', 'LeadSource', 'Type']),
            ('Quote', ['Status']),
            ('Order', ['Status']),
            ('Case', ['Status', 'Origin', 'Priority'])
        ])
        logging.info("✓ Pre-fetched picklist values\n")
    except Exception as e:
        logging.warning(f"Could not pre-fetch some picklist values: {e}\n")
    
    # Step 5: Load field metadata for all objects
    logging.info("\n--- Loading Field Metadata ---")
    account_fields = load_insertable_fields('Account', script_dir)
    contact_fields = load_insertable_fields('Contact', script_dir)
    opportunity_fields = load_insertable_fields('Opportunity', script_dir)
    quote_fields = load_insertable_fields('Quote', script_dir)
    order_fields = load_insertable_fields('Order', script_dir)
    case_fields = load_insertable_fields('Case', script_dir)
    logging.info("✓ Loaded field metadata for all objects\n")
    
    return (account_fields, contact_fields, opportunity_fields, quote_fields, 
            order_fields, case_fields, dummy_records)


def main():
    """Main entry point for SandCastle migration tool"""
    # Version from package
    from sandcastle_pkg import __version__
    
    # Parse arguments
    parser = argparse.ArgumentParser(
        description='SandCastle - Two-Phase Salesforce Data Migration Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('-s', '--source-alias', help='Source org alias')
    parser.add_argument('-t', '--target-alias', help='Target sandbox alias')
    parser.add_argument('--no-delete', action='store_true', 
                       help='Skip deletion of existing records')
    parser.add_argument('--config', default=str(Path.home() / 'Sandcastle.json'),
                       help='Path to config file (default: ~/Sandcastle.json)')
    parser.add_argument('--version', action='version', 
                       version=f'SandCastle {__version__}')
    args = parser.parse_args()
    
    # Determine script directory - use current working directory for logs
    script_dir = Path.cwd()
    
    # Setup logging
    log_dir = script_dir / 'logs'
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f'migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    # Track execution time
    start_time = time.time()
    
    # Find config file
    config_path = Path(args.config).expanduser()
    if not config_path.exists():
        console.print("\n[bold red]❌ Configuration Error[/bold red]")
        console.print(f"[red]Sandcastle.json not found at: [bold]{config_path}[/bold][/red]")
        console.print("\n[yellow]To fix this:[/yellow]")
        console.print("  1. Create ~/Sandcastle.json with your settings")
        console.print("  2. Or specify a custom path: [cyan]sandcastle --config /path/to/config.json[/cyan]")
        console.print(f"\n[dim]Expected location: {Path.home() / 'Sandcastle.json'}[/dim]\n")
        return 1
    
    # Load config
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Show title screen
    show_title_screen()
    
    # Determine source/target aliases
    source_org_alias = args.source_alias or config.get("source_prod_alias")
    target_org_alias = args.target_alias or config.get("target_sandbox_alias")
    
    if not source_org_alias or not target_org_alias:
        logging.error("Source and target org aliases must be provided")
        return 1
    
    # Initialize CLI
    sf_cli_source = SalesforceCLI(target_org=source_org_alias)
    sf_cli_target = SalesforceCLI(target_org=target_org_alias)
    
    # Rich console output
    console = Console()
    console.rule("[bold cyan]TWO-PHASE DATA MIGRATION", style="cyan")
    console.print(f"[cyan]Source:[/cyan] [bold white]{source_org_alias}[/bold white]")
    console.print(f"[cyan]Target:[/cyan] [bold white]{target_org_alias}[/bold white]")
    console.print()
    
    # Also log to file
    logging.info("="*80)
    logging.info("TWO-PHASE DATA MIGRATION")
    logging.info("="*80)
    logging.info(f"Source: {source_org_alias}")
    logging.info(f"Target: {target_org_alias}")
    
    try:
        # Safety checks
        if not sf_cli_target.is_sandbox():
            logging.error(f"\nTarget '{target_org_alias}' is NOT a sandbox. Aborting.")
            return 1
        
        source_info = sf_cli_source.get_org_info()
        target_info = sf_cli_target.get_org_info()
        
        if source_info and target_info and source_info['instanceUrl'] == target_info['instanceUrl']:
            logging.error(f"\nSource and target are the SAME org. Aborting.")
            return 1
        
        console.print(f"[green]✓ Safety checks passed[/green]")
        
        # Run pre-migration setup
        (account_fields, contact_fields, opportunity_fields, quote_fields, 
         order_fields, case_fields, dummy_records) = run_pre_migration_setup(
            config, sf_cli_source, sf_cli_target, script_dir
        )
        
        # ========== PHASE 1: CREATE WITH DUMMIES ==========
        console.print()
        console.rule("[bold cyan]PHASE 1: CREATING RECORDS WITH DUMMY LOOKUPS", style="cyan")
        console.print()
        
        # Initialize dictionaries to track created records
        created_accounts = {}
        created_contacts = {}
        created_opportunities = {}
        created_quotes = {}
        created_qlis = {}
        created_orders = {}
        created_order_items = {}
        created_cases = {}
        created_account_relationships = {}
        created_products = {}
        created_pbes = {}
        
        # Create accounts (root + related)
        created_accounts = create_accounts_phase1(config, account_fields, sf_cli_source, 
                                                  sf_cli_target, dummy_records, script_dir)
        
        # Create other objects
        if config.get("contact_limit", 0) != 0:
            for prod_account_id in config["Accounts"]:
                if prod_account_id in created_accounts:
                    sandbox_account_id = created_accounts[prod_account_id]
                    # Query contacts for this account
                    contact_limit = config.get("contact_limit", 10)
                    limit_clause = "" if contact_limit == -1 else f"LIMIT {contact_limit}"
                    contacts_query = f"SELECT Id FROM Contact WHERE AccountId = '{prod_account_id}' {limit_clause}"
                    contacts = sf_cli_source.query_records(contacts_query) or []
                    
                    logging.info(f"\n--- Phase 1: Contacts for Account {prod_account_id[:8]}... ({len(contacts)}) ---")
                    for idx, contact_rec in enumerate(contacts, 1):
                        prod_id = contact_rec['Id']
                        create_contact_phase1(prod_id, created_contacts, contact_fields, 
                                            sf_cli_source, sf_cli_target, dummy_records, 
                                            script_dir, created_accounts)
        
        if config.get("opportunity_limit", 0) != 0:
            for prod_account_id in config["Accounts"]:
                if prod_account_id in created_accounts:
                    opp_limit = config.get("opportunity_limit", 10)
                    limit_clause = "" if opp_limit == -1 else f"LIMIT {opp_limit}"
                    opps_query = f"SELECT Id FROM Opportunity WHERE AccountId = '{prod_account_id}' {limit_clause}"
                    opps = sf_cli_source.query_records(opps_query) or []
                    
                    logging.info(f"\n--- Phase 1: Opportunities for Account {prod_account_id[:8]}... ({len(opps)}) ---")
                    for idx, opp_rec in enumerate(opps, 1):
                        prod_id = opp_rec['Id']
                        create_opportunity_phase1(prod_id, created_opportunities, opportunity_fields, 
                                                sf_cli_source, sf_cli_target, dummy_records, 
                                                script_dir, config, created_accounts, created_contacts)
        
        # Create Quotes and QuoteLineItems
        if config.get("quote_limit", 0) != 0 and created_opportunities:
            logging.info(f"\n--- Phase 1: Quotes & QuoteLineItems ---")
            for prod_opp_id in list(created_opportunities.keys()):
                quote_limit = config.get("quote_limit", 10)
                limit_clause = "" if quote_limit == -1 else f"LIMIT {quote_limit}"
                quotes_query = f"SELECT Id FROM Quote WHERE OpportunityId = '{prod_opp_id}' {limit_clause}"
                quotes = sf_cli_source.query_records(quotes_query) or []
                
                for quote_rec in quotes:
                    prod_id = quote_rec['Id']
                    created_qlis_for_quote = create_quote_phase1(prod_id, created_quotes, 
                                                                 sf_cli_source, sf_cli_target, 
                                                                 dummy_records, script_dir, 
                                                                 created_accounts, created_contacts,
                                                                 created_opportunities)
        
        # Create Orders and OrderItems
        if config.get("order_limit", 0) != 0:
            for prod_account_id in config["Accounts"]:
                if prod_account_id in created_accounts:
                    order_limit = config.get("order_limit", 10)
                    limit_clause = "" if order_limit == -1 else f"LIMIT {order_limit}"
                    orders_query = f"SELECT Id FROM Order WHERE AccountId = '{prod_account_id}' {limit_clause}"
                    orders = sf_cli_source.query_records(orders_query) or []
                    
                    logging.info(f"\n--- Phase 1: Orders & OrderItems for Account {prod_account_id[:8]}... ({len(orders)}) ---")
                    for order_rec in orders:
                        prod_id = order_rec['Id']
                        created_order_items_for_order = create_order_phase1(prod_id, created_orders, 
                                                                            sf_cli_source, sf_cli_target, 
                                                                            dummy_records, script_dir, 
                                                                            created_accounts, created_contacts)
        
        # Create Cases
        if config.get("case_limit", 0) != 0:
            for prod_account_id in config["Accounts"]:
                if prod_account_id in created_accounts:
                    case_limit = config.get("case_limit", 10)
                    limit_clause = "" if case_limit == -1 else f"LIMIT {case_limit}"
                    cases_query = f"SELECT Id FROM Case WHERE AccountId = '{prod_account_id}' {limit_clause}"
                    cases = sf_cli_source.query_records(cases_query) or []
                    
                    logging.info(f"\n--- Phase 1: Cases for Account {prod_account_id[:8]}... ({len(cases)}) ---")
                    for idx, case_rec in enumerate(cases, 1):
                        prod_id = case_rec['Id']
                        create_case_phase1(prod_id, created_cases, sf_cli_source, sf_cli_target, 
                                         dummy_records, script_dir, created_accounts, created_contacts)
        
        # ========== PHASE 2: UPDATE LOOKUPS ==========
        console = Console()
        console.print()
        console.rule("[bold cyan]PHASE 2: UPDATING LOOKUPS WITH ACTUAL RELATIONSHIPS", style="cyan")
        console.print()
        
        created_mappings = {
            'Account': created_accounts,
            'Contact': created_contacts,
            'Opportunity': created_opportunities,
            'Quote': created_quotes,
            'QuoteLineItem': created_qlis,
            'Order': created_orders,
            'OrderItem': created_order_items,
            'Case': created_cases,
            'Product2': created_products,
            'PricebookEntry': created_pbes,
            'AccountRelationship': created_account_relationships
        }
        
        # Update each object type
        update_lookups_phase2(sf_cli_source, sf_cli_target, script_dir, account_fields, created_mappings, 'Account', dummy_records)
        update_lookups_phase2(sf_cli_source, sf_cli_target, script_dir, contact_fields, created_mappings, 'Contact', dummy_records)
        update_lookups_phase2(sf_cli_source, sf_cli_target, script_dir, opportunity_fields, created_mappings, 'Opportunity', dummy_records)
        
        quote_fields = load_insertable_fields('Quote', script_dir)
        update_lookups_phase2(sf_cli_source, sf_cli_target, script_dir, quote_fields, created_mappings, 'Quote', dummy_records)
        
        qli_fields = load_insertable_fields('QuoteLineItem', script_dir)
        update_lookups_phase2(sf_cli_source, sf_cli_target, script_dir, qli_fields, created_mappings, 'QuoteLineItem', dummy_records)
        
        order_fields = load_insertable_fields('Order', script_dir)
        update_lookups_phase2(sf_cli_source, sf_cli_target, script_dir, order_fields, created_mappings, 'Order', dummy_records)
        
        order_item_fields = load_insertable_fields('OrderItem', script_dir)
        update_lookups_phase2(sf_cli_source, sf_cli_target, script_dir, order_item_fields, created_mappings, 'OrderItem', dummy_records)
        
        case_fields = load_insertable_fields('Case', script_dir)
        update_lookups_phase2(sf_cli_source, sf_cli_target, script_dir, case_fields, created_mappings, 'Case', dummy_records)
        
        # ========== SUMMARY ==========
        console = Console()
        
        console.print("\n")
        console.rule("[bold cyan]MIGRATION SUMMARY", style="cyan")
        console.print()
        
        # Create summary table
        table = Table(show_header=True, header_style="bold cyan", border_style="cyan")
        table.add_column("Object Type", style="white", width=30)
        table.add_column("Count", justify="right", style="green", width=10)
        
        summary_data = [
            ('Accounts', len(created_accounts)),
            ('Contacts', len(created_contacts)),
            ('Opportunities', len(created_opportunities)),
            ('Quotes', len(created_quotes)),
            ('Quote Line Items', len(created_qlis)),
            ('Orders', len(created_orders)),
            ('Order Items', len(created_order_items)),
            ('Cases', len(created_cases)),
            ('Account Relationships', len(created_account_relationships)),
            ('Products (reused)', len(created_products)),
            ('Pricebook Entries (reused)', len(created_pbes)),
        ]
        
        total = sum(count for _, count in summary_data)
        for obj_type, count in summary_data:
            if count > 0:
                table.add_row(obj_type, str(count))
        
        # Add separator and total
        table.add_section()
        table.add_row("[bold]TOTAL[/bold]", f"[bold]{total}[/bold]")
        
        console.print(table)
        console.print()
        
        # Calculate and display elapsed time
        elapsed_time = time.time() - start_time
        hours = int(elapsed_time // 3600)
        minutes = int((elapsed_time % 3600) // 60)
        seconds = int(elapsed_time % 60)
        
        if hours > 0:
            time_str = f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            time_str = f"{minutes}m {seconds}s"
        else:
            time_str = f"{seconds}s"
        
        console.print(f"[cyan]⏱  Total execution time:[/cyan] [bold white]{time_str}[/bold white]")
        console.print(f"[dim]ℹ  Counts include both newly created and existing/reused records.[/dim]")
        console.print(f"[dim]📋 Query log: {script_dir / 'logs' / 'queries.csv'}[/dim]")
        console.print()
        console.rule(style="cyan")

        # Clean up dummy records except NO ACCOUNT
        # Dummy cleanup function not present in this version

        # Also log to file
        logging.info("="*80)
        logging.info("MIGRATION SUMMARY")
        logging.info("="*80)
        for obj_type, count in summary_data:
            if count > 0:
                logging.info(f"  {obj_type:<20} {count:>6} record(s)")
        logging.info(f"  {'-'*28}")
        logging.info(f"  {'TOTAL':<20} {total:>6} record(s)")
        logging.info("="*80)
        logging.info(f"\nTotal execution time: {time_str}")
        logging.info("\nNote: Counts include both newly created and existing/reused records.")
        logging.info("Check output above for specific errors and warnings.")
        logging.info(f"\nQuery log available at: {script_dir / 'logs' / 'queries.csv'}")
        logging.info("="*80)
        
        return 0
        
    except RuntimeError as e:
        logging.error(f"\nCLI Error: {e}")
        return 1
    except Exception as e:
        logging.error(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
