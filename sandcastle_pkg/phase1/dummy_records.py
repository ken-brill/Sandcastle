#!/usr/bin/env python3
"""
Dummy Record Creation Utilities

Author: Ken Brill
Version: 1.0
Date: December 24, 2025
License: MIT License

Utility to create and manage dummy records for two-phase migration.
Dummy records are used to satisfy required lookup fields during Phase 1 creation.
"""
from datetime import date, timedelta

def create_dummy_records(sf_cli_target, config=None):
    """
    Creates dummy records for all object types that will be migrated.
    Returns a dictionary mapping object types to their dummy record IDs.
    Note: User is not included as dummy - all User lookups use production IDs.
    
    Args:
        sf_cli_target: SalesforceCLI instance for the target sandbox
        config: Optional config dictionary
        
    Returns:
        dict: {object_type: dummy_record_id}
    """
    dummy_records = {}
    
    print("\n--- Creating Dummy Records for Phase 1 ---")
    print("Note: User lookups are not replaced with dummies - production User IDs are used directly")
    
    # Create NO ACCOUNT
    print("Creating NO ACCOUNT dummy record...")
    no_account_data = {'Name': 'NO ACCOUNT'}
    no_account_id = sf_cli_target.create_record('Account', no_account_data)
    if no_account_id:
        dummy_records['Account'] = no_account_id
        print(f"  ✓ NO ACCOUNT created: {no_account_id}")
    else:
        print("  ✗ Failed to create NO ACCOUNT")
        raise RuntimeError("Failed to create required dummy Account record")
    
    # Create NO CONTACT (requires AccountId)
    print("Creating NO CONTACT dummy record...")
    no_contact_data = {
        'LastName': 'NO CONTACT',
        'AccountId': no_account_id
    }
    no_contact_id = sf_cli_target.create_record('Contact', no_contact_data)
    if no_contact_id:
        dummy_records['Contact'] = no_contact_id
        print(f"  ✓ NO CONTACT created: {no_contact_id}")
    else:
        print("  ✗ Failed to create NO CONTACT")
        raise RuntimeError("Failed to create required dummy Contact record")
    
    # Create NO OPPORTUNITY (requires AccountId, Name, StageName, CloseDate)
    print("Creating NO OPPORTUNITY dummy record...")
    from datetime import date, timedelta
    close_date = (date.today() + timedelta(days=30)).isoformat()
    no_opp_data = {
        'Name': 'NO OPPORTUNITY',
        'AccountId': no_account_id,
        'StageName': 'Prospecting',
        'CloseDate': close_date
    }
    no_opp_id = sf_cli_target.create_record('Opportunity', no_opp_data)
    if no_opp_id:
        dummy_records['Opportunity'] = no_opp_id
        print(f"  ✓ NO OPPORTUNITY created: {no_opp_id}")
    else:
        print("  ✗ Failed to create NO OPPORTUNITY")
        raise RuntimeError("Failed to create required dummy Opportunity record")
    
    # Create NO QUOTE (requires Name, OpportunityId)
    print("Creating NO QUOTE dummy record...")
    no_quote_data = {
        'Name': 'NO QUOTE',
        'OpportunityId': no_opp_id
    }
    no_quote_id = sf_cli_target.create_record('Quote', no_quote_data)
    if no_quote_id:
        dummy_records['Quote'] = no_quote_id
        print(f"  ✓ NO QUOTE created: {no_quote_id}")
    else:
        print("  ✗ Failed to create NO QUOTE")
        raise RuntimeError("Failed to create required dummy Quote record")
    
    # Create NO ORDER (requires AccountId, EffectiveDate, Status)
    print("Creating NO ORDER dummy record...")
    effective_date = date.today().isoformat()
    no_order_data = {
        'AccountId': no_account_id,
        'EffectiveDate': effective_date,
        'Status': 'Draft'
    }
    no_order_id = sf_cli_target.create_record('Order', no_order_data)
    if no_order_id:
        dummy_records['Order'] = no_order_id
        print(f"  ✓ NO ORDER created: {no_order_id}")
    else:
        print("  ✗ Failed to create NO ORDER")
        raise RuntimeError("Failed to create required dummy Order record")
    
    # Create NO CASE (requires optional fields only, so minimal data)
    print("Creating NO CASE dummy record...")
    no_case_data = {
        'Subject': 'NO CASE'
    }
    no_case_id = sf_cli_target.create_record('Case', no_case_data)
    if no_case_id:
        dummy_records['Case'] = no_case_id
        print(f"  ✓ NO CASE created: {no_case_id}")
    else:
        print("  ✗ Failed to create NO CASE")
        raise RuntimeError("Failed to create required dummy Case record")
    
    # Note: Product2, Pricebook2, and PricebookEntry are NOT created as dummies
    # These have complex dependencies and already exist in sandbox
    # Phase 1 will remove these lookups, Phase 2 will restore them
    
    print(f"\n✓ Created {len(dummy_records)} dummy records")
    return dummy_records



# Delete all dummy records except NO ACCOUNT (for cleanup at end of migration)
def delete_all_dummies_except_no_account(sf_cli_target):
    """
    Deletes all dummy records (NO CONTACT, NO OPPORTUNITY, etc.) except NO ACCOUNT.
    Shows detailed error info if deletion fails.
    """
    from rich.console import Console
    import traceback
    console = Console()

    # Map of (sobject, field_name, field_value) for querying dummies
    # Different objects use different fields for identification
    dummy_queries = [
        ("Contact", "Name", "NO CONTACT"),        # Name is formula field = FirstName + LastName
        ("Opportunity", "Name", "NO OPPORTUNITY"),
        ("Quote", "Name", "NO QUOTE"),
        ("Case", "Subject", "NO CASE"),           # Case uses Subject, not Name
    ]

    # Order doesn't have a Name field - find by Account relationship to NO ACCOUNT
    # We'll handle Order separately by finding orders linked to NO ACCOUNT

    total_deleted = 0
    total_failed = 0
    console.print("\n[bold yellow]Cleaning up dummy records (except NO ACCOUNT)...[/bold yellow]")

    # Delete standard dummies
    for sobject, field, value in dummy_queries:
        query = f"SELECT Id FROM {sobject} WHERE {field} = '{value}'"
        try:
            records = sf_cli_target.query_records(query) or []
        except Exception as e:
            console.print(f"[red]✗ Error querying {sobject} dummies: {e}[/red]")
            continue
        if not records:
            continue
        for rec in records:
            try:
                deleted = sf_cli_target.delete_record(sobject, rec['Id'])
                if deleted:
                    console.print(f"[green]✓ Deleted {sobject} dummy: {rec['Id']} ({value})[/green]")
                    total_deleted += 1
                else:
                    console.print(f"[red]✗ Failed to delete {sobject} dummy: {rec['Id']} ({value}) (API returned False)[/red]")
                    total_failed += 1
            except Exception as e:
                console.print(f"[red]✗ Exception deleting {sobject} dummy: {rec['Id']} ({value}): {e}[/red]")
                total_failed += 1

    # Handle Order separately - find by Account.Name = 'NO ACCOUNT'
    try:
        order_query = "SELECT Id FROM Order WHERE Account.Name = 'NO ACCOUNT'"
        order_records = sf_cli_target.query_records(order_query) or []
        for rec in order_records:
            try:
                deleted = sf_cli_target.delete_record('Order', rec['Id'])
                if deleted:
                    console.print(f"[green]✓ Deleted Order dummy: {rec['Id']} (NO ORDER)[/green]")
                    total_deleted += 1
                else:
                    console.print(f"[red]✗ Failed to delete Order dummy: {rec['Id']} (API returned False)[/red]")
                    total_failed += 1
            except Exception as e:
                console.print(f"[red]✗ Exception deleting Order dummy: {rec['Id']}: {e}[/red]")
                total_failed += 1
    except Exception as e:
        console.print(f"[red]✗ Error querying Order dummies: {e}[/red]")

    if total_deleted == 0 and total_failed == 0:
        console.print("[dim]No dummy records needed to be deleted.[/dim]")
    else:
        console.print(f"[bold green]✓ Deleted {total_deleted} dummy record(s).[/bold green]")
        if total_failed > 0:
            console.print(f"[bold red]✗ Failed to delete {total_failed} dummy record(s). See above for details.[/bold red]")
