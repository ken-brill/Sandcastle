#!/usr/bin/env python3
"""
Delete Existing Records with Portal User Protection

Author: Ken Brill
Version: 1.1.8
Date: December 24, 2025
License: MIT License
"""
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

def delete_existing_records(sf_cli_target, args, target_org_alias):
    """
    Deletes all demo data records from the target org, unless --no-delete is specified.
    This includes: Cases, OrderItems, Orders, QuoteLineItems, Quotes, Opportunities, Contacts, Accounts, and AccountRelationships.
    """
    # CRITICAL SAFETY CHECK: Verify this is a sandbox, not production
    console.print()
    console.rule("[bold yellow]🔒 SAFETY CHECK: Verifying Target Org", style="yellow")
    console.print()
    
    try:
        org_info = sf_cli_target.query_records("SELECT IsSandbox, Name, OrganizationType FROM Organization LIMIT 1")
        if not org_info or len(org_info) == 0:
            console.print("[red]❌ SAFETY CHECK FAILED: Could not retrieve Organization information[/red]")
            raise RuntimeError("SAFETY CHECK FAILED: Could not retrieve Organization information")
        
        org = org_info[0]
        is_sandbox = org.get('IsSandbox', False)
        org_name = org.get('Name', 'Unknown')
        org_type = org.get('OrganizationType', 'Unknown')
        
        # Display org info in a table
        info_table = Table(show_header=False, border_style="yellow", padding=(0, 1))
        info_table.add_column("Property", style="cyan")
        info_table.add_column("Value", style="white")
        info_table.add_row("Org Name", org_name)
        info_table.add_row("Org Type", org_type)
        info_table.add_row("Is Sandbox", "✅ Yes" if is_sandbox else "❌ No")
        console.print(info_table)
        console.print()
        
        if not is_sandbox:
            error_panel = Panel(
                f"[bold red]Target org '{org_name}' (Type: {org_type}) is NOT a sandbox![/bold red]\n\n"
                f"This function is designed to delete data ONLY from sandbox environments.\n"
                f"Deletion has been BLOCKED to protect production data.",
                title="[bold red]❌ CRITICAL ERROR: PRODUCTION ORG DETECTED[/bold red]",
                border_style="red",
                padding=(1, 2)
            )
            console.print(error_panel)
            console.print()
            raise RuntimeError("SAFETY ABORT: Attempted deletion on production org. Operation blocked.")
        
        console.print("[green]✅ Safety check passed: Confirmed sandbox environment[/green]\n")
        
    except RuntimeError:
        # Re-raise RuntimeError (our safety block)
        raise
    except Exception as e:
        console.print(f"[red]❌ SAFETY CHECK FAILED: Could not verify org type: {e}[/red]")
        console.print("[yellow]Aborting deletion as a safety precaution.[/yellow]\n")
        raise RuntimeError(f"Could not verify target org is a sandbox: {e}")
    
    if args.no_delete:
        console.print("[yellow]⏭ Skipping deletion of existing demo records (--no-delete flag)[/yellow]\n")
        return

    console.rule("[bold red]🗑️  DELETING EXISTING DEMO DATA", style="red")
    console.print(f"\n[yellow]⚠ This will delete all demo data from: [bold white]{target_org_alias}[/bold white][/yellow]")
    console.print("[dim]Objects: Cases, OrderItems, Orders, QuoteLineItems, Quotes, Opportunities, Contacts, Accounts, AccountRelationships[/dim]")
    console.print("[dim]This operation cannot be undone. To skip, use --no-delete flag.[/dim]\n")

    # Step 1: Identify Accounts/Contacts with portal users (they cannot be deleted)
    console.print("[cyan]🔍 Checking for portal users...[/cyan]")
    portal_account_ids = set()
    portal_contact_ids = set()
    try:
        # Query for portal users to find their associated Contacts and Accounts
        portal_users_query = "SELECT Id, Username, ContactId, Contact.AccountId FROM User WHERE ContactId != null"
        portal_users = sf_cli_target.query_records(portal_users_query)
        
        if portal_users and len(portal_users) > 0:
            console.print(f"[yellow]⚠ Found {len(portal_users)} portal user(s)[/yellow]")
            for user in portal_users:
                contact_id = user.get('ContactId')
                if contact_id:
                    portal_contact_ids.add(contact_id)
                # Try to get AccountId from nested Contact
                contact_data = user.get('Contact')
                if contact_data and isinstance(contact_data, dict):
                    account_id = contact_data.get('AccountId')
                    if account_id:
                        portal_account_ids.add(account_id)
                console.print(f"  [dim]Portal user: {user.get('Username', user['Id'])} (Contact: {contact_id})[/dim]")
            
            console.print(f"[yellow]⚠ Found {len(portal_contact_ids)} Contact(s) and {len(portal_account_ids)} Account(s) with portal users[/yellow]")
            console.print(f"[yellow]⚠ These records CANNOT be deleted and will be REUSED during migration[/yellow]\n")
        else:
            console.print("[green]✓ No portal users found[/green]\n")
    except Exception as e:
        console.print(f"[yellow]⚠ Warning: Could not query portal users: {e}[/yellow]")
        console.print("[dim]Continuing with deletion...[/dim]\n")

    # Step 2: Delete records in proper order (excluding portal-protected records)
    # Deletion order: Case, OrderItem, Order, QuoteLineItem, Quote, Opportunity, Contact, AccountRelationship, Account
    # AccountRelationship must be deleted before Accounts since it references them
    console.rule("[bold red]Deletion Progress", style="red")
    console.print()
    
    object_order = [
        'Case',
        'OrderItem',
        'Order',
        'QuoteLineItem',
        'Quote',
        'Opportunity',
        'Contact',
        'AccountRelationship',  # Delete before Account
        'Account',
    ]
    
    for obj in object_order:
        # Pass excluded IDs for Contacts and Accounts with portal users
        excluded_ids = None
        if obj == 'Contact' and portal_contact_ids:
            excluded_ids = portal_contact_ids
            console.print(f"[cyan]🗑️  Deleting all {obj} records (excluding {len(excluded_ids)} with portal users)...[/cyan]")
        elif obj == 'Account' and portal_account_ids:
            excluded_ids = portal_account_ids
            console.print(f"[cyan]🗑️  Deleting all {obj} records (excluding {len(excluded_ids)} with portal users)...[/cyan]")
        else:
            console.print(f"[cyan]🗑️  Deleting all {obj} records...[/cyan]")
        
        if not sf_cli_target.bulk_delete_all_records(obj, excluded_ids):
            console.print(f"[red]✗ Failed to delete existing {obj} records. Aborting.[/red]\n")
            raise RuntimeError(f"Failed to delete existing {obj} records.")
        console.print(f"[green]✓ Deleted {obj} records successfully[/green]")
    
    console.print()
    console.print("[green]✅ All demo data deletion complete[/green]\n")
