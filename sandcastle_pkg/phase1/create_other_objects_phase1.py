#!/usr/bin/env python3
"""
Quote, Order, Case Creation - Phase 1

Author: Ken Brill
Version: 1.1.8
Date: December 24, 2025
License: MIT License

Phase 1: Creates Quote, Order, QuoteLineItem, OrderItem, and Case with dummy lookups.
"""
from rich.console import Console, Group
from rich.panel import Panel
from sandcastle_pkg.utils.record_utils import filter_record_data, replace_lookups_with_dummies, load_insertable_fields
from sandcastle_pkg.utils.csv_utils import write_record_to_csv

console = Console()

def create_product2_phase1(prod_product_id, created_products, sf_cli_source, sf_cli_target, dummy_records, script_dir):
    """Phase 1: Create Product2 using real values - check if exists in sandbox first"""
    if prod_product_id in created_products:
        return created_products[prod_product_id]
    
    console.rule(f"[bold cyan][PHASE 1] Creating Product2 {prod_product_id}")
    
    prod_product_record = sf_cli_source.get_record('Product2', prod_product_id)
    if not prod_product_record:
        console.print(f"[red]✗ Could not fetch Product2 {prod_product_id}[/red]\n")
        return None
    
    original_record = prod_product_record.copy()
    
    # Capture processing output
    with console.capture() as capture:
        # Try to find existing product in sandbox by ProductCode or Name
        product_code = prod_product_record.get('ProductCode')
        product_name = prod_product_record.get('Name')
        
        existing_product_id = None
        if product_code:
            query = f"SELECT Id FROM Product2 WHERE ProductCode = '{product_code}' LIMIT 1"
            existing = sf_cli_target.query_records(query)
            if existing and len(existing) > 0:
                existing_product_id = existing[0]['Id']
                console.print(f"  [green]✓ Found existing Product2 by ProductCode: {existing_product_id}[/green]")
    
    if not existing_product_id and product_name:
        # Escape single quotes in name for SOQL (SOQL uses doubled single quotes)
        safe_name = product_name.replace("'", "''")
        query = f"SELECT Id FROM Product2 WHERE Name = '{safe_name}' LIMIT 1"
        existing = sf_cli_target.query_records(query)
        if existing and len(existing) > 0:
            existing_product_id = existing[0]['Id']
            console.print(f"  [green]✓ Found existing Product2 by Name: {existing_product_id}[/green]")
    
    if existing_product_id:
        created_products[prod_product_id] = existing_product_id
        write_record_to_csv('Product2', prod_product_id, existing_product_id, original_record, script_dir)
        return existing_product_id
    
    # Product doesn't exist, create it
    product_insertable_fields_info = load_insertable_fields('Product2', script_dir)
    
    # If no field CSV exists, use minimal required fields
    if not product_insertable_fields_info:
        product_insertable_fields_info = {
            'Name': {'type': 'string', 'referenceTo': ''},
            'IsActive': {'type': 'boolean', 'referenceTo': ''},
            'ProductCode': {'type': 'string', 'referenceTo': ''},
            'Family': {'type': 'picklist', 'referenceTo': ''},
            'Description': {'type': 'textarea', 'referenceTo': ''}
        }
    
    created_mappings = {'Product2': created_products}
    record_with_dummies = replace_lookups_with_dummies(
        prod_product_record, product_insertable_fields_info, dummy_records, created_mappings,
        sf_cli_source, sf_cli_target, 'Product2'
    )
    filtered_data = filter_record_data(record_with_dummies, product_insertable_fields_info, sf_cli_target, 'Product2')
    filtered_data.pop('Id', None)
    
    # Ensure required fields
    if 'Name' not in filtered_data and 'Name' in original_record:
        filtered_data['Name'] = original_record['Name']
    if 'IsActive' not in filtered_data:
        filtered_data['IsActive'] = True
    
    try:
        sandbox_product_id = sf_cli_target.create_record('Product2', filtered_data)
        if sandbox_product_id:
            console.print(f"  [green]✓ Created Product2: {prod_product_id} → {sandbox_product_id}[/green]")
            created_products[prod_product_id] = sandbox_product_id
            write_record_to_csv('Product2', prod_product_id, sandbox_product_id, original_record, script_dir)
            return sandbox_product_id
    except Exception as e:
        console.print(f"  [red]✗ Error creating Product2 {prod_product_id}: {e}[/red]")
    
    return None


def create_pricebook_entry_phase1(prod_pbe_id, created_pbes, sf_cli_source, sf_cli_target, dummy_records, script_dir, created_products):
    """Phase 1: Create PricebookEntry with real Product2 and Pricebook2 from sandbox"""
    if prod_pbe_id in created_pbes:
        return created_pbes[prod_pbe_id]
    
    console.print(f"\n[bold cyan][PHASE 1] Creating PricebookEntry {prod_pbe_id}[/bold cyan]")
    
    prod_pbe_record = sf_cli_source.get_record('PricebookEntry', prod_pbe_id)
    if not prod_pbe_record:
        console.print(f"[red]✗ Could not fetch PricebookEntry {prod_pbe_id}[/red]\n")
        return None
    
    original_record = prod_pbe_record.copy()
    
    # Get Product2Id from production record
    prod_product_id = prod_pbe_record.get('Product2Id')
    if not prod_product_id:
        console.print(f"  [red]✗ PricebookEntry missing Product2Id[/red]")
        return None

    # Ensure Product2 exists in sandbox (find or create)
    if prod_product_id not in created_products:
        create_product2_phase1(prod_product_id, created_products, sf_cli_source, sf_cli_target, dummy_records, script_dir)
    
    sandbox_product_id = created_products.get(prod_product_id)
    if not sandbox_product_id:
        console.print(f"  [red]✗ Could not find or create Product2 for PricebookEntry[/red]")
        return None
    
    # Get Pricebook2Id from production record
    prod_pricebook_id = prod_pbe_record.get('Pricebook2Id')
    if not prod_pricebook_id:
        # Fallback to Standard Pricebook if not specified
        standard_pb_query = "SELECT Id FROM Pricebook2 WHERE IsStandard = true LIMIT 1"
        standard_pb = sf_cli_target.query_records(standard_pb_query)
        if standard_pb and len(standard_pb) > 0:
            prod_pricebook_id = standard_pb[0]['Id']
            console.print(f"  [blue]ℹ [PRICEBOOK] No Pricebook2Id in production, using Standard Pricebook: {prod_pricebook_id}[/blue]")
        else:
            console.print(f"  [red]✗ Could not determine Pricebook for PricebookEntry[/red]")
            return None
    else:
        console.print(f"  [blue]ℹ [PRICEBOOK] Using production Pricebook: {prod_pricebook_id}[/blue]")
    
    console.print(f"  [blue]ℹ [PRODUCT2] Using Product2: {prod_product_id} → {sandbox_product_id}[/blue]")
    
    # Check if PricebookEntry already exists for this Product and Pricebook
    existing_pbe_query = f"SELECT Id FROM PricebookEntry WHERE Product2Id = '{sandbox_product_id}' AND Pricebook2Id = '{prod_pricebook_id}' LIMIT 1"
    existing_pbe = sf_cli_target.query_records(existing_pbe_query)
    if existing_pbe and len(existing_pbe) > 0:
        existing_pbe_id = existing_pbe[0]['Id']
        console.print(f"  [green]✓ Found existing PricebookEntry: {existing_pbe_id}[/green]")
        created_pbes[prod_pbe_id] = existing_pbe_id
        write_record_to_csv('PricebookEntry', prod_pbe_id, existing_pbe_id, original_record, script_dir)
        return existing_pbe_id
    
    # Create new PricebookEntry
    pbe_insertable_fields_info = load_insertable_fields('PricebookEntry', script_dir)
    
    # If no field CSV exists, use minimal required fields
    if not pbe_insertable_fields_info:
        pbe_insertable_fields_info = {
            'Pricebook2Id': {'type': 'reference', 'referenceTo': 'Pricebook2'},
            'Product2Id': {'type': 'reference', 'referenceTo': 'Product2'},
            'UnitPrice': {'type': 'currency', 'referenceTo': ''},
            'IsActive': {'type': 'boolean', 'referenceTo': ''},
            'UseStandardPrice': {'type': 'boolean', 'referenceTo': ''}
        }
    
    created_mappings = {
        'Product2': created_products,
        'PricebookEntry': created_pbes
    }
    record_with_dummies = replace_lookups_with_dummies(
        prod_pbe_record, pbe_insertable_fields_info, dummy_records, created_mappings,
        sf_cli_source, sf_cli_target, 'PricebookEntry'
    )
    
    # Set real Product2 and Pricebook2 IDs
    record_with_dummies['Product2Id'] = sandbox_product_id
    record_with_dummies['Pricebook2Id'] = prod_pricebook_id
    
    filtered_data = filter_record_data(record_with_dummies, pbe_insertable_fields_info, sf_cli_target, 'PricebookEntry')
    filtered_data.pop('Id', None)
    
    # Ensure required fields
    if 'UnitPrice' not in filtered_data and 'UnitPrice' in original_record:
        filtered_data['UnitPrice'] = original_record['UnitPrice']
    if 'IsActive' not in filtered_data:
        filtered_data['IsActive'] = True
    
    # Ensure Product2Id and Pricebook2Id are set
    filtered_data['Product2Id'] = sandbox_product_id
    filtered_data['Pricebook2Id'] = prod_pricebook_id
    
    try:
        sandbox_pbe_id = sf_cli_target.create_record('PricebookEntry', filtered_data)
        if sandbox_pbe_id:
            console.print(f"  [green]✓ Created PricebookEntry: {prod_pbe_id} → {sandbox_pbe_id}[/green]")
            created_pbes[prod_pbe_id] = sandbox_pbe_id
            write_record_to_csv('PricebookEntry', prod_pbe_id, sandbox_pbe_id, original_record, script_dir)
            return sandbox_pbe_id
    except Exception as e:
        console.print(f"  [red]✗ Error creating PricebookEntry {prod_pbe_id}: {e}[/red]")
    
    return None

def create_quote_phase1(prod_quote_id, created_quotes, sf_cli_source, sf_cli_target, dummy_records, script_dir, created_accounts=None, created_contacts=None, created_opportunities=None):
    """Phase 1: Create Quote with dummy OpportunityId"""
    if prod_quote_id in created_quotes:
        return created_quotes[prod_quote_id]
    
    console.rule(f"[bold cyan][PHASE 1] Creating Quote {prod_quote_id}")
    
    prod_quote_record = sf_cli_source.get_record('Quote', prod_quote_id)
    if not prod_quote_record:
        console.print(f"  [red]✗ Could not fetch Quote {prod_quote_id}[/red]")
        return None
    
    original_record = prod_quote_record.copy()
    quote_insertable_fields_info = load_insertable_fields('Quote', script_dir)
    
    created_mappings = {
        'Account': created_accounts or {},
        'Contact': created_contacts or {},
        'Opportunity': created_opportunities or {},
        'Quote': created_quotes
    }
    record_with_dummies = replace_lookups_with_dummies(
        prod_quote_record, quote_insertable_fields_info, dummy_records, created_mappings,
        sf_cli_source, sf_cli_target, 'Quote'
    )
    filtered_data = filter_record_data(record_with_dummies, quote_insertable_fields_info, sf_cli_target, 'Quote')
    filtered_data.pop('Id', None)
    
    # Ensure Pricebook2Id from production is preserved (all pricebooks exist in sandbox)
    if 'Pricebook2Id' in original_record and original_record['Pricebook2Id']:
        filtered_data['Pricebook2Id'] = original_record['Pricebook2Id']
        console.print(f"  [blue]ℹ [PRICEBOOK] Using production Pricebook: {original_record['Pricebook2Id']}[/blue]")
    
    try:
        sandbox_quote_id = sf_cli_target.create_record('Quote', filtered_data)
        if sandbox_quote_id:
            console.print(f"  [green]✓ Created Quote: {prod_quote_id} → {sandbox_quote_id}[/green]")
            created_quotes[prod_quote_id] = sandbox_quote_id
            write_record_to_csv('Quote', prod_quote_id, sandbox_quote_id, original_record, script_dir)
            return sandbox_quote_id
    except Exception as e:
        console.print(f"  [red]✗ Error creating Quote {prod_quote_id}: {e}[/red]")
    
    return None


def create_quote_line_item_phase1(prod_qli_id, created_qlis, sf_cli_source, sf_cli_target, dummy_records, script_dir, created_products, created_pbes, created_quotes, created_accounts=None, created_contacts=None, created_opportunities=None):
    """Phase 1: Create QuoteLineItem with Product2 and PricebookEntry dependencies"""
    if prod_qli_id in created_qlis:
        return created_qlis[prod_qli_id]
    
    console.print(f"\n[bold cyan][PHASE 1] Creating QuoteLineItem {prod_qli_id}[/bold cyan]")
    
    prod_qli_record = sf_cli_source.get_record('QuoteLineItem', prod_qli_id)
    if not prod_qli_record:
        console.print(f"  [red]✗ Could not fetch QuoteLineItem {prod_qli_id}[/red]")
        return None
    
    original_record = prod_qli_record.copy()
    qli_insertable_fields_info = load_insertable_fields('QuoteLineItem', script_dir)
    
    # Get the parent Quote ID from production
    prod_quote_id = prod_qli_record.get('QuoteId')
    if prod_quote_id:
        # Ensure parent Quote exists first (don't use dummy)
        if prod_quote_id not in created_quotes:
            console.print(f"  [blue]ℹ [QUOTE] Parent Quote not yet created, creating now: {prod_quote_id}[/blue]")
            create_quote_phase1(prod_quote_id, created_quotes, sf_cli_source, sf_cli_target, dummy_records, script_dir)
        
        # Use the real Quote ID from sandbox
        if prod_quote_id in created_quotes:
            sandbox_quote_id = created_quotes[prod_quote_id]
            console.print(f"  [blue]ℹ [QUOTE] Using parent Quote: {prod_quote_id} → {sandbox_quote_id}[/blue]")
    
    # Handle Product2Id - create if needed
    prod_product_id = prod_qli_record.get('Product2Id')
    if prod_product_id and prod_product_id not in created_products:
        create_product2_phase1(prod_product_id, created_products, sf_cli_source, sf_cli_target, dummy_records, script_dir)
    
    # Handle PricebookEntryId - create if needed
    prod_pbe_id = prod_qli_record.get('PricebookEntryId')
    if prod_pbe_id and prod_pbe_id not in created_pbes:
        create_pricebook_entry_phase1(prod_pbe_id, created_pbes, sf_cli_source, sf_cli_target, dummy_records, script_dir, created_products)
    
    created_mappings = {
        'Product2': created_products,
        'PricebookEntry': created_pbes,
        'Quote': created_quotes,
        'QuoteLineItem': created_qlis,
        'Account': created_accounts or {},
        'Contact': created_contacts or {},
        'Opportunity': created_opportunities or {}
    }
    record_with_dummies = replace_lookups_with_dummies(
        prod_qli_record, qli_insertable_fields_info, dummy_records, created_mappings,
        sf_cli_source, sf_cli_target, 'QuoteLineItem'
    )
    
    # Override with real Quote ID (don't use dummy)
    if prod_quote_id and prod_quote_id in created_quotes:
        record_with_dummies['QuoteId'] = created_quotes[prod_quote_id]
    
    # Use created Product2 and PricebookEntry IDs if available
    if prod_product_id and prod_product_id in created_products:
        record_with_dummies['Product2Id'] = created_products[prod_product_id]
        console.print(f"  [blue]ℹ [PRODUCT2] Using created Product2: {prod_product_id} → {created_products[prod_product_id]}[/blue]")
    
    if prod_pbe_id and prod_pbe_id in created_pbes:
        record_with_dummies['PricebookEntryId'] = created_pbes[prod_pbe_id]
        console.print(f"  [blue]ℹ [PBE] Using created PricebookEntry: {prod_pbe_id} → {created_pbes[prod_pbe_id]}[/blue]")
    
    filtered_data = filter_record_data(record_with_dummies, qli_insertable_fields_info, sf_cli_target, 'QuoteLineItem')
    filtered_data.pop('Id', None)
    
    # CRITICAL: Ensure PricebookEntryId is present (required field)
    # Re-add after filtering in case it was removed
    if prod_pbe_id and prod_pbe_id in created_pbes:
        filtered_data['PricebookEntryId'] = created_pbes[prod_pbe_id]
    
    # Handle negative prices - Salesforce doesn't allow negative UnitPrice
    if 'UnitPrice' in filtered_data and filtered_data['UnitPrice'] is not None:
        if isinstance(filtered_data['UnitPrice'], (int, float)) and filtered_data['UnitPrice'] < 0:
            console.print(f"  [yellow]⚠ [PRICE FIX] Converting negative UnitPrice {filtered_data['UnitPrice']} to 0.01[/yellow]")
            filtered_data['UnitPrice'] = 0.01
    
    # Handle negative custom total price fields
    for field_name in ['Custom_Total_Price__c', 'TotalPrice']:
        if field_name in filtered_data and filtered_data[field_name] is not None:
            if isinstance(filtered_data[field_name], (int, float)) and filtered_data[field_name] < 0:
                console.print(f"  [yellow]⚠ [PRICE FIX] Converting negative {field_name} {filtered_data[field_name]} to 0[/yellow]")
                filtered_data[field_name] = 0
    
    try:
        sandbox_qli_id = sf_cli_target.create_record('QuoteLineItem', filtered_data)
        if sandbox_qli_id:
            console.print(f"  [green]✓ Created QuoteLineItem: {prod_qli_id} → {sandbox_qli_id}[/green]")
            created_qlis[prod_qli_id] = sandbox_qli_id
            write_record_to_csv('QuoteLineItem', prod_qli_id, sandbox_qli_id, original_record, script_dir)
            return sandbox_qli_id
    except Exception as e:
        console.print(f"  [red]✗ Error creating QuoteLineItem {prod_qli_id}: {e}[/red]")
    
    return None


def create_order_phase1(prod_order_id, created_orders, sf_cli_source, sf_cli_target, dummy_records, script_dir, created_accounts=None, created_contacts=None):
    """Phase 1: Create Order with dummy AccountId"""
    if prod_order_id in created_orders:
        return created_orders[prod_order_id]
    
    console.rule(f"[bold cyan][PHASE 1] Creating Order {prod_order_id}")
    
    prod_order_record = sf_cli_source.get_record('Order', prod_order_id)
    if not prod_order_record:
        console.print(f"  [red]✗ Could not fetch Order {prod_order_id}[/red]")
        return None
    
    original_record = prod_order_record.copy()
    order_insertable_fields_info = load_insertable_fields('Order', script_dir)
    
    # Capture all intermediate output
    with console.capture() as capture:
        created_mappings = {
            'Account': created_accounts or {},
            'Contact': created_contacts or {},
            'Order': created_orders
        }
        record_with_dummies = replace_lookups_with_dummies(
            prod_order_record, order_insertable_fields_info, dummy_records, created_mappings,
            sf_cli_source, sf_cli_target, 'Order'
        )
        filtered_data = filter_record_data(record_with_dummies, order_insertable_fields_info, sf_cli_target, 'Order')
        filtered_data.pop('Id', None)
        
        # Ensure Pricebook2Id from production is preserved (all pricebooks exist in sandbox)
        if 'Pricebook2Id' in original_record and original_record['Pricebook2Id']:
            filtered_data['Pricebook2Id'] = original_record['Pricebook2Id']
            console.print(f"  [blue]ℹ [PRICEBOOK] Using production Pricebook: {original_record['Pricebook2Id']}[/blue]")
    
    # Display captured output in a panel if there's content
    captured_text = capture.get().strip()
    if captured_text:
        console.print(Panel(captured_text, title="[dim]Processing Details[/dim]", border_style="dim", padding=(0, 1)))
    
    try:
        sandbox_order_id = sf_cli_target.create_record('Order', filtered_data)
        if sandbox_order_id:
            console.print(f"[green]✓ Successfully created Order with ID: {sandbox_order_id}[/green]\n")
            created_orders[prod_order_id] = sandbox_order_id
            write_record_to_csv('Order', prod_order_id, sandbox_order_id, original_record, script_dir)
            return sandbox_order_id
    except Exception as e:
        console.print(f"[red]✗ Error creating Order {prod_order_id}: {e}[/red]\n")
    
    return None


def create_order_item_phase1(prod_order_item_id, created_order_items, sf_cli_source, sf_cli_target, dummy_records, script_dir, created_products, created_pbes, created_orders, created_accounts=None, created_contacts=None):
    """Phase 1: Create OrderItem with Product2 and PricebookEntry dependencies"""
    if prod_order_item_id in created_order_items:
        return created_order_items[prod_order_item_id]
    
    console.rule(f"[bold cyan][PHASE 1] Creating OrderItem {prod_order_item_id}")
    
    prod_order_item_record = sf_cli_source.get_record('OrderItem', prod_order_item_id)
    if not prod_order_item_record:
        console.print(f"  [red]✗ Could not fetch OrderItem {prod_order_item_id}[/red]")
        return None
    
    original_record = prod_order_item_record.copy()
    order_item_insertable_fields_info = load_insertable_fields('OrderItem', script_dir)
    
    # Get the parent Order ID from production
    prod_order_id = prod_order_item_record.get('OrderId')
    if prod_order_id:
        # Ensure parent Order exists first (don't use dummy)
        if prod_order_id not in created_orders:
            console.print(f"  [blue]ℹ [ORDER] Parent Order not yet created, creating now: {prod_order_id}[/blue]")
            create_order_phase1(prod_order_id, created_orders, sf_cli_source, sf_cli_target, dummy_records, script_dir)
        
        # Use the real Order ID from sandbox
        if prod_order_id in created_orders:
            sandbox_order_id = created_orders[prod_order_id]
            console.print(f"  [blue]ℹ [ORDER] Using parent Order: {prod_order_id} → {sandbox_order_id}[/blue]")
    
    # Handle Product2Id - create if needed
    prod_product_id = prod_order_item_record.get('Product2Id')
    if prod_product_id and prod_product_id not in created_products:
        create_product2_phase1(prod_product_id, created_products, sf_cli_source, sf_cli_target, dummy_records, script_dir)
    
    # Handle PricebookEntryId - create if needed
    prod_pbe_id = prod_order_item_record.get('PricebookEntryId')
    if prod_pbe_id and prod_pbe_id not in created_pbes:
        create_pricebook_entry_phase1(prod_pbe_id, created_pbes, sf_cli_source, sf_cli_target, dummy_records, script_dir, created_products)
    
    created_mappings = {
        'Product2': created_products,
        'PricebookEntry': created_pbes,
        'Order': created_orders,
        'OrderItem': created_order_items,
        'Account': created_accounts or {},
        'Contact': created_contacts or {}
    }
    record_with_dummies = replace_lookups_with_dummies(
        prod_order_item_record, order_item_insertable_fields_info, dummy_records, created_mappings,
        sf_cli_source, sf_cli_target, 'OrderItem'
    )
    
    # Override with real Order ID (don't use dummy)
    if prod_order_id and prod_order_id in created_orders:
        record_with_dummies['OrderId'] = created_orders[prod_order_id]
    
    # Use created Product2 and PricebookEntry IDs if available
    if prod_product_id and prod_product_id in created_products:
        record_with_dummies['Product2Id'] = created_products[prod_product_id]
        console.print(f"  [blue]ℹ [PRODUCT2] Using created Product2: {prod_product_id} → {created_products[prod_product_id]}[/blue]")
    
    if prod_pbe_id and prod_pbe_id in created_pbes:
        record_with_dummies['PricebookEntryId'] = created_pbes[prod_pbe_id]
        console.print(f"  [blue]ℹ [PBE] Using created PricebookEntry: {prod_pbe_id} → {created_pbes[prod_pbe_id]}[/blue]")
    
    filtered_data = filter_record_data(record_with_dummies, order_item_insertable_fields_info, sf_cli_target, 'OrderItem')
    filtered_data.pop('Id', None)
    
    # Handle negative prices - Salesforce doesn't allow negative UnitPrice
    if 'UnitPrice' in filtered_data and filtered_data['UnitPrice'] is not None:
        if isinstance(filtered_data['UnitPrice'], (int, float)) and filtered_data['UnitPrice'] < 0:
            console.print(f"  [yellow]⚠ [PRICE FIX] Converting negative UnitPrice {filtered_data['UnitPrice']} to 0.01[/yellow]")
            filtered_data['UnitPrice'] = 0.01
    
    # Handle negative custom total price fields
    for field_name in ['Custom_Total_Price__c', 'TotalPrice']:
        if field_name in filtered_data and filtered_data[field_name] is not None:
            if isinstance(filtered_data[field_name], (int, float)) and filtered_data[field_name] < 0:
                console.print(f"  [yellow]⚠ [PRICE FIX] Converting negative {field_name} {filtered_data[field_name]} to 0[/yellow]")
                filtered_data[field_name] = 0
    
    try:
        sandbox_order_item_id = sf_cli_target.create_record('OrderItem', filtered_data)
        if sandbox_order_item_id:
            console.print(f"  [green]✓ Created OrderItem: {prod_order_item_id} → {sandbox_order_item_id}[/green]")
            created_order_items[prod_order_item_id] = sandbox_order_item_id
            write_record_to_csv('OrderItem', prod_order_item_id, sandbox_order_item_id, original_record, script_dir)
            return sandbox_order_item_id
    except Exception as e:
        console.print(f"  [red]✗ Error creating OrderItem {prod_order_item_id}: {e}[/red]")
    
    return None


def create_case_phase1(prod_case_id, created_cases, sf_cli_source, sf_cli_target, dummy_records, script_dir, created_accounts=None, created_contacts=None):
    """Phase 1: Create Case with dummy AccountId and ContactId"""
    if prod_case_id in created_cases:
        return created_cases[prod_case_id]
    
    console.rule(f"[bold cyan][PHASE 1] Creating Case {prod_case_id}")
    
    prod_case_record = sf_cli_source.get_record('Case', prod_case_id)
    if not prod_case_record:
        console.print(f"  [red]✗ Could not fetch Case {prod_case_id}[/red]")
        return None
    
    original_record = prod_case_record.copy()
    case_insertable_fields_info = load_insertable_fields('Case', script_dir)
    
    created_mappings = {
        'Account': created_accounts or {},
        'Contact': created_contacts or {},
        'Case': created_cases
    }
    record_with_dummies = replace_lookups_with_dummies(
        prod_case_record, case_insertable_fields_info, dummy_records, created_mappings,
        sf_cli_source, sf_cli_target, 'Case'
    )
    filtered_data = filter_record_data(record_with_dummies, case_insertable_fields_info, sf_cli_target, 'Case')
    filtered_data.pop('Id', None)
    
    try:
        sandbox_case_id = sf_cli_target.create_record('Case', filtered_data)
        if sandbox_case_id:
            console.print(f"  [green]✓ Created Case: {prod_case_id} → {sandbox_case_id}[/green]")
            created_cases[prod_case_id] = sandbox_case_id
            write_record_to_csv('Case', prod_case_id, sandbox_case_id, original_record, script_dir)
            return sandbox_case_id
    except Exception as e:
        console.print(f"  [red]✗ Error creating Case {prod_case_id}: {e}[/red]")
    
    return None
