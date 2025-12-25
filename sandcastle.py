#!/usr/bin/env python3
"""
SandCastle - Two-Phase Data Migration Script

Author: Ken Brill
Version: 1.0
Date: December 24, 2025
License: MIT License

Phase 1: Create all records with dummy lookups and save to CSV
Phase 2: Update all records with actual lookup relationships
"""
import os
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime
from glob import glob
from salesforce_cli import SalesforceCLI
from record_utils import load_insertable_fields
from delete_existing_records import delete_existing_records
from dummy_records import create_dummy_records
from csv_utils import clear_migration_csvs
from create_account_phase1 import create_account_phase1
from create_contact_phase1 import create_contact_phase1
from create_opportunity_phase1 import create_opportunity_phase1
# from create_account_relationship_phase1 import create_account_relationship_phase1  # Disabled - not currently used
from create_other_objects_phase1 import (
    create_quote_phase1, create_quote_line_item_phase1,
    create_order_phase1, create_order_item_phase1, create_case_phase1
)
from update_lookups_phase2 import update_lookups_phase2


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
    fields_str = 'Id, ' + ', '.join(field_names)
    
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
    
    # Step 4: Process all other related accounts (locations, partners, etc.)
    related_count = len(all_account_records) - len(root_account_ids)
    if related_count > 0:
        logging.info(f"  Creating {related_count} related account(s) (locations, customers, partners, etc.)")
        for prod_account_id, record in all_account_records.items():
            if prod_account_id not in created_accounts:
                create_account_phase1(prod_account_id, created_accounts, account_fields,
                                    sf_cli_source, sf_cli_target, dummy_records, script_dir,
                                    prefetched_record=record,
                                    all_prefetched_accounts=all_account_records,
                                    progress_index=current_index, total_count=total_accounts)
                current_index += 1
    
    return created_accounts


def create_contacts_phase1(config, created_accounts, contact_fields, sf_cli_source, sf_cli_target, dummy_records, script_dir):
    """
    Phase 1: Create contacts using bulk API.
    Returns dictionary mapping production Contact IDs to sandbox IDs.
    """
    created_contacts = {}
    
    logging.info(f"\n--- Phase 1: Contacts ---")
    contact_limit = config.get('contact_limit', 10)
    if contact_limit == 0:
        logging.info("Skipping contacts (limit is 0)")
        return created_contacts
    
    # OPTIMIZED: Batch query all contacts for all accounts at once
    account_ids = list(created_accounts.keys())
    logging.info(f"  Fetching contacts for {len(account_ids)} account(s) in batches")
    
    all_contacts = []
    # Process in batches of 200 (SOQL limit for IN clause)
    for i in range(0, len(account_ids), 200):
        batch = account_ids[i:i+200]
        ids_str = "','".join(batch)
        
        # this is causing problems with accounts and it's still not letting us get account relationships so we will deal with it later
        # # First get community-licensed contacts
        # community_query = f"""
        #     SELECT Id, AccountId, Name 
        #     FROM Contact 
        #     WHERE AccountId IN ('{ids_str}')
        #     AND Id IN (
        #         SELECT ContactId FROM User 
        #         WHERE IsActive = true 
        #         AND (Profile.UserLicense.Name LIKE '%Customer%' OR Profile.UserLicense.Name LIKE '%Partner%')
        #     )
        # """
        # try:
        #     community_contacts = sf_cli_source.query_records(community_query) or []
        #     for contact in community_contacts:
        #         contact['IsCommunity'] = True
        #     all_contacts.extend(community_contacts)
        #     if community_contacts:
        #         logging.info(f"  Found {len(community_contacts)} community-licensed contact(s) in batch")
        # except Exception as e:
        #     logging.info(f"  Could not query community contacts (might not have community enabled): {e}")
        
        # Get regular contacts
        query = f"SELECT Id, AccountId FROM Contact WHERE AccountId IN ('{ids_str}') ORDER BY CreatedDate DESC"
        regular_contacts = sf_cli_source.query_records(query) or []
        for contact in regular_contacts:
            contact['IsCommunity'] = False
        all_contacts.extend(regular_contacts)
    
    # Group contacts by account and limit per account
    contacts_by_account = {}
    for contact in all_contacts:
        account_id = contact['AccountId']
        if account_id not in contacts_by_account:
            contacts_by_account[account_id] = {'community': [], 'regular': []}
        
        if contact.get('IsCommunity'):
            contacts_by_account[account_id]['community'].append(contact)
        else:
            contacts_by_account[account_id]['regular'].append(contact)
    
    # OPTIMIZED: Bulk create contacts using Bulk API 2.0
    logging.info("  Using Bulk API 2.0 for Contact creation")
    from bulk_utils import BulkRecordCreator
    from record_utils import filter_record_data, replace_lookups_with_dummies
    
    bulk_creator_contacts = BulkRecordCreator(sf_cli_target, batch_size=200)
    contacts_to_create = []
    contacts_being_batched = set()  # Track contacts being added to bulk to avoid duplicates
    
    # Prepare contacts respecting per-account limits
    for account_id, contacts in contacts_by_account.items():
        community_contacts = contacts['community']
        regular_contacts = contacts['regular']
        
        # Take community contacts first (max 1)
        for contact in community_contacts[:1]:
            if contact['Id'] not in created_contacts and contact['Id'] not in contacts_being_batched:
                contacts_to_create.append((contact['Id'], account_id, True))
                contacts_being_batched.add(contact['Id'])
        
        # Then regular contacts up to limit
        remaining_limit = contact_limit - len(community_contacts[:1])
        if contact_limit == -1:
            remaining_limit = len(regular_contacts)
        
        for contact in regular_contacts[:remaining_limit]:
            if contact['Id'] not in created_contacts and contact['Id'] not in contacts_being_batched:
                contacts_to_create.append((contact['Id'], account_id, False))
                contacts_being_batched.add(contact['Id'])
    
    logging.info(f"  Preparing {len(contacts_to_create)} Contact(s) for bulk creation")
    
    # Fetch and process all contacts
    processed_count = 0
    for prod_contact_id, prod_account_id, is_community in contacts_to_create:
        processed_count += 1
        # Progress indicator every 10 records
        if processed_count % 10 == 0:
            logging.info(f"  Processing... {processed_count}/{len(contacts_to_create)} contacts prepared")
        prod_contact_record = sf_cli_source.get_record('Contact', prod_contact_id)
        if not prod_contact_record:
            continue
        
        created_mappings = {
            'Account': created_accounts,
            'Contact': created_contacts
        }
        record_with_dummies = replace_lookups_with_dummies(
            prod_contact_record, contact_fields, dummy_records, created_mappings,
            sf_cli_source, sf_cli_target, 'Contact'
        )
        
        # Override AccountId with real sandbox ID
        if prod_account_id in created_accounts:
            record_with_dummies['AccountId'] = created_accounts[prod_account_id]
        
        filtered_data = filter_record_data(record_with_dummies, contact_fields, sf_cli_target, 'Contact')
        filtered_data.pop('Id', None)
        
        bulk_creator_contacts.add_record('Contact', filtered_data)
    
    # Flush all contacts
    if bulk_creator_contacts.get_pending_count('Contact') > 0:
        logging.info(f"  Flushing {bulk_creator_contacts.get_pending_count('Contact')} Contact(s) to Salesforce")
        try:
            result_ids = bulk_creator_contacts.flush('Contact')
            contact_ids = result_ids.get('Contact', [])
            
            # Map production IDs to sandbox IDs
            community_count = 0
            for idx, (prod_id, _, is_community) in enumerate(contacts_to_create):
                if idx < len(contact_ids):
                    created_contacts[prod_id] = contact_ids[idx]
                    if is_community:
                        community_count += 1
            
            logging.info(f"  ✓ Bulk created {len(contact_ids)} Contact(s) ({community_count} community-licensed)")
        except Exception as e:
            logging.error(f"  ✗ Bulk creation failed for Contacts: {e}")
            logging.warning("  Falling back to individual creation...")
            for prod_id, _, _ in contacts_to_create:
                if prod_id not in created_contacts:
                    create_contact_phase1(prod_id, created_contacts, contact_fields,
                                        sf_cli_source, sf_cli_target, dummy_records, script_dir, created_accounts)
    
    return created_contacts


def create_opportunities_phase1_bulk(config, created_accounts, created_contacts, opportunity_fields,
                                      sf_cli_source, sf_cli_target, dummy_records, script_dir):
    """
    Phase 1: Create opportunities using bulk API.
    Returns dictionary mapping production Opportunity IDs to sandbox IDs.
    """
    created_opportunities = {}
    
    logging.info(f"\n--- Phase 1: Opportunities ---")
    opportunity_limit = config.get('opportunity_limit', 10)
    if opportunity_limit == 0:
        logging.info("Skipping opportunities (limit is 0)")
        return created_opportunities
    
    # OPTIMIZED: Batch query all opportunities for all accounts at once
    account_ids = list(created_accounts.keys())
    logging.info(f"  Fetching opportunities for {len(account_ids)} account(s) in batches")
    
    # Find all Account lookup fields in Opportunity
    account_lookup_fields = []
    for field_name, field_info in opportunity_fields.items():
        if field_info.get('type') == 'reference' and field_info.get('referenceTo') == 'Account':
            account_lookup_fields.append(field_name)
    
    # Build SELECT clause with all Account lookup fields
    select_fields = ['Id'] + account_lookup_fields
    select_clause = ', '.join(select_fields)
    
    # Build WHERE clause with OR conditions for each Account lookup field
    # Calculate batch size based on number of Account lookup fields to avoid HTTP 431 error
    # Each lookup field adds to the query size, so reduce batch size accordingly
    base_batch_size = 200
    batch_size = max(50, base_batch_size // max(1, len(account_lookup_fields)))
    logging.info(f"  Using batch size of {batch_size} (adjusted for {len(account_lookup_fields)} Account lookup fields)")
    
    all_opportunities = []
    for i in range(0, len(account_ids), batch_size):
        batch = account_ids[i:i+batch_size]
        ids_str = "','".join(batch)
        
        where_conditions = []
        for field_name in account_lookup_fields:
            where_conditions.append(f"{field_name} IN ('{ids_str}')")
        where_clause = " OR ".join(where_conditions)
        
        query = f"""SELECT {select_clause} 
                   FROM Opportunity 
                   WHERE {where_clause}
                   ORDER BY CreatedDate DESC"""
        opps = sf_cli_source.query_records(query) or []
        all_opportunities.extend(opps)
        logging.info(f"    Batch {i//batch_size + 1}: Fetched {len(opps)} opportunities")
    
    # Remove duplicates
    seen_ids = set()
    unique_opportunities = []
    for opp in all_opportunities:
        if opp['Id'] not in seen_ids:
            seen_ids.add(opp['Id'])
            unique_opportunities.append(opp)
    
    # Group opportunities by primary account
    opps_by_primary_account = {}
    opps_via_other_lookups = []
    
    # this should create an SOQ well that looks something like this
#     SELECT Id, AccountId, Referred_to_Account__c, Partner_Account__c
#       FROM Opportunity
#       WHERE AccountId IN ('0014U00003NPdH5QAL','0014U00002qXYUJQA4')
#        OR Referred_to_Account__c IN ('0014U00003NPdH5QAL','0014U00002qXYUJQA4')
#        OR Partner_Account__c IN ('0014U00003NPdH5QAL','0014U00002qXYUJQA4')
#       ORDER BY CreatedDate DESC
    
    for opp in unique_opportunities:
        account_id = opp.get('AccountId')
        if account_id and account_id in created_accounts:
            if account_id not in opps_by_primary_account:
                opps_by_primary_account[account_id] = []
            opps_by_primary_account[account_id].append(opp)
        else:
            # Check if any other Account lookup field references a created account
            has_other_account_lookup = False
            for field_name in account_lookup_fields:
                if field_name != 'AccountId':  # Already checked above
                    lookup_value = opp.get(field_name)
                    if lookup_value and lookup_value in created_accounts:
                        has_other_account_lookup = True
                        break
            
            if has_other_account_lookup:
                opps_via_other_lookups.append(opp)
    
    # OPTIMIZED: Bulk create opportunities
    logging.info("  Using Bulk API 2.0 for Opportunity creation")
    from bulk_utils import BulkRecordCreator
    from record_utils import filter_record_data, replace_lookups_with_dummies
    
    bulk_creator_opps = BulkRecordCreator(sf_cli_target, batch_size=200)
    opps_to_create = []
    
    # Prepare opportunities respecting per-account limits
    for account_id, opps in opps_by_primary_account.items():
        limit_to_apply = opportunity_limit if opportunity_limit != -1 else len(opps)
        for opp in opps[:limit_to_apply]:
            if opp['Id'] not in created_opportunities:
                opps_to_create.append((opp['Id'], opp['AccountId']))
    
    for opp in opps_via_other_lookups:
        if opp['Id'] not in created_opportunities:
            opps_to_create.append((opp['Id'], opp['AccountId']))
    
    logging.info(f"  Preparing {len(opps_to_create)} Opportunity(ies) for bulk creation")
    
    # Fetch and process all opportunities
    processed_count = 0
    for prod_opp_id, prod_account_id in opps_to_create:
        processed_count += 1
        if processed_count % 10 == 0:
            logging.info(f"  Processing... {processed_count}/{len(opps_to_create)} opportunities prepared")
        
        if prod_account_id not in created_accounts:
            logging.warning(f"  Skipping Opportunity {prod_opp_id} - Account {prod_account_id} not created")
            continue
        
        prod_opp_record = sf_cli_source.get_record('Opportunity', prod_opp_id)
        if not prod_opp_record:
            continue
        
        created_mappings = {
            'Account': created_accounts,
            'Contact': created_contacts,
            'Opportunity': created_opportunities
        }
        record_with_dummies = replace_lookups_with_dummies(
            prod_opp_record, opportunity_fields, dummy_records, created_mappings,
            sf_cli_source, sf_cli_target, 'Opportunity'
        )
        record_with_dummies['AccountId'] = created_accounts[prod_account_id]
        
        # Map all other Account lookup fields to sandbox IDs
        for field_name in account_lookup_fields:
            if field_name != 'AccountId':  # AccountId already set above
                lookup_value = prod_opp_record.get(field_name)
                if lookup_value and lookup_value in created_accounts:
                    record_with_dummies[field_name] = created_accounts[lookup_value]
        
        filtered_data = filter_record_data(record_with_dummies, opportunity_fields, sf_cli_target, 'Opportunity')
        filtered_data.pop('Id', None)
        
        try:
            bulk_creator_opps.add_record('Opportunity', filtered_data)
        except Exception as e:
            logging.warning(f"  Failed to add Opportunity to bulk: {e}")
            create_opportunity_phase1(prod_opp_id, created_opportunities, opportunity_fields,
                                    sf_cli_source, sf_cli_target, dummy_records, script_dir, config, created_accounts, created_contacts)
    
    # Flush all opportunities
    if bulk_creator_opps.get_pending_count('Opportunity') > 0:
        logging.info(f"  Flushing {bulk_creator_opps.get_pending_count('Opportunity')} Opportunity(ies) to Salesforce")
        result_ids = bulk_creator_opps.flush('Opportunity')
        opp_ids = result_ids.get('Opportunity', []) if result_ids else None
        
        if not opp_ids or len(opp_ids) == 0:
            logging.warning("  Bulk creation returned no IDs, falling back to individual creation...")
            for prod_id, _ in opps_to_create:
                if prod_id not in created_opportunities:
                    create_opportunity_phase1(prod_id, created_opportunities, opportunity_fields,
                                            sf_cli_source, sf_cli_target, dummy_records, script_dir, config, created_accounts, created_contacts)
        else:
            for idx, (prod_id, _) in enumerate(opps_to_create):
                if idx < len(opp_ids):
                    created_opportunities[prod_id] = opp_ids[idx]
            logging.info(f"  ✓ Bulk created {len(opp_ids)} Opportunity(ies)")
    
    return created_opportunities


def setup_logging(script_dir):
    """Configure logging for the migration script."""
    if not os.path.exists(script_dir + '/logs'):
        os.makedirs(script_dir + '/logs')
    
    log_file = f"{script_dir}/logs/sandcastle_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    logging.info("=" * 80)
    logging.info("Starting Salesforce Data Population Script (v2 - Optimized)")
    logging.info("=" * 80)
    return log_file


def run_pre_migration_setup(config, sf_cli_source, sf_cli_target, script_dir):
    """
    Execute all pre-migration setup steps: clear CSVs, create dummies,
    pre-fetch picklists, and load metadata.
    Returns tuple: (account_fields, contact_fields, opportunity_fields, quote_fields, 
                    order_fields, case_fields, dummy_records)
    """
    # Step 1: Clear temporary CSV files
    clear_migration_csvs(script_dir)
    
    # Step 2: Create dummy records
    dummy_records = create_dummy_records(sf_cli_target)
    
    # Step 3: Pre-fetch all picklist values
    logging.info("\n--- Pre-fetching Picklist Values ---")
    from picklist_utils import prefetch_picklists_for_object
    try:
        for obj_type in ['Account', 'Contact', 'Opportunity', 'Quote', 'Order', 'Case']:
            prefetch_picklists_for_object(sf_cli_target, obj_type)
        logging.info("✓ Pre-fetched picklist values for both orgs\n")
    except Exception as e:
        logging.warning(f"Could not pre-fetch some picklist values: {e}\n")
    
    # Step 4: Load field metadata for all objects
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
    import time
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description="Two-Phase data migration from production to sandbox.")
    parser.add_argument('-s', '--source-alias', help='Salesforce org alias for the production/source org.')
    parser.add_argument('-t', '--target-alias', help='Salesforce org alias for the sandbox/target org.')
    parser.add_argument('--no-delete', action='store_true', help='Skip the deletion of existing records in the sandbox.')
    args = parser.parse_args()
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Set up logging (both file and console)
    setup_logging(script_dir)
    
    # Delete query log from previous run
    query_log = Path(script_dir) / "logs" / "queries.csv"
    if query_log.exists():
        query_log.unlink()
        logging.info(f"Deleted previous query log")
    
    config_path = os.path.join(script_dir, 'config.json')
    
    # Load config
    if not os.path.exists(config_path):
        logging.error(f"config.json not found at {config_path}")
        return
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Determine source/target aliases
    source_org_alias = args.source_alias or config.get("source_prod_alias")
    target_org_alias = args.target_alias or config.get("target_sandbox_alias")
    
    if not source_org_alias or not target_org_alias:
        logging.error("Source and target org aliases must be provided")
        return
    
    # Initialize CLI
    sf_cli_source = SalesforceCLI(target_org=source_org_alias)
    sf_cli_target = SalesforceCLI(target_org=target_org_alias)
    
    logging.info("\n" + "="*80)
    logging.info("TWO-PHASE DATA MIGRATION")
    logging.info("="*80)
    logging.info(f"Source: {source_org_alias}")
    logging.info(f"Target: {target_org_alias}")
    
    try:
        # Safety checks
        if not sf_cli_target.is_sandbox():
            logging.error(f"\nTarget '{target_org_alias}' is NOT a sandbox. Aborting.")
            return
        
        source_info = sf_cli_source.get_org_info()
        target_info = sf_cli_target.get_org_info()
        
        if source_info and target_info and source_info['instanceUrl'] == target_info['instanceUrl']:
            logging.error(f"\nSource and target are the SAME org. Aborting.")
            return
        
        logging.info(f"✓ Safety checks passed")
        
        # Delete existing records if not skipped
        deletion_skipped_reason = None
        if args.no_delete:
            deletion_skipped_reason = "--no-delete flag was used"
        elif not config.get('delete_existing_records', False):
            deletion_skipped_reason = "delete_existing_records is not set to true in config.json"
        
        if deletion_skipped_reason:
            logging.warning(f"\n⚠ SKIPPING DELETION: {deletion_skipped_reason}")
            logging.warning("⚠ This may cause duplicate errors if records already exist in the target org")
        else:
            logging.info("\n--- Deleting Existing Records ---")
            delete_existing_records(sf_cli_target, args, target_org_alias)
        
        # Run pre-migration setup (clear CSVs, create dummies, load metadata)
        (account_fields, contact_fields, opportunity_fields, quote_fields,
         order_fields, case_fields, dummy_records) = run_pre_migration_setup(
            config, sf_cli_source, sf_cli_target, script_dir
        )
        
        if not account_fields:
            logging.error("No insertable Account fields found")
            return
        
        # Initialize tracking dictionaries
        created_accounts = {}
        created_contacts = {}
        created_opportunities = {}
        created_quotes = {}
        created_qlis = {}
        created_orders = {}
        created_order_items = {}
        created_cases = {}
        created_products = {}
        created_pbes = {}
        created_account_relationships = {}
        
        # ========== PHASE 1: CREATE WITH DUMMY LOOKUPS ==========
        logging.info("\n" + "="*80)
        logging.info("PHASE 1: CREATING RECORDS WITH DUMMY LOOKUPS")
        logging.info("="*80)
        
        if "Accounts" not in config or not config["Accounts"]:
            logging.error("No Account IDs in config.json")
            return
        
        # Call helper function to create all accounts
        created_accounts = create_accounts_phase1(config, account_fields, sf_cli_source, 
                                                  sf_cli_target, dummy_records, script_dir)
        
        # Account Relationships (TEMPORARILY DISABLED - focus on batch optimizations)
        logging.info(f"\n--- Phase 1: Account Relationships ---")
        logging.info("SKIPPING: AccountRelationship creation temporarily disabled")
        
        # Call helper function to create all contacts
        created_contacts = create_contacts_phase1(config, created_accounts, contact_fields,
                                                  sf_cli_source, sf_cli_target, dummy_records, script_dir)
        
        # Call helper function to create all opportunities
        created_opportunities = create_opportunities_phase1_bulk(config, created_accounts, created_contacts,
                                                                opportunity_fields, sf_cli_source, sf_cli_target,
                                                                dummy_records, script_dir)
        
        # Quotes
        logging.info(f"\n--- Phase 1: Quotes ---")
        quote_limit = config.get('quote_limit', 10)
        if quote_limit == 0:
            logging.info("Skipping quotes (limit is 0)")
        else:
            for prod_opp_id in list(created_opportunities.keys()):
                query = f"SELECT Id FROM Quote WHERE OpportunityId = '{prod_opp_id}' ORDER BY CreatedDate DESC"
                if quote_limit != -1:
                    query += f" LIMIT {quote_limit}"
                quotes = sf_cli_source.query_records(query) or []
                for quote in quotes:
                    if quote['Id'] not in created_quotes:
                        create_quote_phase1(quote['Id'], created_quotes, sf_cli_source, sf_cli_target, dummy_records, script_dir, created_accounts, created_contacts, created_opportunities)
        
        # Quote Line Items - OPTIMIZED: Batch creation using Bulk API 2.0
        logging.info(f"\n--- Phase 1: Quote Line Items ---")
        qli_limit = config.get('quote_line_item_limit', 100)
        if qli_limit == 0:
            logging.info("Skipping quote line items (limit is 0)")
        else:
            logging.info("  Using Bulk API 2.0 for QuoteLineItem creation")
            from bulk_utils import BulkRecordCreator
            from record_utils import filter_record_data, replace_lookups_with_dummies
            
            bulk_creator = BulkRecordCreator(sf_cli_target, batch_size=200)
            qli_fields = load_insertable_fields('QuoteLineItem', script_dir)
            
            # Collect all QLIs to create
            qlis_to_create = []
            for prod_quote_id in list(created_quotes.keys()):
                query = f"SELECT Id FROM QuoteLineItem WHERE QuoteId = '{prod_quote_id}' ORDER BY CreatedDate DESC"
                if qli_limit != -1:
                    query += f" LIMIT {qli_limit}"
                qlis = sf_cli_source.query_records(query) or []
                
                for qli in qlis:
                    if qli['Id'] not in created_qlis:
                        prod_qli_record = sf_cli_source.get_record('QuoteLineItem', qli['Id'])
                        if prod_qli_record:
                            qlis_to_create.append((qli['Id'], prod_qli_record))
            
            logging.info(f"  Preparing {len(qlis_to_create)} QuoteLineItem(s) for bulk creation")
            
            # Process and batch all QLIs
            processed_count = 0
            for prod_qli_id, prod_qli_record in qlis_to_create:
                processed_count += 1
                # Progress indicator every 50 records
                if processed_count % 50 == 0:
                    logging.info(f"  Processing... {processed_count}/{len(qlis_to_create)} quote line items prepared")
                # Ensure dependencies
                prod_product_id = prod_qli_record.get('Product2Id')
                if prod_product_id and prod_product_id not in created_products:
                    from create_other_objects_phase1 import create_product2_phase1
                    create_product2_phase1(prod_product_id, created_products, sf_cli_source, sf_cli_target, dummy_records, script_dir)
                
                prod_pbe_id = prod_qli_record.get('PricebookEntryId')
                if prod_pbe_id and prod_pbe_id not in created_pbes:
                    from create_other_objects_phase1 import create_pricebook_entry_phase1
                    create_pricebook_entry_phase1(prod_pbe_id, created_pbes, sf_cli_source, sf_cli_target, dummy_records, script_dir, created_products)
                
                # Build record with proper lookups
                created_mappings = {
                    'Product2': created_products,
                    'PricebookEntry': created_pbes,
                    'Quote': created_quotes,
                    'QuoteLineItem': created_qlis,
                    'Account': created_accounts,
                    'Contact': created_contacts,
                    'Opportunity': created_opportunities
                }
                record_with_dummies = replace_lookups_with_dummies(
                    prod_qli_record, qli_fields, dummy_records, created_mappings,
                    sf_cli_source, sf_cli_target, 'QuoteLineItem'
                )
                
                # Override with real IDs
                prod_quote_id = prod_qli_record.get('QuoteId')
                if prod_quote_id and prod_quote_id in created_quotes:
                    record_with_dummies['QuoteId'] = created_quotes[prod_quote_id]
                
                if prod_product_id and prod_product_id in created_products:
                    record_with_dummies['Product2Id'] = created_products[prod_product_id]
                
                if prod_pbe_id and prod_pbe_id in created_pbes:
                    record_with_dummies['PricebookEntryId'] = created_pbes[prod_pbe_id]
                
                filtered_data = filter_record_data(record_with_dummies, qli_fields, sf_cli_target, 'QuoteLineItem')
                filtered_data.pop('Id', None)
                
                # Handle negative prices
                if 'UnitPrice' in filtered_data and filtered_data['UnitPrice'] is not None:
                    if isinstance(filtered_data['UnitPrice'], (int, float)) and filtered_data['UnitPrice'] < 0:
                        filtered_data['UnitPrice'] = 0.01
                
                # Add to bulk batch
                bulk_creator.add_record('QuoteLineItem', filtered_data)
                # Track for CSV writing
                from csv_utils import write_record_to_csv
                # Note: We'll write after bulk create returns IDs
            
            # Flush all batched QLIs
            if bulk_creator.get_pending_count('QuoteLineItem') > 0:
                logging.info(f"  Flushing {bulk_creator.get_pending_count('QuoteLineItem')} QuoteLineItem(s) to Salesforce")
                try:
                    result_ids = bulk_creator.flush('QuoteLineItem')
                    qli_ids = result_ids.get('QuoteLineItem', [])
                    
                    # Map production IDs to sandbox IDs
                    for idx, (prod_id, _) in enumerate(qlis_to_create):
                        if idx < len(qli_ids):
                            created_qlis[prod_id] = qli_ids[idx]
                    
                    logging.info(f"  ✓ Bulk created {len(qli_ids)} QuoteLineItem(s)")
                except Exception as e:
                    logging.error(f"  ✗ Bulk creation failed for QuoteLineItems: {e}")
                    logging.warning("  Falling back to individual creation...")
                    # Fall back to one-by-one creation
                    for prod_qli_id, _ in qlis_to_create:
                        if prod_qli_id not in created_qlis:
                            create_quote_line_item_phase1(prod_qli_id, created_qlis, sf_cli_source, sf_cli_target, 
                                                        dummy_records, script_dir, created_products, created_pbes, 
                                                        created_quotes, created_accounts, created_contacts, created_opportunities)
        
        # Orders
        logging.info(f"\n--- Phase 1: Orders ---")
        order_limit = config.get('order_limit', 10)
        if order_limit == 0:
            logging.info("Skipping orders (limit is 0)")
        else:
            for prod_quote_id in list(created_quotes.keys()):
                query = f"SELECT Id FROM Order WHERE QuoteId = '{prod_quote_id}' ORDER BY CreatedDate DESC"
                if order_limit != -1:
                    query += f" LIMIT {order_limit}"
                orders = sf_cli_source.query_records(query) or []
                for order in orders:
                    if order['Id'] not in created_orders:
                        create_order_phase1(order['Id'], created_orders, sf_cli_source, sf_cli_target, dummy_records, script_dir, created_accounts, created_contacts)
        
        # Order Items - OPTIMIZED: Batch creation using Bulk API 2.0
        logging.info(f"\n--- Phase 1: Order Items ---")
        order_item_limit = config.get('order_item_limit', 20)
        if order_item_limit == 0:
            logging.info("Skipping order items (limit is 0)")
        else:
            logging.info("  Using Bulk API 2.0 for OrderItem creation")
            from bulk_utils import BulkRecordCreator
            from record_utils import filter_record_data, replace_lookups_with_dummies
            bulk_creator_oi = BulkRecordCreator(sf_cli_target, batch_size=200)
            order_item_fields = load_insertable_fields('OrderItem', script_dir)
            
            # Collect all OrderItems to create
            order_items_to_create = []
            for prod_order_id in list(created_orders.keys()):
                query = f"SELECT Id FROM OrderItem WHERE OrderId = '{prod_order_id}' ORDER BY CreatedDate DESC"
                if order_item_limit != -1:
                    query += f" LIMIT {order_item_limit}"
                order_items = sf_cli_source.query_records(query) or []
                
                for item in order_items:
                    if item['Id'] not in created_order_items:
                        prod_order_item_record = sf_cli_source.get_record('OrderItem', item['Id'])
                        if prod_order_item_record:
                            order_items_to_create.append((item['Id'], prod_order_item_record))
            
            logging.info(f"  Preparing {len(order_items_to_create)} OrderItem(s) for bulk creation")
            
            # Process and batch all OrderItems
            processed_count = 0
            for prod_item_id, prod_item_record in order_items_to_create:
                processed_count += 1
                # Progress indicator every 50 records
                if processed_count % 50 == 0:
                    logging.info(f"  Processing... {processed_count}/{len(order_items_to_create)} order items prepared")
                # Ensure dependencies
                prod_product_id = prod_item_record.get('Product2Id')
                if prod_product_id and prod_product_id not in created_products:
                    from create_other_objects_phase1 import create_product2_phase1
                    create_product2_phase1(prod_product_id, created_products, sf_cli_source, sf_cli_target, dummy_records, script_dir)
                
                prod_pbe_id = prod_item_record.get('PricebookEntryId')
                if prod_pbe_id and prod_pbe_id not in created_pbes:
                    from create_other_objects_phase1 import create_pricebook_entry_phase1
                    create_pricebook_entry_phase1(prod_pbe_id, created_pbes, sf_cli_source, sf_cli_target, dummy_records, script_dir, created_products)
                
                # Build record with proper lookups
                created_mappings = {
                    'Product2': created_products,
                    'PricebookEntry': created_pbes,
                    'Order': created_orders,
                    'OrderItem': created_order_items,
                    'Account': created_accounts,
                    'Contact': created_contacts
                }
                record_with_dummies = replace_lookups_with_dummies(
                    prod_item_record, order_item_fields, dummy_records, created_mappings,
                    sf_cli_source, sf_cli_target, 'OrderItem'
                )
                
                # Override with real IDs
                prod_order_id = prod_item_record.get('OrderId')
                if prod_order_id and prod_order_id in created_orders:
                    record_with_dummies['OrderId'] = created_orders[prod_order_id]
                
                if prod_product_id and prod_product_id in created_products:
                    record_with_dummies['Product2Id'] = created_products[prod_product_id]
                
                if prod_pbe_id and prod_pbe_id in created_pbes:
                    record_with_dummies['PricebookEntryId'] = created_pbes[prod_pbe_id]
                
                filtered_data = filter_record_data(record_with_dummies, order_item_fields, sf_cli_target, 'OrderItem')
                filtered_data.pop('Id', None)
                
                # Handle negative prices
                if 'UnitPrice' in filtered_data and filtered_data['UnitPrice'] is not None:
                    if isinstance(filtered_data['UnitPrice'], (int, float)) and filtered_data['UnitPrice'] < 0:
                        filtered_data['UnitPrice'] = 0.01
                
                # Add to bulk batch
                bulk_creator_oi.add_record('OrderItem', filtered_data)
            
            # Flush all batched OrderItems
            if bulk_creator_oi.get_pending_count('OrderItem') > 0:
                logging.info(f"  Flushing {bulk_creator_oi.get_pending_count('OrderItem')} OrderItem(s) to Salesforce")
                try:
                    result_ids = bulk_creator_oi.flush('OrderItem')
                    item_ids = result_ids.get('OrderItem', [])
                    
                    # Map production IDs to sandbox IDs
                    for idx, (prod_id, _) in enumerate(order_items_to_create):
                        if idx < len(item_ids):
                            created_order_items[prod_id] = item_ids[idx]
                    
                    logging.info(f"  ✓ Bulk created {len(item_ids)} OrderItem(s)")
                except Exception as e:
                    logging.error(f"  ✗ Bulk creation failed for OrderItems: {e}")
                    logging.warning("  Falling back to individual creation...")
                    # Fall back to one-by-one creation
                    for prod_item_id, _ in order_items_to_create:
                        if prod_item_id not in created_order_items:
                            create_order_item_phase1(prod_item_id, created_order_items, sf_cli_source, sf_cli_target,
                                                   dummy_records, script_dir, created_products, created_pbes,
                                                   created_orders, created_accounts, created_contacts)
        
        # Cases
        logging.info(f"\n--- Phase 1: Cases ---")
        case_limit = config.get('case_limit', 5)
        if case_limit == 0:
            logging.info("Skipping cases (limit is 0)")
        else:
            # OPTIMIZED: Batch query all cases for all accounts at once
            account_ids = list(created_accounts.keys())
            logging.info(f"  Fetching cases for {len(account_ids)} account(s) in batches")
            
            all_cases = []
            # Process in batches of 200 (SOQL limit for IN clause)
            for i in range(0, len(account_ids), 200):
                batch = account_ids[i:i+200]
                ids_str = "','".join(batch)
                query = f"SELECT Id, AccountId FROM Case WHERE AccountId IN ('{ids_str}') ORDER BY CreatedDate DESC"
                cases = sf_cli_source.query_records(query) or []
                all_cases.extend(cases)
            
            # Group cases by account and limit per account
            cases_by_account = {}
            for case in all_cases:
                account_id = case['AccountId']
                if account_id not in cases_by_account:
                    cases_by_account[account_id] = []
                cases_by_account[account_id].append(case)
            
            # OPTIMIZED: Bulk create cases using Bulk API 2.0
            logging.info("  Using Bulk API 2.0 for Case creation")
            from bulk_utils import BulkRecordCreator
            from record_utils import filter_record_data, replace_lookups_with_dummies
            bulk_creator_cases = BulkRecordCreator(sf_cli_target, batch_size=200)
            case_fields = load_insertable_fields('Case', script_dir)
            cases_to_create = []
            
            # Prepare cases respecting per-account limits
            for account_id, cases in cases_by_account.items():
                limit_to_apply = case_limit if case_limit != -1 else len(cases)
                for case in cases[:limit_to_apply]:
                    if case['Id'] not in created_cases:
                        cases_to_create.append((case['Id'], account_id))
            
            logging.info(f"  Preparing {len(cases_to_create)} Case(s) for bulk creation")
            
            # Fetch and process all cases
            processed_count = 0
            for prod_case_id, prod_account_id in cases_to_create:
                processed_count += 1
                # Progress indicator every 50 records
                if processed_count % 50 == 0:
                    logging.info(f"  Processing... {processed_count}/{len(cases_to_create)} cases prepared")
                prod_case_record = sf_cli_source.get_record('Case', prod_case_id)
                if not prod_case_record:
                    continue
                
                created_mappings = {
                    'Account': created_accounts,
                    'Contact': created_contacts,
                    'Case': created_cases
                }
                record_with_dummies = replace_lookups_with_dummies(
                    prod_case_record, case_fields, dummy_records, created_mappings,
                    sf_cli_source, sf_cli_target, 'Case'
                )
                
                # Override AccountId with real sandbox ID
                if prod_account_id in created_accounts:
                    record_with_dummies['AccountId'] = created_accounts[prod_account_id]
                
                # Handle ContactId if it exists and is in created contacts
                prod_contact_id = prod_case_record.get('ContactId')
                if prod_contact_id and prod_contact_id in created_contacts:
                    record_with_dummies['ContactId'] = created_contacts[prod_contact_id]
                
                # Handle Location__c if it exists and is in created accounts
                prod_location_id = prod_case_record.get('Location__c')
                if prod_location_id and prod_location_id in created_accounts:
                    record_with_dummies['Location__c'] = created_accounts[prod_location_id]
                
                # Handle Partner_Account__c if it exists and is in created accounts
                prod_partner_account_id = prod_case_record.get('Partner_Account__c')
                if prod_partner_account_id and prod_partner_account_id in created_accounts:
                    record_with_dummies['Partner_Account__c'] = created_accounts[prod_partner_account_id]
                
                # Handle Partner_Contact__c if it exists and is in created contacts
                prod_partner_contact_id = prod_case_record.get('Partner_Contact__c')
                if prod_partner_contact_id and prod_partner_contact_id in created_contacts:
                    record_with_dummies['Partner_Contact__c'] = created_contacts[prod_partner_contact_id]
                
                filtered_data = filter_record_data(record_with_dummies, case_fields, sf_cli_target, 'Case')
                filtered_data.pop('Id', None)
                
                bulk_creator_cases.add_record('Case', filtered_data)
            
            # Flush all cases
            if bulk_creator_cases.get_pending_count('Case') > 0:
                logging.info(f"  Flushing {bulk_creator_cases.get_pending_count('Case')} Case(s) to Salesforce")
                try:
                    result_ids = bulk_creator_cases.flush('Case')
                    case_ids = result_ids.get('Case', [])
                    
                    # Map production IDs to sandbox IDs
                    for idx, (prod_id, _) in enumerate(cases_to_create):
                        if idx < len(case_ids):
                            created_cases[prod_id] = case_ids[idx]
                    
                    logging.info(f"  ✓ Bulk created {len(case_ids)} Case(s)")
                except Exception as e:
                    logging.error(f"  ✗ Bulk creation failed for Cases: {e}")
                    logging.warning("  Falling back to individual creation...")
                    for prod_id, _ in cases_to_create:
                        if prod_id not in created_cases:
                            create_case_phase1(prod_id, created_cases, sf_cli_source, sf_cli_target, 
                                             dummy_records, script_dir, created_accounts, created_contacts)
        
        # ========== PHASE 2: UPDATE LOOKUPS ==========
        logging.info("\n" + "="*80)
        logging.info("PHASE 2: UPDATING LOOKUPS WITH ACTUAL RELATIONSHIPS")
        logging.info("="*80)
        
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
        
        # AccountRelationship Phase 2 update (TEMPORARILY DISABLED)
        # if created_account_relationships:
        #     relationship_fields = load_insertable_fields('AccountRelationship', script_dir)
        #     update_lookups_phase2(sf_cli_source, sf_cli_target, script_dir, relationship_fields, created_mappings, 'AccountRelationship', dummy_records)
        
        # Note: Product2 and PricebookEntry are reused from production, not migrated via CSV
        # They don't need Phase 2 updates
        
        # ========== SUMMARY ==========
        logging.info("\n" + "="*80)
        logging.info("MIGRATION SUMMARY")
        logging.info("="*80)
        
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
                logging.info(f"  {obj_type:<20} {count:>6} record(s)")
        
        logging.info(f"  {'-'*28}")
        logging.info(f"  {'TOTAL':<20} {total:>6} record(s)")
        logging.info("="*80)
        
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
        
        logging.info(f"\nTotal execution time: {time_str}")
        logging.info("\nNote: Counts include both newly created and existing/reused records.")
        logging.info("Check output above for specific errors and warnings.")
        logging.info(f"\nQuery log available at: {Path(script_dir) / 'logs' / 'queries.csv'}")
        logging.info("="*80)
        
    except RuntimeError as e:
        logging.error(f"\nCLI Error: {e}")
    except Exception as e:
        logging.error(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
    else:
        logging.info("\n✓ Data migration completed successfully!")


if __name__ == "__main__":
    main()
