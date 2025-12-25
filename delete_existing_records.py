#!/usr/bin/env python3
"""
Delete Existing Records with Portal User Protection

Author: Ken Brill
Version: 1.0
Date: December 24, 2025
License: MIT License
"""

def delete_existing_records(sf_cli_target, args, target_org_alias):
    """
    Deletes all demo data records from the target org, unless --no-delete is specified.
    This includes: Cases, OrderItems, Orders, QuoteLineItems, Quotes, Opportunities, Contacts, Accounts, and AccountRelationships.
    """
    # CRITICAL SAFETY CHECK: Verify this is a sandbox, not production
    print("\n🔒 SAFETY CHECK: Verifying target org is a sandbox...")
    try:
        org_info = sf_cli_target.query_records("SELECT IsSandbox, Name, OrganizationType FROM Organization LIMIT 1")
        if not org_info or len(org_info) == 0:
            raise RuntimeError("❌ SAFETY CHECK FAILED: Could not retrieve Organization information")
        
        org = org_info[0]
        is_sandbox = org.get('IsSandbox', False)
        org_name = org.get('Name', 'Unknown')
        org_type = org.get('OrganizationType', 'Unknown')
        
        print(f"   Org Name: {org_name}")
        print(f"   Org Type: {org_type}")
        print(f"   Is Sandbox: {is_sandbox}")
        
        if not is_sandbox:
            print("\n" + "="*80)
            print("❌ CRITICAL ERROR: PRODUCTION ORG DETECTED")
            print("="*80)
            print(f"Target org '{org_name}' (Type: {org_type}) is NOT a sandbox!")
            print("This function is designed to delete data ONLY from sandbox environments.")
            print("Deletion has been BLOCKED to protect production data.")
            print("="*80 + "\n")
            raise RuntimeError("SAFETY ABORT: Attempted deletion on production org. Operation blocked.")
        
        print("✅ Safety check passed: Confirmed sandbox environment\n")
        
    except RuntimeError:
        # Re-raise RuntimeError (our safety block)
        raise
    except Exception as e:
        print(f"❌ SAFETY CHECK FAILED: Could not verify org type: {e}")
        print("Aborting deletion as a safety precaution.")
        raise RuntimeError(f"Could not verify target org is a sandbox: {e}")
    
    if args.no_delete:
        print("\n--- Skipping deletion of existing demo records as per --no-delete flag ---")
        return

    print(f"\nThis script will now delete all demo data from the target sandbox org: {target_org_alias}.")
    print("This includes Cases, OrderItems, Orders, QuoteLineItems, Quotes, Opportunities, Contacts, Accounts, and AccountRelationships.")
    print("This operation cannot be undone. To skip, re-run the script with the --no-delete flag.\n")

    # Step 1: Identify Accounts/Contacts with portal users (they cannot be deleted)
    print("Checking for portal users...")
    portal_account_ids = set()
    portal_contact_ids = set()
    try:
        # Query for portal users to find their associated Contacts and Accounts
        portal_users_query = "SELECT Id, Username, ContactId, Contact.AccountId FROM User WHERE ContactId != null"
        portal_users = sf_cli_target.query_records(portal_users_query)
        
        if portal_users and len(portal_users) > 0:
            print(f"Found {len(portal_users)} portal user(s)")
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
                print(f"  Portal user: {user.get('Username', user['Id'])} (Contact: {contact_id})")
            
            print(f"⚠ Found {len(portal_contact_ids)} Contact(s) and {len(portal_account_ids)} Account(s) with portal users")
            print(f"⚠ These records CANNOT be deleted and will be REUSED during migration")
        else:
            print("No portal users found")
    except Exception as e:
        print(f"Warning: Could not query portal users: {e}")
        print("Continuing with deletion...")

    # Step 2: Delete records in proper order (excluding portal-protected records)
    # Deletion order: Case, OrderItem, Order, QuoteLineItem, Quote, Opportunity, Contact, AccountRelationship, Account
    # AccountRelationship must be deleted before Accounts since it references them
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
            print(f"Deleting all {obj} records (excluding {len(excluded_ids)} with portal users)...")
        elif obj == 'Account' and portal_account_ids:
            excluded_ids = portal_account_ids
            print(f"Deleting all {obj} records (excluding {len(excluded_ids)} with portal users)...")
        else:
            print(f"Deleting all {obj} records...")
        
        if not sf_cli_target.bulk_delete_all_records(obj, excluded_ids):
            print(f"Failed to delete existing {obj} records. Aborting.")
            raise RuntimeError(f"Failed to delete existing {obj} records.")
    print("All demo data deletion complete.\n")
