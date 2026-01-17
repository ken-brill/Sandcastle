#!/usr/bin/env python3
"""
Guest User Contact Creation

Author: Ken Brill
Version: 1.1.8
Date: December 24, 2025
License: MIT License

Creates Contact/User pairs for guest portal access required by AccountRelationships.
Each account involved in AccountRelationships needs at least one Contact with an associated guest User.
"""
import random
import string

# Track which accounts already have guest user contacts
_accounts_with_guest_users = set()

def generate_random_string(length=10):
    """Generate a random alphanumeric string"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def ensure_guest_user_contact(account_id, sf_cli_target, created_contacts, script_dir):
    """
    Ensures an account has at least one Contact with an associated guest User.
    Required for AccountRelationship functionality.
    
    Args:
        account_id: Sandbox Account ID that needs a guest user contact
        sf_cli_target: Target org CLI instance
        created_contacts: Dictionary tracking created contacts
        script_dir: Script directory
        
    Returns:
        str: Contact ID of the guest user contact, or None if failed
    """
    global _accounts_with_guest_users
    
    # Skip if we already created a guest user for this account
    if account_id in _accounts_with_guest_users:
        print(f"  [GUEST USER] Account {account_id} already has guest user contact")
        return None
    
    # Check if account already has a guest user contact
    try:
        query = f"""
            SELECT Id FROM Contact 
            WHERE AccountId = '{account_id}' 
            AND Sangoma_Portal_Access__c = true 
            LIMIT 1
        """
        existing = sf_cli_target.query_records(query)
        if existing and len(existing) > 0:
            contact_id = existing[0]['Id']
            print(f"  [GUEST USER] Account {account_id} already has guest user contact: {contact_id}")
            _accounts_with_guest_users.add(account_id)
            return contact_id
    except Exception as e:
        print(f"  [WARN] Could not check for existing guest user: {e}")
    
    # Create new guest user contact
    print(f"  [GUEST USER] Creating guest user Contact for Account {account_id}")
    
    # Generate random data for Sangoma fields
    portal_id = random.randint(1000, 9999)
    
    # Get account info for contact name
    try:
        account_query = f"SELECT Name FROM Account WHERE Id = '{account_id}'"
        account_result = sf_cli_target.query_records(account_query)
        if account_result and len(account_result) > 0:
            account_name = account_result[0].get('Name', 'Portal User')
        else:
            account_name = 'Portal User'
    except Exception:
        account_name = 'Portal User'
    
    # Create Contact
    contact_data = {
        'AccountId': account_id,
        'FirstName': 'Guest',
        'LastName': f'{account_name[:30]} Portal',  # Truncate to avoid length issues
        'Email': f'guestuser{portal_id}@portal.sandbox.com',
        'Sangoma_Portal_Access__c': True,
        'Sangoma_Portal_ID__c': str(portal_id)
    }
    
    try:
        contact_id = sf_cli_target.create_record('Contact', contact_data)
        if not contact_id:
            print(f"  [ERROR] Failed to create guest user contact for Account {account_id}")
            return None
        
        print(f"  [GUEST USER] Created Contact {contact_id} for Account {account_id}")
        
        # Track the contact
        created_contacts[f"GUEST_{account_id}"] = contact_id
        _accounts_with_guest_users.add(account_id)
        
        # Create associated User
        username = f"guestuser{portal_id}@website.sandbox.com"
        
        # Get a Profile with 'Overage Customer Portal Manager Standard' license
        # First try to find the exact profile that was used in the example
        profile_query = """
            SELECT Id, Name, UserLicense.Name 
            FROM Profile 
            WHERE UserLicense.Name = 'Overage Customer Portal Manager Standard' 
            LIMIT 1
        """
        try:
            profile_result = sf_cli_target.query_records(profile_query)
            if not profile_result or len(profile_result) == 0:
                print(f"  [ERROR] Could not find profile with 'Overage Customer Portal Manager Standard' license")
                # Try alternate query without UserLicense.Name
                profile_query = "SELECT Id FROM Profile WHERE Name = 'Guest User - Public Portals' LIMIT 1"
                profile_result = sf_cli_target.query_records(profile_query)
                if not profile_result or len(profile_result) == 0:
                    print(f"  [ERROR] Could not find any guest portal profile")
                    return contact_id  # Return contact ID even if User creation fails
            
            profile_id = profile_result[0]['Id']
            profile_name = profile_result[0].get('Name', 'Unknown')
            print(f"  [GUEST USER] Using Profile: {profile_name} (ID: {profile_id})")
        except Exception as e:
            print(f"  [ERROR] Error finding guest portal profile: {e}")
            return contact_id
        
        # Create User record
        user_data = {
            'ContactId': contact_id,
            'Username': username,
            'Email': contact_data['Email'],
            'ProfileId': profile_id,
            'LastName': contact_data['LastName'],
            'Alias': f"gst{portal_id}"[:8],  # Max 8 chars
            'TimeZoneSidKey': 'America/New_York',
            'LocaleSidKey': 'en_US',
            'EmailEncodingKey': 'UTF-8',
            'LanguageLocaleKey': 'en_US',
            'UserType': 'PowerCustomerSuccess'
        }
        
        try:
            user_id = sf_cli_target.create_record('User', user_data)
            if user_id:
                print(f"  [GUEST USER] Created User {user_id} for Contact {contact_id}")
            else:
                print(f"  [WARN] Failed to create User for Contact {contact_id}, but Contact exists")
        except Exception as e:
            print(f"  [WARN] Error creating User for Contact {contact_id}: {e}")
            print(f"  [INFO] Contact {contact_id} exists but User creation failed - may need manual creation")
        
        return contact_id
        
    except Exception as e:
        print(f"  [ERROR] Failed to create guest user contact: {e}")
        return None

def clear_guest_user_cache():
    """Clear the cache of accounts with guest users (call at start of migration)"""
    global _accounts_with_guest_users
    _accounts_with_guest_users.clear()
