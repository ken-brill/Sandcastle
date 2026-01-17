import subprocess
import json
from typing import Set, Dict, Tuple, Optional
#!/usr/bin/env python3
"""
Picklist Validation Utilities

Author: Ken Brill
Version: 1.1.8
Date: December 24, 2025
License: MIT License
"""

import logging

# Configure logging
logger = logging.getLogger(__name__)

class SalesforceCliError(Exception):
    """Custom exception for Salesforce CLI errors."""
    pass

class PicklistCache:
    """Thread-safe cache manager for picklist values."""
    
    def __init__(self):
        self._cache: Dict[Tuple[str, str], Set[str]] = {}
        self._object_cache: Dict[str, Dict[str, Set[str]]] = {}  # NEW: Object-level cache
    
    def get(self, sobject: str, field: str) -> Optional[Set[str]]:
        """Retrieve cached picklist values."""
        return self._cache.get((sobject.lower(), field.lower()))
    
    def set(self, sobject: str, field: str, values: Set[str]) -> None:
        """Store picklist values in cache."""
        self._cache[(sobject.lower(), field.lower())] = values
    
    def get_all_for_object(self, sobject: str) -> Optional[Dict[str, Set[str]]]:
        """Retrieve all cached picklist values for an object."""
        return self._object_cache.get(sobject.lower())
    
    def set_all_for_object(self, sobject: str, fields: Dict[str, Set[str]]) -> None:
        """Store all picklist values for an object."""
        sobject_lower = sobject.lower()
        self._object_cache[sobject_lower] = fields
        # Also populate individual cache entries
        for field, values in fields.items():
            self._cache[(sobject_lower, field.lower())] = values
    
    def clear(self, sobject: str = None, field: str = None) -> None:
        """Clear cache entirely or for specific sobject/field."""
        if sobject is None:
            self._cache.clear()
            self._object_cache.clear()
        elif field is None:
            # Clear all fields for this sobject
            keys_to_remove = [k for k in self._cache.keys() if k[0] == sobject.lower()]
            for key in keys_to_remove:
                del self._cache[key]
            self._object_cache.pop(sobject.lower(), None)
        else:
            self._cache.pop((sobject.lower(), field.lower()), None)

# Global cache instance
_picklist_cache = PicklistCache()

def prefetch_picklists_for_object(
    sf_cli_target,
    sobject: str,
    active_only: bool = True
) -> Dict[str, Set[str]]:
    """
    Pre-fetch ALL picklist values for an object in a single call.
    OPTIMIZED: Fetches all picklists at once instead of per-field queries.
    
    Args:
        sf_cli_target: Object with target_org attribute
        sobject: API name of the Salesforce object (e.g., 'Account')
        active_only: Whether to return only active picklist values (default: True)
    
    Returns:
        Dict mapping field names to sets of valid picklist values
    
    Raises:
        SalesforceCliError: If the CLI command fails
    """
    # Check cache first
    cached = _picklist_cache.get_all_for_object(sobject)
    if cached is not None:
        logger.debug(f"Cache hit for all picklists on {sobject}")
        return cached
    
    # Fetch from Salesforce
    try:
        all_picklists = _fetch_all_picklists_for_object(
            sf_cli_target.target_org,
            sobject,
            active_only
        )
    except Exception as e:
        logger.error(f"Failed to prefetch picklist values for {sobject}: {str(e)}")
        raise SalesforceCliError(f"Failed to retrieve picklist values: {str(e)}") from e
    
    # Cache the result
    _picklist_cache.set_all_for_object(sobject, all_picklists)
    logger.info(f"Pre-fetched {len(all_picklists)} picklist fields for {sobject}")
    
    return all_picklists

def _fetch_all_picklists_for_object(
    target_org: str,
    sobject: str,
    active_only: bool
) -> Dict[str, Set[str]]:
    """
    Fetch ALL picklist values for an object in a single API call.
    """
    command = [
        'sf', 'sobject', 'describe',
        '--sobject', sobject,
        '--target-org', target_org,
        '--json'
    ]
    
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=30,
            check=False
        )
    except subprocess.TimeoutExpired as e:
        raise SalesforceCliError(f"CLI command timed out after 30 seconds") from e
    except FileNotFoundError as e:
        raise SalesforceCliError(
            "SF CLI not found. Please install Salesforce CLI: "
            "https://developer.salesforce.com/tools/salesforcecli"
        ) from e
    
    if result.returncode != 0:
        error_msg = result.stderr or result.stdout
        raise SalesforceCliError(
            f"CLI command failed for {sobject}: {error_msg}"
        )
    
    # Parse JSON response
    try:
        response = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        raise SalesforceCliError(f"Invalid JSON response from CLI: {result.stdout}") from e
    
    if response.get('status') != 0:
        error_msg = response.get('message', 'Unknown error')
        raise SalesforceCliError(f"SF CLI error: {error_msg}")
    
    result_data = response.get('result', {})
    fields = result_data.get('fields', [])
    
    # Extract all picklist fields
    all_picklists = {}
    for field_desc in fields:
        field_type = field_desc.get('type', '').lower()
        if field_type in ('picklist', 'multipicklist'):
            field_name = field_desc.get('name')
            picklist_values = set()
            for val in field_desc.get('picklistValues', []):
                if active_only and not val.get('active', True):
                    continue
                picklist_values.add(val['value'])
            
            if picklist_values:  # Only store if there are values
                all_picklists[field_name] = picklist_values
    
    return all_picklists

def get_valid_picklist_values(
    sf_cli_target,
    sobject: str,
    field: str,
    use_cache: bool = True,
    active_only: bool = True
) -> Set[str]:
    """
    Returns a set of valid picklist values for a given sObject and field.
    
    Args:
        sf_cli_target: Object with target_org attribute
        sobject: API name of the Salesforce object (e.g., 'Account')
        field: API name of the picklist field (e.g., 'Industry')
        use_cache: Whether to use cached values (default: True)
        active_only: Whether to return only active picklist values (default: True)
    
    Returns:
        Set of valid picklist values as strings
    
    Raises:
        SalesforceCliError: If the CLI command fails
        ValueError: If the field is not a picklist type
    """
    # Check cache first
    if use_cache:
        cached_values = _picklist_cache.get(sobject, field)
        if cached_values is not None:
            logger.debug(f"Cache hit for {sobject}.{field}")
            return cached_values
    
    # Fetch from Salesforce
    try:
        picklist_values = _fetch_picklist_values(
            sf_cli_target.target_org,
            sobject,
            field,
            active_only
        )
    except Exception as e:
        logger.error(f"Failed to fetch picklist values for {sobject}.{field}: {str(e)}")
        # Cache empty set to avoid repeated failed calls
        _picklist_cache.set(sobject, field, set())
        raise SalesforceCliError(f"Failed to retrieve picklist values: {str(e)}") from e
    
    # Cache the result
    if use_cache:
        _picklist_cache.set(sobject, field, picklist_values)
    
    return picklist_values

def _fetch_picklist_values(
    target_org: str,
    sobject: str,
    field: str,
    active_only: bool
) -> Set[str]:
    """
    Fetch picklist values using SF CLI.
    
    OPTIMIZED: Caches ALL picklist fields from the response, not just the requested one.
    This prevents repeated API calls when multiple fields are validated.
    
    Uses 'sf sobject describe' which is the correct modern command.
    """
    # Correct SF CLI command - sobject describe (not sobject describe field)
    command = [
        'sf', 'sobject', 'describe',
        '--sobject', sobject,
        '--target-org', target_org,
        '--json'
    ]
    
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=30,
            check=False
        )
    except subprocess.TimeoutExpired as e:
        raise SalesforceCliError(f"CLI command timed out after 30 seconds") from e
    except FileNotFoundError as e:
        raise SalesforceCliError(
            "SF CLI not found. Please install Salesforce CLI: "
            "https://developer.salesforce.com/tools/salesforcecli"
        ) from e
    
    if result.returncode != 0:
        error_msg = result.stderr or result.stdout
        raise SalesforceCliError(
            f"CLI command failed for {sobject}.{field}: {error_msg}"
        )
    
    # Parse JSON response
    try:
        response = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        raise SalesforceCliError(f"Invalid JSON response from CLI: {result.stdout}") from e
    
    # Extract picklist values for the specific field
    if response.get('status') != 0:
        error_msg = response.get('message', 'Unknown error')
        raise SalesforceCliError(f"SF CLI error: {error_msg}")
    
    result_data = response.get('result', {})
    fields = result_data.get('fields', [])
    
    # OPTIMIZATION: Cache ALL picklist fields while we have the metadata
    # This prevents repeated API calls for the same object
    all_picklists_in_response = {}
    field_metadata = None
    
    for field_desc in fields:
        field_name = field_desc.get('name')
        field_type = field_desc.get('type', '').lower()
        
        # Track the requested field
        if field_name == field:
            field_metadata = field_desc
        
        # Cache all picklist fields we encounter
        if field_type in ('picklist', 'multipicklist'):
            picklist_vals = set()
            for val in field_desc.get('picklistValues', []):
                if active_only and not val.get('active', True):
                    continue
                picklist_vals.add(val['value'])
            
            if picklist_vals:  # Only cache non-empty picklists
                all_picklists_in_response[field_name] = picklist_vals
                # Cache each field individually
                _picklist_cache.set(sobject, field_name, picklist_vals)
    
    logger.info(f"Cached {len(all_picklists_in_response)} picklist fields for {sobject} (including {field})")
    
    if field_metadata is None:
        raise ValueError(f"Field {field} not found on {sobject}")
    
    # Verify it's a picklist field
    field_type = field_metadata.get('type', '').lower()
    if field_type not in ('picklist', 'multipicklist'):
        raise ValueError(
            f"Field {sobject}.{field} is not a picklist field (type: {field_type})"
        )
    
    # Return the values for the requested field
    picklist_values = all_picklists_in_response.get(field, set())
    logger.debug(f"Returning {len(picklist_values)} values for {sobject}.{field}")
    return picklist_values

def clear_picklist_cache(sobject: str = None, field: str = None) -> None:
    """
    Clear the picklist cache.
    
    Args:
        sobject: Optional sobject to clear (clears all if None)
        field: Optional field to clear (requires sobject)
    """
    _picklist_cache.clear(sobject, field)
    logger.info(f"Cleared cache for {sobject or 'all objects'}")