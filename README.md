# 🏰 SandCastle

**Build your perfect Salesforce sandbox with real production data**

SandCastle is an intelligent data migration tool that copies production Salesforce data into development sandboxes while preserving all relationships, handling dependencies, and respecting org-specific configurations.

## ✨ Features

- **Two-Phase Migration**: Creates records with dummy lookups first, then updates relationships to avoid circular dependencies
- **Bulk API Optimization**: Uses Salesforce Bulk API 2.0 for 50-100x faster data loading
- **Smart Relationship Mapping**: Automatically discovers and maps all lookup relationships
- **RecordType Intelligence**: Maps RecordTypes by DeveloperName across orgs
- **Portal User Handling**: Detects and preserves portal users that can't be deleted
- **Configurable Limits**: Control how many records to migrate per object type
- **Comprehensive Logging**: Detailed logs with query tracking and execution time
- **Picklist Validation**: Pre-fetches and validates picklist values before insertion
- **CSV Tracking**: Exports all migrations to CSV for auditing

## 🚀 Quick Start

### ⚠️ CRITICAL: Source and Target Requirements

**Source Org (Production/Full Sandbox):**
- ✅ Can be a **Production** instance
- ✅ Can be a **Full Sandbox**
- Used as read-only source for data extraction
- No modifications made to source org

**Target Org (MUST BE SANDBOX):**
- ❌ **CANNOT be a Production instance**
- ✅ **MUST be a Development Sandbox** or other non-production sandbox
- Target org will have data **deleted and recreated**
- Built-in safety checks prevent accidental production deletions
- The `delete_existing_records` function includes multiple safety validations:
  - Queries Organization object to verify `IsSandbox = true`
  - Blocks execution if production org is detected
  - Fails safe if org type cannot be verified

**Why This Matters:**
This tool is designed to populate development sandboxes with production-like data for testing and development. The deletion step removes existing data before migration. Multiple safety checks ensure this ONLY happens in sandbox environments.

### Prerequisites

- Python 3.8+
- Salesforce CLI (`sf`) installed and authenticated
- Access to both source (production/full sandbox) and target (development sandbox) orgs
- **Target org MUST be a sandbox** (not production)

### Installation

**Option 1: Using pipx (Recommended for macOS/Linux)**
```bash
pipx install git+https://github.com/ken-brill/Sandcastle.git
```
To update later:
```bash
pipx upgrade sandcastle-salesforce
```

**Option 2: Using pip3**
```bash
pip3 install --user git+https://github.com/ken-brill/Sandcastle.git
```
To update later:
```bash
pip3 install --user --upgrade git+https://github.com/ken-brill/Sandcastle.git
```

**Option 3: Virtual Environment (Recommended for Windows)**
```bash
git clone https://github.com/ken-brill/Sandcastle.git
cd Sandcastle
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e .
```

**Option 4: Development (Editable Install)**
```bash
git clone https://github.com/ken-brill/Sandcastle.git
cd Sandcastle
pip3 install --user -e .
# Changes to code are immediately active - no reinstall needed
```

### Salesforce CLI Authentication

Authenticate with both source and target orgs:
```bash
sf org login web --alias PROD
sf org login web --alias DEV_SANDBOX
```

### Configuration

Create a `config.json` file in your current directory (or use `--config path/to/config.json`):
```json
{
  "source_prod_alias": "PROD",
  "target_sandbox_alias": "DEV_SANDBOX",
  "delete_existing_records": true,
  "Accounts": ["0014U00003NPdH5QAL", "0014U00002qXYUJQA4"],
  "opportunity_bypass_record_type_id": "012Sv000003wIinIAE",
  "contact_limit": 20,
  "opportunity_limit": 20,
  "quote_limit": 20,
  "case_limit": 20,
  "order_limit": 20
}
```

Configure object limits:
- Set to `-1` for unlimited records
- Set to `0` to skip that object type
- Set to a number to limit records per account

### Usage

**If installed via pip:**
```bash
sandcastle
```

**With options:**
```bash
sandcastle --config my-config.json
sandcastle --no-delete
sandcastle -s PROD -t MY_SANDBOX
```

**If running from source (development):**
```bash
python -m sandcastle_pkg
```

## 🏗️ How It Works

### Phase 1: Create Records with Dummy Lookups

1. **Query Production Data**: Fetches Accounts and all related records in batches
2. **Replace Lookups**: Temporarily replaces lookup relationships with dummy IDs
3. **Bulk Create**: Uses Bulk API 2.0 to create all records rapidly
4. **Track Mappings**: Maintains production ID → sandbox ID mappings

**Objects Created (in order):**
- Accounts (root + locations/partners)
- Contacts
- Opportunities
- Quotes → Quote Line Items
- Orders → Order Items
- Cases

### Phase 2: Update Actual Relationships

1. **Restore Lookups**: Updates all dummy IDs with real sandbox IDs
2. **Bulk Update**: Uses Bulk API 2.0 for mass updates
3. **RecordType Mapping**: Maps RecordTypes by DeveloperName
4. **Relationship Validation**: Ensures all relationships are properly established

## ⚡ Performance Optimizations

- **Bulk API 2.0**: 50-100x faster than individual API calls
- **Batch Queries**: Single SOQL query fetches all related records
- **Picklist Pre-fetching**: Validates picklists once instead of per record
- **Dynamic Field Discovery**: Automatically finds all lookup fields
- **Smart Deletion**: Skips portal-protected records
- **Parallel Processing**: Batches independent operations

**Typical Performance:**
- 500 Accounts: ~2-3 minutes
- 2,500 Accounts: ~15-20 minutes
- 50-120x faster than traditional record-by-record migration

## 📊 Migration Summary

After completion, you'll see a summary like:
```
================================================================================
MIGRATION SUMMARY
================================================================================
  Accounts              2582 record(s)
  Contacts                50 record(s)
  Opportunities           40 record(s)
  Quotes                  15 record(s)
  Quote Line Items       120 record(s)
  Orders                  10 record(s)
  Order Items             85 record(s)
  Cases                   25 record(s)
  ----------------------------
  TOTAL                 2927 record(s)
================================================================================

Total execution time: 18m 42s
```

## 🔧 Configuration Options

### config.json Settings

| Setting | Description | Default |
|---------|-------------|---------|
| `source_prod_alias` | Production org alias | Required |
| `target_sandbox_alias` | Sandbox org alias | Required |
| `delete_existing_records` | Delete data before migration | `false` |
| `Accounts` | Array of root Account IDs | Required |
| `opportunity_bypass_record_type_id` | Bypass RecordType ID for Opportunities | Optional |
| `contact_limit` | Max contacts per account | `10` |
| `opportunity_limit` | Max opportunities per account | `10` |
| `quote_limit` | Max quotes per opportunity | `10` |
| `case_limit` | Max cases per account | `5` |
| `order_limit` | Max orders per quote | `10` |
| `locations_limit` | Max location accounts | `25` |

### Special RecordType Handling

**Opportunities** use a bypass RecordTypeId during Phase 1 to avoid triggering flows on creation. The real RecordTypeId is restored in Phase 2 (flows don't trigger on updates).

**All other objects** get their RecordTypeId mapped by DeveloperName during Phase 1.

## 📁 Project Structure

```
sfDemoRecords/
├── Sandcastle/
│   ├── sandcastle.py                # Main migration script
│   ├── config.json                  # Configuration file
│   ├── salesforce_cli.py            # Salesforce CLI wrapper
│   ├── bulk_utils.py                # Bulk API utilities
│   ├── record_utils.py              # Record transformation
│   ├── delete_existing_records.py   # Pre-migration cleanup
│   ├── create_account_phase1.py     # Account creation
│   ├── create_contact_phase1.py     # Contact creation
│   ├── create_opportunity_phase1.py # Opportunity creation
│   ├── create_other_objects_phase1.py # Quote/Order/Case creation
│   ├── update_lookups_phase2.py     # Phase 2 updates
│   ├── dummy_records.py             # Dummy record creation
│   ├── picklist_utils.py            # Picklist validation
│   ├── csv_utils.py                 # CSV export utilities
│   └── logs/                        # Migration logs
└── README.md
```

## 🐛 Troubleshooting

### "Duplicate value found" errors
- Ensure `delete_existing_records: true` in config.json
- Portal users may block deletion - they'll be preserved and reused

### "RecordType not found" errors
- RecordTypes must exist in both orgs with matching DeveloperNames
- For Opportunities, set `opportunity_bypass_record_type_id` in config

### "Insufficient access rights" errors
- Ensure your Salesforce user has Create/Edit permissions on all objects
- Some portal user operations may fail (expected behavior)

### Slow performance
- Check network connectivity to Salesforce
- Verify Bulk API limits aren't being hit
- Review logs for individual slow queries

### Missing relationships after migration
- Check Phase 2 logs for update errors
- Verify all referenced records were created in Phase 1
- Some lookups may be intentionally skipped (documented in logs)

## 📝 Logs and Debugging

Logs are stored in `Sandcastle/logs/`:
- `sandcastle_YYYYMMDD_HHMMSS.log` - Full migration log
- `queries.csv` - All SOQL queries executed

Enable detailed debugging in the log files to troubleshoot issues.

## 🤝 Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes with clear commit messages
4. Submit a pull request

## 📄 License

[Your license here]

## 🙏 Acknowledgments

Built with:
- Salesforce CLI
- Salesforce Bulk API 2.0
- Python 3

---

**Built with ❤️ for Salesforce developers who need real data in their sandboxes**
