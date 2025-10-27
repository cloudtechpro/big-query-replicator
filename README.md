# BigQuery Replicator

`big-query-replicator.py` replicates datasets, tables, views, materialized views and routines from one BigQuery project to another. It is intended for migrations or for keeping environments in sync while allowing fine grained control over what resources are copied.

## Features
- **Dataset planning and creation** – every dataset selected from the source project is created in the destination with matching metadata (location, labels, encryption, expiration settings, and access entries).
- **Table management** – managed and external tables are pre-created with full schema and configuration. Data is copied separately with table-level IAM policies.
- **View and materialized view recreation** – SQL is rewritten so that references to migrated datasets point to the destination project, with intelligent handling of missing dependencies.
- **Routine replication** – SQL routines (functions, procedures) are rewritten similarly and skipped if they reference tables listed in `SKIP_REFS`.
- **Selective replication** – choose datasets with explicit IDs or regular expressions and exclude others.
- **Reference skipping** – use `SKIP_REFS` to keep specific `dataset.table` references pointing at the source project (supports both `dataset.table` and `project.dataset.table` formats).
- **Graceful error handling** – automatically skips views, materialized views, and tables that reference missing dependencies or temporary tables (e.g., Looker PDTs).
- **Dry run mode** – preview all operations without making changes.
- **External table support** – replicates external tables with BigQuery connections (configurable to skip if connections are missing).
- **Optional Data Catalog support** – replicate policy tags, resource access policies, and tag templates.
- **12-factor app design** – stateless operation with configuration via environment variables or CLI arguments.

## Requirements
- Python 3.8+
- Google Cloud credentials with access to the source and destination projects
- Python packages:
  - `google-cloud-bigquery`
  - `google-api-python-client`
  - `google-cloud-bigquery-connection`
  - `google-cloud-datacatalog` (required only when replicating Data Catalog resources)

Install dependencies with:

```bash
pip install google-cloud-bigquery google-api-python-client \
    google-cloud-bigquery-connection google-cloud-datacatalog
```

## Configuration
The script follows twelve-factor principles: every option is available as a command line flag and as an environment variable. Command line flags take precedence.

### Required
- `--src-project` / `SRC_PROJECT` – source project ID
- `--dst-project` / `DST_PROJECT` – destination project ID

### Dataset selection
- `--datasets` / `DATASETS` – comma-separated list of dataset IDs to include
- `--include-regex` / `INCLUDE_REGEX` – regular expression to include dataset IDs (default `.*`)
- `--exclude-regex` / `EXCLUDE_REGEX` – regular expression to exclude dataset IDs

### Replication behaviour
- `--rewrite-project-refs` / `REWRITE_PROJECT_REFS` – rewrite project references in SQL (default: true)
- `--skip-refs` / `SKIP_REFS` – comma-separated `dataset.table` entries to keep pointing at the source
- `--replicate-taxonomies` / `REPLICATE_TAXONOMIES` – replicate Data Catalog policy tags (default: true)
- `--replicate-rap` / `REPLICATE_RAP` – replicate resource access policies (default: true)
- `--replicate-tag-templates` / `REPLICATE_TAG_TEMPLATES` – replicate tag templates (default: false)
- `--skip-missing-connections` / `SKIP_MISSING_CONNECTIONS` – skip resources that depend on unavailable BigQuery connections (default: true)
- `--dry-run` / `DRY_RUN` – log actions without performing them

### Other
- `--datasets` may be omitted in favour of regex rules
- `LOG_LEVEL` – standard logging levels (`INFO`, `DEBUG`, etc.)

## Usage

### Basic Usage

```bash
python big-query-replicator.py \
    --src-project my-src-project \
    --dst-project my-dst-project \
    --datasets raw,analytics \
    --skip-refs raw.large_table,demo_dashboard.view_stats \
    --dry-run
```

The example above copies two datasets from `my-src-project` to `my-dst-project`, keeps references to `raw.large_table` and `demo_dashboard.view_stats` pointing to the source project, and runs in dry-run mode.

### Using Environment Variables

```bash
export SRC_PROJECT=my-src-project
export DST_PROJECT=my-dst-project
export SKIP_REFS=raw.3pl_invoice,demo_dashboards.view_3pl_to_line_item
export LOG_LEVEL=DEBUG

python big-query-replicator.py
```

### Advanced: Regex-based Dataset Selection

```bash
python big-query-replicator.py \
    --src-project my-src-project \
    --dst-project my-dst-project \
    --include-regex "^(prod|stage)_.*" \
    --exclude-regex ".*_temp$"
```

## Replication Process

The script follows a specific execution order to ensure dependencies are properly handled:

1. **Phase 0: Dataset Planning** – Enumerate and filter datasets based on selection criteria
2. **Phase 1: Dataset Creation** – Create all destination datasets with metadata (location, labels, encryption, etc.)
3. **Phase 2a: Table Pre-creation** – Create empty managed and external tables with full schema
4. **Phase 2b: Materialized Views** – Create materialized views (may reference pre-created tables)
5. **Phase 2c: Views** – Create views (may reference tables, materialized views, or SKIP_REFS tables)
6. **Phase 2d: Data Copy** – Copy data into managed tables and apply IAM policies
7. **Phase 2e: Routines** – Replicate functions and procedures with SQL rewriting

## Error Handling

The script includes robust error handling for common scenarios:

- **Missing table references**: Views and materialized views that reference non-existent tables (including skipped tables or missing dependencies) are automatically skipped with a warning logged.
- **Temporary tables**: Data copy operations gracefully handle source tables that disappear during execution (e.g., Looker PDTs).
- **Missing connections**: External tables requiring unavailable BigQuery connections are skipped when `--skip-missing-connections` is enabled.
- **SKIP_REFS handling**: Resources referencing tables in `SKIP_REFS` are intelligently handled to avoid cross-location errors.

## Notes

- Ensure `GOOGLE_APPLICATION_CREDENTIALS` points to a service account key with the necessary BigQuery and Data Catalog permissions.
- When `SKIP_REFS` contains a referenced table, views, materialized views, and routines that depend on it are skipped to avoid invalid cross-project references.
- The script uses case-insensitive matching for `SKIP_REFS` entries (e.g., `raw.invoice` matches `raw.Invoice`).
- `SKIP_REFS` accepts both `dataset.table` and `project.dataset.table` formats (uses last two segments).
- All operations log to stdout following 12-factor app principles.

## Development

Run a syntax check with:

```bash
python -m py_compile big-query-replicator.py
```

Generate command line help:

```bash
python big-query-replicator.py --help
```

## Troubleshooting

### Common Issues

**Views fail with "Not found: Table" errors**
- Ensure all dependent tables are being replicated or added to `SKIP_REFS`
- Check that materialized views are created before views that reference them
- Use `LOG_LEVEL=DEBUG` to see detailed pattern matching information

**External tables are skipped**
- Verify BigQuery connections exist in the destination project
- Set `--skip-missing-connections=false` to fail on missing connections instead of skipping

**Routines fail with BadRequest errors**
- The script automatically retries with original SQL body if rewrite fails
- Check routine dependencies (connections, remote functions)
- Review logs for specific error details

**Data copy fails for temporary tables**
- This is expected for Looker PDTs and other temporary tables
- The script logs warnings and continues execution

## Permissions Required

The service account or user running the script needs the following IAM roles:

### Source Project
- `roles/bigquery.dataViewer` – Read datasets, tables, views, and routines
- `roles/bigquery.metadataViewer` – Read metadata and schema information
- `roles/datacatalog.viewer` – Read policy tags and taxonomies (if replicating Data Catalog)

### Destination Project
- `roles/bigquery.dataEditor` – Create and modify datasets, tables, views, and routines
- `roles/bigquery.jobUser` – Execute BigQuery jobs (copy operations)
- `roles/datacatalog.admin` – Create policy tags and taxonomies (if replicating Data Catalog)

Ensure the service account has these roles assigned at the project level for both source and destination projects.