#!/usr/bin/env python3

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Set

from google.api_core import exceptions as gex
from google.api_core.iam import Policy
from google.cloud import bigquery
from google.cloud.bigquery import Dataset, DatasetReference, Table, SchemaField, Routine
from google.cloud.bigquery.job import CopyJobConfig

try:
    from googleapiclient.discovery import build as gcp_build
except Exception:
    gcp_build = None

try:
    from google.cloud import bigquery_connection_v1

    _HAS_CONNECTIONS_API = True
except Exception:  # pragma: no cover
    bigquery_connection_v1 = None
    _HAS_CONNECTIONS_API = False

try:
    from google.cloud import datacatalog_v1

    _HAS_DC = True
except Exception:
    datacatalog_v1 = None
    _HAS_DC = False

VERSION = "1.10.0"

# ----------------------------
# 12-factor helpers
# ----------------------------


def env_bool(name: str, default: Optional[bool] = None) -> Optional[bool]:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "t", "y", "yes", "on"}


def env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    raw = os.getenv(name)
    return raw if raw is not None else default


def env_list(name: str) -> Optional[List[str]]:
    raw = os.getenv(name)
    if not raw:
        return None
    return [x.strip() for x in raw.split(",") if x.strip()]


def setup_logging():
    lvl = (env_str("LOG_LEVEL", "INFO") or "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, lvl, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    logging.info("bq_replication_refactored_fixed %s", VERSION)


# ----------------------------
# CLI & Config
# ----------------------------


@dataclass
class Config:
    src_project: str
    dst_project: str
    include_regex: str = ".*"
    exclude_regex: Optional[str] = None
    datasets_explicit: Optional[List[str]] = None
    rewrite_project_refs: bool = True
    replicate_taxonomies: bool = True
    replicate_rap: bool = True
    replicate_tag_templates: bool = False
    skip_missing_connections: bool = True
    skip_refs: Set[Tuple[str, str]] = None  # (dataset, table) lowercased
    dry_run: bool = False


def _normalize_ref_token(token: str) -> Optional[Tuple[str, str]]:
    """
    Normalize SKIP_REFS entries to (dataset, table), lowercased.
    Accepts: dataset.table or project.dataset.table (uses last two segments).
    """
    t = token.strip().strip("`")
    if not t:
        return None
    parts = [p for p in t.split(".") if p]
    if len(parts) < 2:
        return None
    ds, tbl = parts[-2], parts[-1]
    return (ds.lower(), tbl.lower())


def parse_skip_refs(arg: Optional[str]) -> Set[Tuple[str, str]]:
    res: Set[Tuple[str, str]] = set()
    src = arg or env_str("SKIP_REFS", "") or ""
    if not src:
        return res
    for tok in src.split(","):
        norm = _normalize_ref_token(tok)
        if norm:
            res.add(norm)
    return res


def parse_args_and_env() -> Config:
    parser = argparse.ArgumentParser(
        description="Replicate BigQuery resources from one project to another."
    )
    parser.add_argument("--src-project", help="Source GCP project ID")
    parser.add_argument("--dst-project", help="Destination GCP project ID")
    parser.add_argument(
        "--datasets", help="Comma-separated dataset IDs to include (exact match)."
    )
    parser.add_argument(
        "--include-regex",
        default=None,
        help="Regex to include dataset IDs (defaults to env or '.*').",
    )
    parser.add_argument(
        "--exclude-regex", default=None, help="Regex to exclude dataset IDs."
    )
    parser.add_argument(
        "--skip-refs",
        default=None,
        help="Comma-separated list of dataset.table (or project.dataset.table) to skip and keep refs pointed at source.",
    )
    # Boolean flags
    try:
        parser.add_argument(
            "--dry-run",
            dest="dry_run",
            action=argparse.BooleanOptionalAction,
            default=None,
        )
        parser.add_argument(
            "--rewrite-project-refs",
            dest="rewrite_views",
            action=argparse.BooleanOptionalAction,
            default=None,
        )
        parser.add_argument(
            "--replicate-taxonomies",
            dest="repl_tax",
            action=argparse.BooleanOptionalAction,
            default=None,
        )
        parser.add_argument(
            "--replicate-rap",
            dest="repl_rap",
            action=argparse.BooleanOptionalAction,
            default=None,
        )
        parser.add_argument(
            "--replicate-tag-templates",
            dest="repl_tags",
            action=argparse.BooleanOptionalAction,
            default=None,
        )
        parser.add_argument(
            "--skip-missing-connections",
            dest="skip_conn",
            action=argparse.BooleanOptionalAction,
            default=None,
        )
    except Exception:
        parser.add_argument("--dry-run", dest="dry_run", action="store_true")
        parser.add_argument("--no-dry-run", dest="dry_run", action="store_false")
        parser.set_defaults(dry_run=None)
        for flag, dest in [
            ("--rewrite-project-refs", "rewrite_views"),
            ("--replicate-taxonomies", "repl_tax"),
            ("--replicate-rap", "repl_rap"),
            ("--replicate-tag-templates", "repl_tags"),
            ("--skip-missing-connections", "skip_conn"),
        ]:
            parser.add_argument(flag, dest=dest, action="store_true")
            parser.add_argument(
                f"--no-{flag.lstrip('--')}", dest=dest, action="store_false"
            )
            parser.set_defaults(**{dest: None})

    args = parser.parse_args()

    # Merge ENV -> CLI (CLI wins)
    src_project = args.src_project or env_str("SRC_PROJECT")
    dst_project = args.dst_project or env_str("DST_PROJECT")
    if not src_project or not dst_project:
        parser.error(
            "Both --src-project and --dst-project (or SRC_PROJECT/DST_PROJECT env vars) are required."
        )

    datasets = args.datasets or env_str("DATASETS")
    datasets_explicit = (
        [d.strip() for d in datasets.split(",")] if datasets else env_list("DATASETS")
    )

    include_regex = args.include_regex or env_str("INCLUDE_REGEX", ".*")
    exclude_regex = args.exclude_regex or env_str("EXCLUDE_REGEX")

    rewrite_views = (
        args.rewrite_views
        if args.rewrite_views is not None
        else env_bool("REWRITE_PROJECT_REFS", True)
    )
    repl_tax = (
        args.repl_tax
        if args.repl_tax is not None
        else env_bool("REPLICATE_TAXONOMIES", True)
    )
    repl_rap = (
        args.repl_rap if args.repl_rap is not None else env_bool("REPLICATE_RAP", True)
    )
    repl_tags = (
        args.repl_tags
        if args.repl_tags is not None
        else env_bool("REPLICATE_TAG_TEMPLATES", False)
    )
    skip_conn = (
        args.skip_conn
        if args.skip_conn is not None
        else env_bool("SKIP_MISSING_CONNECTIONS", True)
    )
    dry_run = args.dry_run if args.dry_run is not None else env_bool("DRY_RUN", False)

    skip_refs = parse_skip_refs(args.skip_refs)

    return Config(
        src_project=src_project,
        dst_project=dst_project,
        include_regex=include_regex or ".*",
        exclude_regex=exclude_regex,
        datasets_explicit=datasets_explicit,
        rewrite_project_refs=bool(rewrite_views),
        replicate_taxonomies=bool(repl_tax),
        replicate_rap=bool(repl_rap),
        replicate_tag_templates=bool(repl_tags),
        skip_missing_connections=bool(skip_conn),
        skip_refs=skip_refs,
        dry_run=bool(dry_run),
    )


# ----------------------------
# Location normalization
# ----------------------------


def dc_location_from_bq(location: Optional[str]) -> str:
    if not location:
        return "us"
    return location.strip().lower()


def dc_parent(project_id: str, bq_location: Optional[str]) -> str:
    return f"projects/{project_id}/locations/{dc_location_from_bq(bq_location)}"


# ----------------------------
# Connection helpers
# ----------------------------


def _normalize_connection_id(
    raw: str, default_project: str, default_location: str
) -> str:
    raw = (raw or "").strip()
    if not raw:
        return raw
    if raw.startswith("projects/"):
        return raw
    parts = raw.split(".")
    if len(parts) == 3:
        p, l, c = parts
        return f"projects/{p}/locations/{l}/connections/{c}"
    if len(parts) == 2:
        l, c = parts
        return f"projects/{default_project}/locations/{l}/connections/{c}"
    return f"projects/{default_project}/locations/{default_location}/connections/{raw}"


def connection_exists(connection_id: str) -> bool:
    if not _HAS_CONNECTIONS_API:
        return False
    try:
        client = bigquery_connection_v1.ConnectionServiceClient()
        conn = client.get_connection(name=connection_id)
        return bool(conn)
    except Exception:
        return False


def is_missing_connection_error(err: Exception) -> bool:
    msg = f"{err}"
    if isinstance(err, gex.NotFound) and "Connection " in msg:
        return True
    if isinstance(err, gex.BadRequest) and "Connection " in msg and "Not found" in msg:
        return True
    if "Not found: Connection " in msg:
        return True
    return False


# ----------------------------
# SQL rewrite helpers
# ----------------------------


def _is_skipped(ds: str, tbl: str, skip_refs: Set[Tuple[str, str]]) -> bool:
    return (ds.lower(), tbl.lower()) in skip_refs


def rewrite_sql_with_skip_refs(
    sql: str,
    src_project: str,
    dst_project: str,
    migrating_datasets: Set[str],
    skip_refs: Set[Tuple[str, str]],
) -> str:
    """Rewrites SQL to target dst_project for migrated datasets, except any dataset.table in skip_refs,
    which are forced to src_project.
    Handles: backticked and bare identifiers; fully-qualified and unqualified.
    """

    # 1) Replace fully-qualified backticked refs: `project.dataset.table`
    def repl_fq_bt(m: re.Match) -> str:
        proj, ds, tbl = m.group("project"), m.group("dataset"), m.group("table")
        if _is_skipped(ds, tbl, skip_refs):
            target_proj = src_project
        elif ds in migrating_datasets:
            target_proj = dst_project
        else:
            target_proj = proj  # leave as-is
        return f"`{target_proj}.{ds}.{tbl}`"

    sql = re.sub(
        r"`(?P<project>[A-Za-z0-9_\-]+)\.(?P<dataset>[A-Za-z_]\w*)\.(?P<table>[A-Za-z_]\w*)`",
        repl_fq_bt,
        sql,
    )

    # 2) Replace fully-qualified bare refs: project.dataset.table
    def repl_fq_bare(m: re.Match) -> str:
        proj, ds, tbl = m.group("project"), m.group("dataset"), m.group("table")
        if _is_skipped(ds, tbl, skip_refs):
            target_proj = src_project
        elif ds in migrating_datasets:
            target_proj = dst_project
        else:
            target_proj = proj
        return f"{target_proj}.{ds}.{tbl}"

    sql = re.sub(
        r"\b(?P<project>[A-Za-z0-9_\-]+)\.(?P<dataset>[A-Za-z_]\w*)\.(?P<table>[A-Za-z_]\w*)\b",
        repl_fq_bare,
        sql,
    )

    # 3) Backticked `dataset.table` or `` `dataset`.`table` ``
    def repl_ds_bt(m: re.Match) -> str:
        ds, tbl = m.group("dataset"), m.group("table")
        if _is_skipped(ds, tbl, skip_refs):
            return f"`{src_project}.{ds}.{tbl}`"
        if ds in migrating_datasets:
            return f"`{dst_project}.{ds}.{tbl}`"
        return m.group(0)

    sql = re.sub(
        r"`(?P<dataset>[A-Za-z_]\w*)\.(?P<table>[A-Za-z_]\w*)`", repl_ds_bt, sql
    )
    sql = re.sub(
        r"`(?P<dataset>[A-Za-z_]\w*)`\.`(?P<table>[A-Za-z_]\w*)`", repl_ds_bt, sql
    )

    # 4) Bare dataset.table after data-manipulation keywords (heuristic to avoid CTE aliases)
    kw = r"(FROM|JOIN|UPDATE|INTO|TABLE|DELETE\s+FROM|INSERT\s+INTO|MERGE\s+INTO)"
    bare = re.compile(
        rf"(?ix)\b{kw}\s+"
        r"(?P<q>`?)"
        r"(?P<dataset>[A-Za-z_]\w*)\.(?P<table>[A-Za-z_]\w*)"
        r"(?!\.\w)"
        r"(?P=q)"
    )

    def repl_bare(m: re.Match) -> str:
        ds, tbl = m.group("dataset"), m.group("table")
        q = m.group("q") or ""
        if _is_skipped(ds, tbl, skip_refs):
            name = f"{q}{src_project}.{ds}.{tbl}{q}"
        elif ds in migrating_datasets:
            name = f"{q}{dst_project}.{ds}.{tbl}{q}"
        else:
            return m.group(0)
        start = m.start("dataset") - m.start(0)
        end = m.end("table") - m.start(0)
        pre = m.group(0)
        return pre[:start] + name + pre[end:]

    sql = bare.sub(repl_bare, sql)

    return sql


# Quick detector: does a SQL text reference any dataset.table from SKIP_REFS?
def sql_mentions_any_skip_ref(
    sql: str, skip_refs: Set[Tuple[str, str]]
) -> Optional[Tuple[str, str]]:
    if not sql or not skip_refs:
        return None
    s = sql
    for ds, tbl in skip_refs:
        ds_re = re.escape(ds)
        tbl_re = re.escape(tbl)
        patterns = [
            rf"`[^`]+\.{ds_re}\.{tbl_re}`",  # `project.dataset.table`
            rf"`{ds_re}`\.`{tbl_re}`",  # `dataset`.`table`
            rf"`{ds_re}\.{tbl_re}`",  # `dataset.table`
            rf"\b{ds_re}\.{tbl_re}\b",  # dataset.table
            rf"\b[A-Za-z0-9_\-]+\.{ds_re}\.{tbl_re}\b",  # project.dataset.table
        ]
        for pat in patterns:
            if re.search(pat, s, re.IGNORECASE):
                return (ds, tbl)
    return None


# ----------------------------
# Core logic helpers
# ----------------------------


def match_dataset(
    name: str,
    include_regex: Optional[str],
    exclude_regex: Optional[str],
    explicit: Optional[List[str]],
) -> bool:
    if explicit is not None:
        return name in explicit
    if include_regex and not re.fullmatch(include_regex, name):
        return False
    if exclude_regex and re.fullmatch(exclude_regex, name):
        return False
    return True


def ensure_dataset(
    bq: bigquery.Client,
    project_id: str,
    ds_id: str,
    location: str,
    description: Optional[str],
    labels: Optional[Dict[str, str]],
    default_table_expiration_ms: Optional[int],
    default_partition_expiration_ms: Optional[int],
    default_encryption_config: Optional[bigquery.EncryptionConfiguration],
    access_entries: Optional[List[bigquery.AccessEntry]],
    dry_run: bool,
) -> Dataset:
    ds_ref = bigquery.DatasetReference(project_id, ds_id)
    try:
        ds = bq.get_dataset(ds_ref)
        logging.info("Dataset exists: %s:%s", project_id, ds_id)
        updated = False
        if description is not None and ds.description != description:
            ds.description = description
            updated = True
        if labels is not None and ds.labels != labels:
            ds.labels = labels
            updated = True
        if (
            default_table_expiration_ms is not None
            and ds.default_table_expiration_ms != default_table_expiration_ms
        ):
            ds.default_table_expiration_ms = default_table_expiration_ms
            updated = True
        if (
            default_partition_expiration_ms is not None
            and ds.default_partition_expiration_ms != default_partition_expiration_ms
        ):
            ds.default_partition_expiration_ms = default_partition_expiration_ms
            updated = True
        if default_encryption_config is not None:
            src_kms = getattr(default_encryption_config, "kms_key_name", None)
            dst_kms = getattr(
                getattr(ds, "default_encryption_configuration", None),
                "kms_key_name",
                None,
            )
            if src_kms and src_kms != dst_kms:
                ds.default_encryption_configuration = default_encryption_config
                updated = True
        if access_entries is not None and sorted(ds.access_entries, key=str) != sorted(
            access_entries, key=str
        ):
            ds.access_entries = access_entries
            updated = True
        if updated:
            if dry_run:
                logging.info(
                    "[DRY-RUN] Would update dataset properties/access: %s:%s",
                    project_id,
                    ds_id,
                )
            else:
                fields = [
                    "description",
                    "labels",
                    "default_table_expiration_ms",
                    "default_partition_expiration_ms",
                    "default_encryption_configuration",
                ]
                if access_entries is not None:
                    fields.append("access_entries")
                ds = bq.update_dataset(ds, fields)
                logging.info("Updated dataset: %s:%s", project_id, ds_id)
        return ds
    except gex.NotFound:
        ds = bigquery.Dataset(ds_ref)
        ds.location = location
        ds.description = description
        ds.labels = labels or {}
        ds.default_table_expiration_ms = default_table_expiration_ms
        ds.default_partition_expiration_ms = default_partition_expiration_ms
        if default_encryption_config is not None:
            ds.default_encryption_configuration = default_encryption_config
        if access_entries is not None:
            ds.access_entries = access_entries
        if dry_run:
            logging.info(
                "[DRY-RUN] Would create dataset: %s:%s (location=%s)",
                project_id,
                ds_id,
                location,
            )
            return ds
        ds = bq.create_dataset(ds, exists_ok=True)
        logging.info("Created dataset: %s:%s", project_id, ds_id)
        return ds


def copy_table_iam_policy(
    bq_src: bigquery.Client,
    bq_dst: bigquery.Client,
    src_tbl: Table,
    dst_tbl_ref: bigquery.TableReference,
    dry_run: bool,
):
    try:
        src_policy = bq_src.get_iam_policy(src_tbl.reference)
    except Exception as e:
        logging.debug(
            "No table IAM policy on source or unable to fetch for %s: %s",
            src_tbl.reference,
            e,
        )
        return
    if not src_policy or not getattr(src_policy, "bindings", None):
        return
    new_policy = Policy(version=getattr(src_policy, "version", 3))
    try:
        for role, members in src_policy.bindings.items():
            new_policy.bindings[role] = set(members)
    except Exception:
        new_policy = Policy(version=3)
        for b in getattr(src_policy, "bindings", []):
            role = b.get("role")
            members = set(b.get("members", [])) if isinstance(b, dict) else set()
            if role:
                new_policy.bindings[role] = members
    if dry_run:
        logging.info("[DRY-RUN] Would set table IAM policy on: %s", dst_tbl_ref)
        return
    try:
        bq_dst.set_iam_policy(dst_tbl_ref, new_policy)
        logging.info("Copied table IAM policy: %s", dst_tbl_ref)
    except Exception as e:
        logging.warning("Failed setting table IAM policy on %s: %s", dst_tbl_ref, e)


def walk_schema_replace_policy_tags(
    fields: List[SchemaField], policy_tag_map: Dict[str, str]
) -> List[SchemaField]:
    out: List[SchemaField] = []
    for f in fields:
        new_policy = None
        if f.policy_tags and f.policy_tags.names:
            new_names = [policy_tag_map.get(n, n) for n in f.policy_tags.names]
            new_policy = bigquery.PolicyTagList(names=new_names)
        sub = (
            walk_schema_replace_policy_tags(f.fields, policy_tag_map)
            if f.fields
            else []
        )
        out.append(
            bigquery.SchemaField(
                name=f.name,
                field_type=f.field_type,
                mode=f.mode,
                description=f.description,
                fields=sub,
                policy_tags=new_policy,
            )
        )
    return out


# ----------------------------
# Copy/Create resources
# ----------------------------


def ensure_table_presence(
    bq_dst: bigquery.Client,
    src_table: bigquery.Table,
    dst_project: str,
    dst_dataset: str,
    policy_tag_map: Dict[str, str],
    skip_missing_connections: bool,
    skip_refs: Set[Tuple[str, str]],
    dry_run: bool,
) -> Optional[bigquery.Table]:
    """Create the destination table (empty) with schema and config. Handles external tables too."""
    if (dst_dataset.lower(), src_table.table_id.lower()) in skip_refs:
        logging.info(
            "Skipping pre-create of table per SKIP_REFS: %s.%s",
            dst_dataset,
            src_table.table_id,
        )
        return None

    dst_ref = bigquery.TableReference(
        bigquery.DatasetReference(dst_project, dst_dataset), src_table.table_id
    )
    dst_tbl = bigquery.Table(dst_ref)
    schema = src_table.schema
    if policy_tag_map and schema:
        schema = walk_schema_replace_policy_tags(schema, policy_tag_map)
    dst_tbl.schema = schema
    dst_tbl.description = src_table.description
    dst_tbl.labels = src_table.labels
    dst_tbl.time_partitioning = src_table.time_partitioning
    dst_tbl.range_partitioning = src_table.range_partitioning
    dst_tbl.clustering_fields = src_table.clustering_fields
    dst_tbl.encryption_configuration = src_table.encryption_configuration

    # External table path
    if src_table.external_data_configuration is not None:
        dst_tbl.external_data_configuration = src_table.external_data_configuration
        conn_id = None
        try:
            conn_id = getattr(
                dst_tbl.external_data_configuration, "connection_id", None
            ) or getattr(dst_tbl.external_data_configuration, "connectionId", None)
        except Exception:
            conn_id = None
        if conn_id:
            norm_conn = _normalize_connection_id(
                conn_id, dst_project, (src_table.location or "us").lower()
            )
            exists = connection_exists(norm_conn)
            if not exists and skip_missing_connections:
                logging.warning(
                    "Skipping external table %s due to missing connection: %s",
                    dst_ref,
                    conn_id,
                )
                return None

        if dry_run:
            logging.info("[DRY-RUN] Would create external table: %s", dst_ref)
            return None
        try:
            created = bq_dst.create_table(dst_tbl, exists_ok=True)
            logging.info("Ensured external table: %s", created.reference)
            return created
        except gex.Conflict:
            existing = bq_dst.get_table(dst_ref)
            mutable = [
                "schema",
                "description",
                "labels",
                "time_partitioning",
                "range_partitioning",
                "clustering_fields",
                "external_data_configuration",
                "encryption_configuration",
            ]
            for f in mutable:
                setattr(existing, f, getattr(dst_tbl, f))
            bq_dst.update_table(existing, mutable)
            logging.info("Updated external table: %s", existing.reference)
            return existing
        except Exception as e:
            if skip_missing_connections and is_missing_connection_error(e):
                logging.warning(
                    "Skipping external table %s (missing connection): %s", dst_ref, e
                )
                return None
            raise

    # Managed table path
    if dry_run:
        logging.info("[DRY-RUN] Would pre-create managed table: %s", dst_ref)
        return None
    try:
        created = bq_dst.create_table(dst_tbl, exists_ok=True)
        logging.info("Ensured managed table (empty): %s", created.reference)
        return created
    except gex.Conflict:
        existing = bq_dst.get_table(dst_ref)
        mutable = [
            "schema",
            "description",
            "labels",
            "time_partitioning",
            "range_partitioning",
            "clustering_fields",
            "encryption_configuration",
        ]
        for f in mutable:
            try:
                setattr(existing, f, getattr(dst_tbl, f))
            except Exception:
                pass
        bq_dst.update_table(existing, mutable)
        logging.info("Updated managed table attrs: %s", existing.reference)
        return existing


def copy_table_data(
    bq: bigquery.Client,
    src_table: bigquery.Table,
    dst_project: str,
    dst_dataset: str,
    skip_refs: Set[Tuple[str, str]],
    dry_run: bool,
) -> Optional[bigquery.Table]:
    """Copy data after the destination table already exists."""
    if (dst_dataset.lower(), src_table.table_id.lower()) in skip_refs:
        logging.info(
            "Skipping data copy per SKIP_REFS: %s.%s", dst_dataset, src_table.table_id
        )
        return None
    if src_table.external_data_configuration is not None:
        return None  # external tables don't copy data
    dst_ref = bigquery.TableReference(
        bigquery.DatasetReference(dst_project, dst_dataset), src_table.table_id
    )
    job_cfg = CopyJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    if dry_run:
        logging.info(
            "[DRY-RUN] Would copy data into: %s <- %s", dst_ref, src_table.reference
        )
        return None
    try:
        job = bq.copy_table(
            sources=src_table.reference,
            destination=dst_ref,
            job_config=job_cfg,
            location=src_table.location or bq.location,
        )
        job.result()
        dst_tbl = bq.get_table(dst_ref)
        logging.info("Copied data: %s -> %s", src_table.reference, dst_ref)
        return dst_tbl
    except gex.NotFound as e:
        # Table may have disappeared (e.g., temporary Looker PDTs)
        logging.warning(
            "Skipping data copy for %s.%s - source table no longer exists: %s",
            dst_dataset,
            src_table.table_id,
            str(e)[:200],
        )
        return None


def create_or_update_view(
    bq_dst: bigquery.Client,
    src_view: Table,
    dst_project: str,
    ds_id: str,
    rewrite_projects: bool,
    migrating_datasets: Set[str],
    skip_refs: Set[Tuple[str, str]],
    src_project: str,
    dry_run: bool,
):
    if (ds_id.lower(), src_view.table_id.lower()) in skip_refs:
        logging.info("Skipping VIEW per SKIP_REFS: %s.%s", ds_id, src_view.table_id)
        return

    view_sql = src_view.view_query or ""
    # If this view references any SKIP_REFS table, skip materializing it in the destination
    # to avoid cross-location errors while we intend those refs to stay on source.
    hit = sql_mentions_any_skip_ref(view_sql, skip_refs)
    if hit:
        logging.info(
            "Skipping VIEW %s.%s because it references SKIP_REFS table %s.%s",
            ds_id,
            src_view.table_id,
            hit[0],
            hit[1],
        )
        return
    if rewrite_projects:
        view_sql = rewrite_sql_with_skip_refs(
            view_sql, src_project, dst_project, migrating_datasets, skip_refs
        )

    dst_ref = bigquery.TableReference(
        DatasetReference(dst_project, ds_id), src_view.table_id
    )
    dst_view = bigquery.Table(dst_ref)
    dst_view.view_query = view_sql
    dst_view.view_use_legacy_sql = src_view.view_use_legacy_sql
    dst_view.description = src_view.description
    dst_view.labels = src_view.labels
    if dry_run:
        logging.info("[DRY-RUN] Would create/update VIEW: %s", dst_ref)
    else:
        try:
            created = bq_dst.create_table(dst_view, exists_ok=True)
            logging.info("Created/updated VIEW: %s", created.reference)
        except gex.Conflict:
            existing = bq_dst.get_table(dst_ref)
            existing.view_query = view_sql
            existing.view_use_legacy_sql = src_view.view_use_legacy_sql
            existing.description = src_view.description
            existing.labels = src_view.labels
            bq_dst.update_table(
                existing, ["view_query", "view_use_legacy_sql", "description", "labels"]
            )
            logging.info("Updated VIEW: %s", existing.reference)
        except gex.NotFound as e:
            # Check if error is due to missing table reference (could be SKIP_REFS or missing MVs)
            err_msg = str(e).lower()
            skip_refs_matched = False
            logging.debug(
                "Checking NotFound error for view %s.%s. Error message (lowercased): %s",
                ds_id,
                src_view.table_id,
                err_msg[:300],
            )

            # First check if it's a SKIP_REFS table
            for ds_name, tbl_name in skip_refs:
                patterns = [
                    f"{ds_name}.{tbl_name}",
                    f"{ds_name}:{tbl_name}",
                    f":{ds_name}.{tbl_name}",
                ]
                for pattern in patterns:
                    if pattern in err_msg:
                        logging.info(
                            "Skipping VIEW %s.%s because it references SKIP_REFS table %s.%s which doesn't exist in destination location (matched pattern: %s)",
                            ds_id,
                            src_view.table_id,
                            ds_name,
                            tbl_name,
                            pattern,
                        )
                        skip_refs_matched = True
                        break
                if skip_refs_matched:
                    return

            # If not SKIP_REFS, check if it's any table not found error
            if "not found: table " in err_msg:
                logging.warning(
                    "Skipping VIEW %s.%s because it references a table that doesn't exist in destination (possibly a skipped materialized view or missing table): %s",
                    ds_id,
                    src_view.table_id,
                    str(e)[:300],
                )
                return

            # If it's not a table reference issue, re-raise
            logging.error(
                "NotFound error for view %s.%s (not a missing table reference): %s",
                ds_id,
                src_view.table_id,
                str(e),
            )
            raise


def create_or_update_materialized_view(
    bq_dst: bigquery.Client,
    src_mv: Table,
    dst_project: str,
    ds_id: str,
    rewrite_projects: bool,
    migrating_datasets: Set[str],
    skip_refs: Set[Tuple[str, str]],
    src_project: str,
    dry_run: bool,
):
    if (ds_id.lower(), src_mv.table_id.lower()) in skip_refs:
        logging.info(
            "Skipping MATERIALIZED VIEW per SKIP_REFS: %s.%s", ds_id, src_mv.table_id
        )
        return

    mv_def = getattr(src_mv, "materialized_view", None) or getattr(
        src_mv, "materialized_view_definition", None
    )
    if mv_def is None:
        logging.warning(
            "Materialized view definition missing for %s; skipping.", src_mv.reference
        )
        return
    query = getattr(mv_def, "query", None) or getattr(mv_def, "query_string", None)
    if not query:
        logging.warning(
            "Materialized view has no query for %s; skipping.", src_mv.reference
        )
        return

    hit = sql_mentions_any_skip_ref(query, skip_refs)
    if hit:
        logging.info(
            "Skipping MATERIALIZED VIEW %s.%s because it references SKIP_REFS table %s.%s",
            ds_id,
            src_mv.table_id,
            hit[0],
            hit[1],
        )
        return
    if rewrite_projects:
        query = rewrite_sql_with_skip_refs(
            query, src_project, dst_project, migrating_datasets, skip_refs
        )

    enable_refresh = getattr(mv_def, "enable_refresh", None)
    refresh_interval_ms = getattr(mv_def, "refresh_interval_ms", None)
    max_staleness = getattr(mv_def, "max_staleness", None)

    dst_ref = bigquery.TableReference(
        DatasetReference(dst_project, ds_id), src_mv.table_id
    )
    dst_tbl = bigquery.Table(dst_ref)
    try:
        mv_class = bigquery.MaterializedViewDefinition  # type: ignore[attr-defined]
    except Exception:
        mv_class = None

    if mv_class is not None:
        mv = mv_class(query=query)
        if enable_refresh is not None:
            try:
                mv.enable_refresh = enable_refresh
            except Exception:
                pass
        if refresh_interval_ms is not None:
            try:
                mv.refresh_interval_ms = refresh_interval_ms
            except Exception:
                pass
        if max_staleness is not None:
            try:
                mv.max_staleness = max_staleness
            except Exception:
                pass
        dst_tbl.materialized_view = mv
    else:
        try:
            dst_tbl.materialized_view_definition = {"query": query}
        except Exception:
            logging.warning(
                "Client library does not support materialized views. Skipping %s",
                dst_ref,
            )
            return

    dst_tbl.description = src_mv.description
    dst_tbl.labels = src_mv.labels

    if dry_run:
        logging.info("[DRY-RUN] Would create/update MATERIALIZED VIEW: %s", dst_ref)
        return

    try:
        created = bq_dst.create_table(dst_tbl, exists_ok=True)
        logging.info("Created/updated MATERIALIZED VIEW: %s", created.reference)
    except gex.Conflict:
        existing = bq_dst.get_table(dst_ref)
        try:
            existing.materialized_view = dst_tbl.materialized_view
        except Exception:
            try:
                existing.materialized_view_definition = (
                    dst_tbl.materialized_view_definition
                )
            except Exception:
                pass
        existing.description = src_mv.description
        existing.labels = src_mv.labels
        try:
            bq_dst.update_table(
                existing, ["materialized_view", "description", "labels"]
            )
        except Exception:
            bq_dst.update_table(existing, ["description", "labels"])
        logging.info("Updated MATERIALIZED VIEW: %s", existing.reference)
    except gex.NotFound as e:
        # Check if error is due to missing table reference (could be SKIP_REFS or missing tables)
        err_msg = str(e).lower()
        skip_refs_matched = False

        # First check if it's a SKIP_REFS table
        for ds_name, tbl_name in skip_refs:
            patterns = [
                f"{ds_name}.{tbl_name}",
                f"{ds_name}:{tbl_name}",
                f":{ds_name}.{tbl_name}",
            ]
            for pattern in patterns:
                if pattern in err_msg:
                    logging.info(
                        "Skipping MATERIALIZED VIEW %s.%s because it references SKIP_REFS table %s.%s which doesn't exist in destination location",
                        ds_id,
                        src_mv.table_id,
                        ds_name,
                        tbl_name,
                    )
                    skip_refs_matched = True
                    break
            if skip_refs_matched:
                return

        # If not SKIP_REFS, check if it's any table not found error
        if "not found: table " in err_msg:
            logging.warning(
                "Skipping MATERIALIZED VIEW %s.%s because it references a table that doesn't exist in destination (possibly a skipped table or missing dependency): %s",
                ds_id,
                src_mv.table_id,
                str(e)[:300],
            )
            return

        # If it's not a table reference issue, re-raise
        logging.error(
            "NotFound error for materialized view %s.%s (not a missing table reference): %s",
            ds_id,
            src_mv.table_id,
            str(e),
        )
        raise


# ----------------------------
# Routine helpers
# ----------------------------


def supported_routine_fields() -> Set[str]:
    # Try to read mapping from the client class (if available); fall back to a safe set.
    try:
        from google.cloud.bigquery.routine import Routine as RoutineCls  # type: ignore

        mapping = getattr(RoutineCls, "_PROPERTY_TO_API_FIELD", None)
        if isinstance(mapping, dict):
            return set(mapping.keys())
    except Exception:
        pass
    # Safe defaults across library versions
    return {
        "type_",
        "language",
        "body",
        "arguments",
        "return_type",
        "return_table_type",
        "imported_libraries",
        "determinism_level",
        "remote_function_options",
        "spark_options",
        "strict_mode",
    }


def build_dst_routine_from_source(
    src_r: Routine,
    dst_project: str,
    ds_id: str,
    rewrite_projects: bool,
    migrating_ds_ids: Set[str],
    skip_refs: Set[Tuple[str, str]],
    src_project: str,
) -> Routine:
    dst_ref = bigquery.RoutineReference.from_string(
        f"{dst_project}.{ds_id}.{src_r.routine_id}"
    )
    dst_r = bigquery.Routine(dst_ref)

    allowed = supported_routine_fields()

    def copy_if(field: str, value):
        if field in allowed and value is not None:
            setattr(dst_r, field, value)

    copy_if("type_", src_r.type_)
    copy_if("language", src_r.language)

    body = getattr(src_r, "body", None)
    lang = (getattr(src_r, "language", None) or "").upper()
    if body and rewrite_projects and lang == "SQL":
        body = rewrite_sql_with_skip_refs(
            body, src_project, dst_project, migrating_ds_ids, skip_refs
        )
    copy_if("body", body)

    copy_if("arguments", getattr(src_r, "arguments", None))
    copy_if("return_type", getattr(src_r, "return_type", None))
    copy_if("return_table_type", getattr(src_r, "return_table_type", None))

    copy_if("imported_libraries", getattr(src_r, "imported_libraries", None))
    copy_if("determinism_level", getattr(src_r, "determinism_level", None))
    copy_if("remote_function_options", getattr(src_r, "remote_function_options", None))
    copy_if("spark_options", getattr(src_r, "spark_options", None))
    copy_if("strict_mode", getattr(src_r, "strict_mode", None))

    try:
        dst_r.description = src_r.description
    except Exception:
        pass

    return dst_r


def update_routine_safe(bq_client: bigquery.Client, routine: Routine) -> None:
    allowed = supported_routine_fields()
    candidates = [
        "type_",
        "language",
        "body",
        "arguments",
        "return_type",
        "return_table_type",
        "imported_libraries",
        "determinism_level",
        "remote_function_options",
        "spark_options",
        "strict_mode",
    ]
    fields = [
        f for f in candidates if f in allowed and getattr(routine, f, None) is not None
    ]
    try:
        bq_client.update_routine(routine, fields)
        return
    except ValueError as ve:
        msg = str(ve)
        logging.warning(
            "Routine update raised ValueError (%s). Retrying with pruned fields.", msg
        )
        pruned = [f for f in fields if f not in msg]
        essential = [
            "body",
            "language",
            "arguments",
            "return_type",
            "return_table_type",
            "type_",
        ]
        fields2 = [f for f in pruned if f in essential or f in allowed]
        if not fields2:
            fields2 = [
                f
                for f in essential
                if f in allowed and getattr(routine, f, None) is not None
            ]
        bq_client.update_routine(routine, fields2)


# ----------------------------
# Data Catalog (optional)
# ----------------------------


def list_all_taxonomies(ptm, project_id: str, bq_location: str):
    if not _HAS_DC:
        return []
    parent = dc_parent(project_id, bq_location)
    return list(ptm.list_taxonomies(parent=parent))


# ----------------------------
# Dataset enumeration & planning
# ----------------------------


def collect_selected_source_datasets(
    bq_src: bigquery.Client, cfg: Config
) -> List[Dataset]:
    found: List[Dataset] = []
    if cfg.datasets_explicit:
        for ds_id in cfg.datasets_explicit:
            try:
                ds = bq_src.get_dataset(
                    bigquery.DatasetReference(cfg.src_project, ds_id)
                )
                found.append(ds)
            except gex.NotFound:
                logging.warning("Dataset '%s' not found in source; skipping.", ds_id)
    else:
        for item in bq_src.list_datasets():
            ds_id = item.dataset_id
            if match_dataset(ds_id, cfg.include_regex, cfg.exclude_regex, None):
                try:
                    found.append(bq_src.get_dataset(item.reference))
                except gex.NotFound:
                    logging.warning(
                        "Dataset disappeared while listing: %s; skipping.",
                        item.reference,
                    )
    return found


def list_tables_for_datasets(
    bq_src: bigquery.Client, datasets: List[Dataset]
) -> Dict[str, List[Table]]:
    out: Dict[str, List[Table]] = {}
    for ds in datasets:
        ds_id = ds.dataset_id
        out[ds_id] = []
        for t in bq_src.list_tables(ds.reference):
            try:
                out[ds_id].append(bq_src.get_table(t.reference))
            except gex.NotFound:
                logging.warning("Skipping disappeared table: %s", t.reference)
    return out


# ----------------------------
# Main
# ----------------------------


def main():
    setup_logging()
    cfg = parse_args_and_env()

    bq_src = bigquery.Client(project=cfg.src_project)
    bq_dst = bigquery.Client(project=cfg.dst_project)

    bq_rest = gcp_build("bigquery", "v2", cache_discovery=False) if gcp_build else None
    ptm = datacatalog_v1.PolicyTagManagerClient() if _HAS_DC else None

    # Phase 0: Plan datasets
    src_datasets: List[Dataset] = collect_selected_source_datasets(bq_src, cfg)
    if not src_datasets:
        logging.warning(
            "No datasets selected for replication in source project %s", cfg.src_project
        )
        return

    migrating_ds_ids: Set[str] = {ds.dataset_id for ds in src_datasets}

    # Phase 1: Ensure ALL destination datasets exist
    for src_ds in src_datasets:
        ensure_dataset(
            bq=bq_dst,
            project_id=cfg.dst_project,
            ds_id=src_ds.dataset_id,
            location=src_ds.location,
            description=src_ds.description,
            labels=src_ds.labels,
            default_table_expiration_ms=src_ds.default_table_expiration_ms,
            default_partition_expiration_ms=src_ds.default_partition_expiration_ms,
            default_encryption_config=getattr(
                src_ds, "default_encryption_configuration", None
            ),
            access_entries=None,  # defer ACLs; authorized views later
            dry_run=cfg.dry_run,
        )

    # Phase 2: Enumerate tables once
    tables_by_ds = list_tables_for_datasets(bq_src, src_datasets)

    # Phase 2a: PRE-CREATE ALL tables (managed + external), except SKIP_REFS
    for src_ds in src_datasets:
        ds_id = src_ds.dataset_id
        for t in tables_by_ds.get(ds_id, []):
            if t.table_type in ("VIEW", "MATERIALIZED_VIEW"):
                continue
            ensure_table_presence(
                bq_dst=bq_dst,
                src_table=t,
                dst_project=cfg.dst_project,
                dst_dataset=ds_id,
                policy_tag_map={},  # wire in Data Catalog tag map if you replicate tags
                skip_missing_connections=cfg.skip_missing_connections,
                skip_refs=cfg.skip_refs,
                dry_run=cfg.dry_run,
            )

    # Phase 2b: MATERIALIZED VIEWS
    for src_ds in src_datasets:
        ds_id = src_ds.dataset_id
        for t in tables_by_ds.get(ds_id, []):
            if t.table_type == "MATERIALIZED_VIEW":
                create_or_update_materialized_view(
                    bq_dst,
                    t,
                    cfg.dst_project,
                    ds_id,
                    cfg.rewrite_project_refs,
                    migrating_ds_ids,
                    cfg.skip_refs,
                    cfg.src_project,
                    cfg.dry_run,
                )

    # Phase 2c: VIEWS
    for src_ds in src_datasets:
        ds_id = src_ds.dataset_id
        for t in tables_by_ds.get(ds_id, []):
            if t.table_type == "VIEW":
                create_or_update_view(
                    bq_dst,
                    t,
                    cfg.dst_project,
                    ds_id,
                    cfg.rewrite_project_refs,
                    migrating_ds_ids,
                    cfg.skip_refs,
                    cfg.src_project,
                    cfg.dry_run,
                )

    # Phase 2d: COPY DATA into managed tables
    for src_ds in src_datasets:
        ds_id = src_ds.dataset_id
        for t in tables_by_ds.get(ds_id, []):
            if t.table_type in ("VIEW", "MATERIALIZED_VIEW"):
                continue
            dst_tbl = copy_table_data(
                bq_dst, t, cfg.dst_project, ds_id, cfg.skip_refs, cfg.dry_run
            )
            if dst_tbl is not None:
                copy_table_iam_policy(bq_src, bq_dst, t, dst_tbl.reference, cfg.dry_run)

    # Phase 2e: Routines
    for src_ds in src_datasets:
        ds_id = src_ds.dataset_id
        try:
            routines = list(bq_src.list_routines(src_ds.reference))
        except Exception:
            routines = []
        for r in routines:
            try:
                src_r: Routine = bq_src.get_routine(r.reference)
            except gex.NotFound:
                logging.warning("Skipping disappeared routine: %s", r.reference)
                continue

            # Skip this routine itself if listed
            if (ds_id.lower(), src_r.routine_id.lower()) in cfg.skip_refs:
                logging.info(
                    "Skipping routine per SKIP_REFS: %s.%s", ds_id, src_r.routine_id
                )
                continue

            dst_r = build_dst_routine_from_source(
                src_r=src_r,
                dst_project=cfg.dst_project,
                ds_id=ds_id,
                rewrite_projects=cfg.rewrite_project_refs,
                migrating_ds_ids=migrating_ds_ids,
                skip_refs=cfg.skip_refs,
                src_project=cfg.src_project,
            )

            # If routine language is SQL and it references any SKIP_REFS table, skip creating it
            # to avoid cross-project location issues (mirrors view behavior).
            try:
                lang = (getattr(src_r, "language", None) or "").upper()
            except Exception:
                lang = ""
            if lang == "SQL":
                original_body = getattr(src_r, "body", None) or ""
                hit = sql_mentions_any_skip_ref(original_body, cfg.skip_refs)
                if hit:
                    logging.info(
                        "Skipping ROUTINE %s.%s because it references SKIP_REFS table %s.%s",
                        ds_id,
                        src_r.routine_id,
                        hit[0],
                        hit[1],
                    )
                    continue

            if cfg.dry_run:
                logging.info(
                    "[DRY-RUN] Would create/update routine: %s", dst_r.reference
                )
                continue

            try:
                bq_dst.create_routine(dst_r)
                logging.info("Created routine: %s", dst_r.reference)
            except gex.Conflict:
                try:
                    update_routine_safe(bq_dst, dst_r)
                    logging.info("Updated routine: %s", dst_r.reference)
                except Exception as e:
                    if cfg.skip_missing_connections and is_missing_connection_error(e):
                        logging.warning(
                            "Skipping routine %s due to missing connection: %s",
                            dst_r.reference,
                            e,
                        )
                    else:
                        raise
            except Exception as e:
                # If the creation failed and we rewrote SQL, try once with original body.
                if cfg.rewrite_project_refs and (
                    getattr(dst_r, "language", "").upper() == "SQL"
                ):
                    try:
                        orig_body = getattr(src_r, "body", None)
                        if orig_body:
                            dst_r.body = orig_body
                            bq_dst.create_routine(dst_r)
                            logging.info(
                                "Created routine with original body (fallback): %s",
                                dst_r.reference,
                            )
                            continue
                    except Exception as e2:
                        e = e2
                if cfg.skip_missing_connections and is_missing_connection_error(e):
                    logging.warning(
                        "Skipping routine %s due to missing connection: %s",
                        dst_r.reference,
                        e,
                    )
                else:
                    # If it's any BadRequest, log and skip so the run can continue.
                    msg = str(e)
                    if isinstance(e, gex.BadRequest):
                        logging.warning(
                            "Skipping routine %s due to BadRequest: %s",
                            dst_r.reference,
                            msg,
                        )
                        continue
                    raise

    logging.info("Replication completed.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.error("Interrupted by user.")
        sys.exit(2)
