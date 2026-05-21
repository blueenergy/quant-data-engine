"""Back up MongoDB databases to an S3-compatible object store.

This task is designed for the quant-data-engine cron container. It uses
``mongodump`` for wire-format-compatible backups, then uploads one archive per
database plus a manifest to Volcengine TOS or any S3-compatible endpoint.

Example:
    python -m quant_data_engine.tasks.run_mongodb_backup --dry-run
    python -m quant_data_engine.tasks.run_mongodb_backup --databases finance --skip-upload --keep-local
    MONGODB_BACKUP_S3_UPLOAD_ENABLED=1 python -m quant_data_engine.tasks.run_mongodb_backup
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

from dotenv import load_dotenv

from quant_data_engine.config.settings import get_settings


DEFAULT_PREFIX = "mongodb/daily"


@dataclass
class BackupConfig:
    mongo_uri: str
    databases: List[str]
    bucket: Optional[str]
    endpoint_url: Optional[str]
    prefix: str
    work_dir: Path
    dry_run: bool
    skip_upload: bool
    keep_local: bool
    s3_upload_enabled: bool


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp() -> str:
    return _utc_now().strftime("%Y%m%dT%H%M%SZ")


def _date_path() -> str:
    return _utc_now().strftime("%Y%m%d")


def _split_csv(value: str) -> List[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _dedupe(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out


def _mask_uri(uri: str) -> str:
    """Mask credentials before logging a MongoDB URI."""
    if not uri:
        return ""
    if uri.startswith("mongodb+srv://"):
        scheme = "mongodb+srv"
        rest = uri[len("mongodb+srv://") :]
    elif uri.startswith("mongodb://"):
        scheme = "mongodb"
        rest = uri[len("mongodb://") :]
    else:
        return "***"
    if "@" not in rest:
        return uri
    _, host_and_path = rest.rsplit("@", 1)
    return f"{scheme}://***:***@{host_and_path}"


def _s3_key(prefix: str, date_part: str, filename: str) -> str:
    clean_prefix = str(prefix or DEFAULT_PREFIX).strip("/")
    return f"{clean_prefix}/{date_part}/{filename}"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_size(path: Path) -> int:
    return path.stat().st_size


def _load_env() -> None:
    # Prefer the local project .env, then allow the shared quantFinance .env
    # fallback already handled by settings.py.
    load_dotenv(override=False)


def _default_databases() -> List[str]:
    settings = get_settings()
    return _dedupe(
        [
            os.getenv("MONGO_DB", settings.mongo_db),
            os.getenv("ANALYZER_DB_NAME", "quant_analyzer"),
            os.getenv("DATA_DB_NAME", settings.data_db),
            os.getenv("REALTIME_DB_NAME", settings.realtime_db),
        ]
    )


def _build_config(args: argparse.Namespace) -> BackupConfig:
    _load_env()
    settings = get_settings()
    databases = _split_csv(args.databases or os.getenv("MONGODB_BACKUP_DATABASES", ""))
    if not databases:
        databases = _default_databases()

    mongo_uri = (
        args.mongo_uri
        or os.getenv("MONGODB_BACKUP_MONGO_URI", "").strip()
        or os.getenv("MONGO_URI", "").strip()
        or settings.mongo_uri
    )
    bucket = args.bucket or os.getenv("MONGODB_BACKUP_S3_BUCKET", "").strip() or None
    endpoint_url = (
        args.endpoint_url
        or os.getenv("MONGODB_BACKUP_S3_ENDPOINT_URL", "").strip()
        or os.getenv("TOS_ENDPOINT_URL", "").strip()
        or None
    )
    work_dir = Path(args.work_dir or os.getenv("MONGODB_BACKUP_WORK_DIR", "/tmp/quant-data-engine-mongodb-backup"))
    return BackupConfig(
        mongo_uri=mongo_uri,
        databases=_dedupe(databases),
        bucket=bucket,
        endpoint_url=endpoint_url,
        prefix=args.prefix or os.getenv("MONGODB_BACKUP_S3_PREFIX", DEFAULT_PREFIX),
        work_dir=work_dir,
        dry_run=bool(args.dry_run),
        skip_upload=bool(args.skip_upload),
        keep_local=bool(args.keep_local),
        s3_upload_enabled=bool(args.enable_upload or _bool_env("MONGODB_BACKUP_S3_UPLOAD_ENABLED", False)),
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Back up MongoDB databases to Volcengine TOS/S3.")
    parser.add_argument("--mongo-uri", help="MongoDB URI. Defaults to MONGODB_BACKUP_MONGO_URI, then MONGO_URI.")
    parser.add_argument("--databases", help="Comma-separated database list. Defaults to finance/analyzer/data/realtime.")
    parser.add_argument("--bucket", help="S3 bucket. Defaults to MONGODB_BACKUP_S3_BUCKET.")
    parser.add_argument("--endpoint-url", help="S3-compatible endpoint. Defaults to MONGODB_BACKUP_S3_ENDPOINT_URL.")
    parser.add_argument("--prefix", default=None, help=f"S3 key prefix. Defaults to {DEFAULT_PREFIX!r}.")
    parser.add_argument("--work-dir", default=None, help="Local working directory for temporary archives.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned actions without dumping or uploading.")
    parser.add_argument("--skip-upload", action="store_true", help="Create local archives and manifest but skip S3 upload.")
    parser.add_argument("--keep-local", action="store_true", help="Keep temporary local archives after success.")
    parser.add_argument(
        "--enable-upload",
        action="store_true",
        help="Enable S3 upload for this run. Cron usually uses MONGODB_BACKUP_S3_UPLOAD_ENABLED=1 instead.",
    )
    return parser.parse_args()


def _run_mongodump(config: BackupConfig, database: str, output_path: Path) -> None:
    cmd = [
        "mongodump",
        "--uri",
        config.mongo_uri,
        "--db",
        database,
        f"--archive={output_path}",
        "--gzip",
    ]
    safe_cmd = ["mongodump", "--uri", _mask_uri(config.mongo_uri), "--db", database, f"--archive={output_path}", "--gzip"]
    print(f"[backup] running: {' '.join(safe_cmd)}")
    subprocess.run(cmd, check=True)


def _upload_file(path: Path, bucket: str, key: str, endpoint_url: Optional[str]) -> None:
    import boto3

    kwargs = {}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    client = boto3.client("s3", **kwargs)
    print(f"[backup] uploading {path.name} -> s3://{bucket}/{key}")
    client.upload_file(str(path), bucket, key)


def _write_manifest(path: Path, manifest: dict) -> None:
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _validate_config(config: BackupConfig) -> None:
    if not config.mongo_uri:
        raise SystemExit("MONGODB_BACKUP_MONGO_URI or MONGO_URI is required.")
    if not config.databases:
        raise SystemExit("No databases selected for backup.")
    if config.s3_upload_enabled and not config.skip_upload and not config.dry_run and not config.bucket:
        raise SystemExit("MONGODB_BACKUP_S3_BUCKET is required unless --skip-upload or --dry-run is used.")


def _mongodump_version() -> str:
    binary = shutil.which("mongodump")
    if not binary:
        return "missing"
    try:
        result = subprocess.run([binary, "--version"], check=False, capture_output=True, text=True)
    except Exception as exc:  # pragma: no cover - diagnostic only
        return f"error: {exc}"
    return (result.stdout or result.stderr or "").strip().splitlines()[0] if (result.stdout or result.stderr) else "unknown"


def run_backup(config: BackupConfig) -> dict:
    _validate_config(config)
    date_part = _date_path()
    run_id = _timestamp()
    run_dir = config.work_dir / run_id
    print("[backup] mongo_uri:", _mask_uri(config.mongo_uri))
    print("[backup] databases:", ", ".join(config.databases))
    print("[backup] bucket:", config.bucket or "(skip)")
    print("[backup] endpoint:", config.endpoint_url or "(default)")
    print("[backup] prefix:", config.prefix)
    print("[backup] s3_upload_enabled:", config.s3_upload_enabled)
    print("[backup] work_dir:", run_dir)
    print("[backup] mongodump:", _mongodump_version())

    if config.dry_run:
        return {
            "success": True,
            "dry_run": True,
            "databases": config.databases,
            "bucket": config.bucket,
            "prefix": config.prefix,
            "date_path": date_part,
            "s3_upload_enabled": config.s3_upload_enabled,
        }

    if not config.skip_upload and not config.s3_upload_enabled:
        print("[backup] S3 upload is disabled. Set MONGODB_BACKUP_S3_UPLOAD_ENABLED=1 to enable scheduled backups.")
        return {
            "success": True,
            "skipped": True,
            "reason": "s3_upload_disabled",
            "databases": config.databases,
            "date_path": date_part,
        }

    run_dir.mkdir(parents=True, exist_ok=True)
    started_at = _utc_now().isoformat()
    manifest = {
        "backup_type": "mongodb",
        "started_at": started_at,
        "completed_at": None,
        "date_path": date_part,
        "run_id": run_id,
        "mongo_uri": _mask_uri(config.mongo_uri),
        "databases": [],
        "mongodump_version": _mongodump_version(),
        "s3_bucket": config.bucket,
        "s3_prefix": config.prefix,
        "s3_upload_enabled": config.s3_upload_enabled,
    }

    try:
        for database in config.databases:
            archive = run_dir / f"{database}.archive.gz"
            _run_mongodump(config, database, archive)
            checksum = _sha256_file(archive)
            size_bytes = _file_size(archive)
            key = _s3_key(config.prefix, date_part, archive.name)
            item = {
                "database": database,
                "filename": archive.name,
                "size_bytes": size_bytes,
                "sha256": checksum,
                "s3_key": key,
            }
            if not config.skip_upload:
                _upload_file(archive, config.bucket or "", key, config.endpoint_url)
            manifest["databases"].append(item)

        manifest["completed_at"] = _utc_now().isoformat()
        manifest_path = run_dir / "manifest.json"
        _write_manifest(manifest_path, manifest)
        if not config.skip_upload:
            _upload_file(manifest_path, config.bucket or "", _s3_key(config.prefix, date_part, "manifest.json"), config.endpoint_url)
        print(json.dumps({"success": True, "manifest": manifest}, ensure_ascii=False, indent=2))
        return {"success": True, "manifest": manifest}
    finally:
        if not config.keep_local and run_dir.exists():
            shutil.rmtree(run_dir, ignore_errors=True)
            print(f"[backup] removed local work directory: {run_dir}")


def main() -> int:
    args = _parse_args()
    config = _build_config(args)
    try:
        result = run_backup(config)
    except subprocess.CalledProcessError as exc:
        print(f"[backup] command failed with exit code {exc.returncode}", file=sys.stderr)
        return exc.returncode or 1
    except Exception as exc:
        print(f"[backup] failed: {exc}", file=sys.stderr)
        return 1
    if config.dry_run:
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
