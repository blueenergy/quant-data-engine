# MongoDB Backup to Volcengine TOS

This document describes the nightly MongoDB backup task in `quant-data-engine`.
The task is designed to run from the cron container and upload compressed
`mongodump` archives to Volcengine TOS via the S3-compatible API.

## What Gets Backed Up

By default, the task backs up one archive per database:

- `finance`
- `quant_analyzer`
- `quant_data`
- `quant_realtime`

The default list is resolved from:

- `MONGO_DB`
- `ANALYZER_DB_NAME`
- `DATA_DB_NAME`
- `REALTIME_DB_NAME`

Override the list with either:

```bash
MONGODB_BACKUP_DATABASES=finance,quant_analyzer
```

or:

```bash
python -m quant_data_engine.tasks.run_mongodb_backup --databases finance,quant_analyzer
```

## Required Runtime Tools

The task requires:

- `mongodump`, from MongoDB Database Tools.
- `boto3`, for S3/TOS uploads.

The Docker image installs both, so the cron container can run the task directly.

## Environment Variables

MongoDB:

```bash
MONGO_URI=mongodb://user:password@host:27017/?authSource=admin
MONGO_DB=finance
ANALYZER_DB_NAME=quant_analyzer
DATA_DB_NAME=quant_data
REALTIME_DB_NAME=quant_realtime
```

Backup target:

```bash
# Explicit opt-in. The cron job is a safe no-op unless this is enabled.
MONGODB_BACKUP_S3_UPLOAD_ENABLED=1
MONGODB_BACKUP_S3_ENDPOINT_URL=https://tos-s3-cn-beijing.volces.com
MONGODB_BACKUP_S3_BUCKET=wukongquant-backup
MONGODB_BACKUP_S3_PREFIX=mongodb/daily
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=cn-beijing
```

Optional:

```bash
MONGODB_BACKUP_DATABASES=finance,quant_analyzer,quant_data,quant_realtime
MONGODB_BACKUP_WORK_DIR=/tmp/quant-data-engine-mongodb-backup
MONGODB_BACKUP_MONGO_URI=mongodb://backup-user:password@host:27017/?authSource=admin
```

`MONGODB_BACKUP_MONGO_URI` can be used if the backup user is different from the
application user.

`MONGODB_BACKUP_S3_UPLOAD_ENABLED` defaults to disabled. This prevents multiple
deployments that share the same image and cron configuration from writing to the
same bucket. Enable it only in the single deployment that should own production
backups.

## Manual Commands

Dry run:

```bash
python -m quant_data_engine.tasks.run_mongodb_backup --dry-run
```

Create a local backup without uploading:

```bash
python -m quant_data_engine.tasks.run_mongodb_backup \
  --databases finance \
  --skip-upload \
  --keep-local
```

Run a full backup and upload:

```bash
MONGODB_BACKUP_S3_UPLOAD_ENABLED=1 python -m quant_data_engine.tasks.run_mongodb_backup
```

Use the shell wrapper:

```bash
MONGODB_BACKUP_S3_UPLOAD_ENABLED=1 /app/daily_market_sync.sh mongodb_backup
```

## Cron Schedule

Both `crontab.txt` and `crontab_domestic.txt` run the backup daily at 02:10
Beijing time:

```cron
10 2 * * * /app/daily_market_sync.sh mongodb_backup
```

This time avoids the A-share post-close sync window, the early morning global
market sync window, and the Saturday 02:30 weekly stock sync.

The cron entry is intentionally present in both crontab files, but it will only
perform a real backup when `MONGODB_BACKUP_S3_UPLOAD_ENABLED=1` is configured in
that deployment.

## Object Layout

For a run on `20260521`, files are uploaded as:

```text
s3://wukongquant-backup/mongodb/daily/20260521/finance.archive.gz
s3://wukongquant-backup/mongodb/daily/20260521/quant_analyzer.archive.gz
s3://wukongquant-backup/mongodb/daily/20260521/quant_data.archive.gz
s3://wukongquant-backup/mongodb/daily/20260521/quant_realtime.archive.gz
s3://wukongquant-backup/mongodb/daily/20260521/manifest.json
```

`manifest.json` includes:

- database name
- archive filename
- archive size
- SHA-256 checksum
- S3 key
- masked Mongo URI
- `mongodump` version
- start and completion timestamps

## TOS Permission Model

Use a dedicated access key with the smallest useful permission set:

- write objects under `mongodb/*`
- read objects for restore testing
- optionally delete objects only after a retention policy is implemented

The bucket should not be public. Enable server-side encryption if available.

## MongoDB 7.0 Restore Compatibility

Backups are created with MongoDB Database Tools using:

```bash
mongodump --archive --gzip --db <database>
```

This format is compatible with `mongorestore --archive --gzip`. For MongoDB 7.0,
use a recent MongoDB Database Tools 100.x release. Restoring from an older source
MongoDB version into MongoDB 7.0 is generally supported for normal BSON
documents and standard indexes.

Do not restore directly over production first. Restore into a test namespace:

```bash
mongorestore \
  --uri "mongodb://user:password@mongo7-host:27017/?authSource=admin" \
  --archive=finance.archive.gz \
  --gzip \
  --nsFrom="finance.*" \
  --nsTo="finance_restore.*"
```

Then verify:

```javascript
db = db.getSiblingDB("finance_restore")
db.users.countDocuments()
db.analysis_tasks.countDocuments()
db.market_insight_events.countDocuments()
```

After verification, drop the restore test database if it is no longer needed:

```javascript
db.getSiblingDB("finance_restore").dropDatabase()
```

## Operational Checklist

Before enabling the cron job in production:

1. Confirm the container has `mongodump`:

   ```bash
   mongodump --version
   ```

2. Run dry-run:

   ```bash
   python -m quant_data_engine.tasks.run_mongodb_backup --dry-run
   ```

3. Run a single-database local backup:

   ```bash
   python -m quant_data_engine.tasks.run_mongodb_backup --databases finance --skip-upload --keep-local
   ```

4. Run a single-database upload test to a test prefix:

   ```bash
   MONGODB_BACKUP_S3_UPLOAD_ENABLED=1 \
     python -m quant_data_engine.tasks.run_mongodb_backup --databases finance --prefix mongodb/test
   ```

5. Restore the test backup into a MongoDB 7.0 test database.

6. Only then enable the daily cron in production.

## Retention Policy

The first version does not delete remote backups. Prefer configuring lifecycle
rules on the TOS bucket first, for example:

- daily backups retained for 14-30 days
- weekly backups retained for 8-12 weeks
- monthly backups retained for 12 months

Remote deletion can be automated later after restore tests are routine.
