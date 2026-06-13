from quant_data_engine.tasks.run_sw_industry_backfill import _resample_daily_to_period


class _Cursor(list):
    def sort(self, field, direction):
        return _Cursor(sorted(self, key=lambda row: row.get(field, ""), reverse=direction < 0))


class _DailyCollection:
    def __init__(self, rows):
        self.rows = rows

    def distinct(self, field):
        return sorted({row.get(field) for row in self.rows if row.get(field)})

    def find_one(self, filt=None, projection=None, sort=None):
        rows = [dict(row) for row in self.rows]
        if sort:
            for field, direction in reversed(sort):
                rows.sort(key=lambda row: row.get(field) or "", reverse=direction < 0)
        return rows[0] if rows else None

    def find(self, filt, projection=None):
        ts_code = filt.get("ts_code")
        date_cond = filt.get("trade_date") or {}
        start = date_cond.get("$gte")
        end = date_cond.get("$lte")
        rows = [
            dict(row)
            for row in self.rows
            if row.get("ts_code") == ts_code
            and (not start or row.get("trade_date") >= start)
            and (not end or row.get("trade_date") <= end)
        ]
        return _Cursor(rows)


class _PeriodCollection:
    def __init__(self):
        self.deleted = []

    def delete_many(self, filt):
        self.deleted.append(filt)


class _Repo:
    def __init__(self, rows):
        self.daily_coll = _DailyCollection(rows)
        self.monthly_coll = _PeriodCollection()
        self.weekly_coll = _PeriodCollection()
        self.meta = {"sw_monthly_backfill": "20260613"}
        self.monthly_records = []

    def get_meta(self, key):
        return self.meta.get(key)

    def set_meta(self, key, value):
        self.meta[key] = value

    def upsert_monthly_batch(self, records):
        self.monthly_records.extend(records)
        return len(records)


def test_monthly_resume_rebuilds_recent_full_windows():
    rows = [
        {
            "ts_code": "801001.SI",
            "trade_date": "20260529",
            "open": 100,
            "high": 120,
            "low": 95,
            "close": 110,
            "vol": 1000,
            "amount": 2000,
        },
        {
            "ts_code": "801001.SI",
            "trade_date": "20260612",
            "open": 111,
            "high": 130,
            "low": 108,
            "close": 125,
            "vol": 800,
            "amount": 1600,
        },
    ]
    repo = _Repo(rows)

    total = _resample_daily_to_period(
        repo,
        period="monthly",
        start="",
        end="20260614",
        resume=True,
        dry_run=False,
    )

    trade_dates = [row["trade_date"] for row in repo.monthly_records]
    assert total == 2
    assert "20260529" in trade_dates
    assert "20260612" in trade_dates
    assert repo.monthly_coll.deleted
    assert repo.meta["sw_monthly_backfill"] == "20260612"


def test_monthly_resume_does_not_overwrite_when_daily_is_stale():
    rows = [
        {
            "ts_code": "801001.SI",
            "trade_date": "20260424",
            "open": 100,
            "high": 120,
            "low": 95,
            "close": 110,
            "vol": 1000,
            "amount": 2000,
        }
    ]
    repo = _Repo(rows)

    total = _resample_daily_to_period(
        repo,
        period="monthly",
        start="",
        end="20260614",
        resume=True,
        dry_run=False,
    )

    assert total == 0
    assert repo.monthly_records == []
    assert repo.monthly_coll.deleted == []
