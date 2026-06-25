"""theme_lag_trade_execution_writer：股数折算、订阅列表、计划应用、整段写入与 run_validation 薄封装。"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pytest
from pymongo.errors import OperationFailure

from quant_data_engine.config.settings import Settings
from quant_data_engine.tasks import theme_lag_trade_execution_writer as w


def _settings() -> Settings:
    return Settings(
        mongo_uri="mongodb://x",
        mongo_db="finance",
        trading_db="quant_trading",
        data_db="quant_data",
        realtime_db="quant_realtime",
        tushare_token=None,
        rate_limit_per_sec=1.0,
        job_status_sync_uri=None,
    )


def test_shares_for_notional_rounds_down_to_lot():
    assert w._shares_for_notional(10.0, 10_000.0) == 1000
    assert w._shares_for_notional(10.0, 10_050.0) == 1000
    assert w._shares_for_notional(500.0, 10_000.0) == 0  # 不足 100 股


def test_shares_zero_on_bad_input():
    assert w._shares_for_notional(0, 10_000) == 0
    assert w._shares_for_notional(10, 0) == 0


def test_list_subscriber_user_ids_dedup_and_order():
    class FakeCursor(List[Dict[str, Any]]):
        pass

    class FakeSubs:
        def __init__(self, rows: List[Dict[str, Any]]):
            self.rows = rows

        def find(self, query: Dict[str, Any], projection: Any = None):
            sk = query["index_store_key"]
            return [r for r in self.rows if r.get("index_store_key") == sk and r.get("enabled") is not False]

    class FakeDB:
        def __init__(self, rows: List[Dict[str, Any]]):
            self._subs = FakeSubs(rows)

        def __getitem__(self, name: str):
            if name == w.SUBS_COLLECTION:
                return self._subs
            raise KeyError(name)

    class FakeClient:
        def __init__(self, rows: List[Dict[str, Any]]):
            self._rows = rows

        def __getitem__(self, db: str) -> FakeDB:
            return FakeDB(self._rows)

    rows = [
        {"user_id": "a", "index_store_key": "csi1000", "enabled": True},
        {"user_id": "a", "index_store_key": "csi1000", "enabled": True},  # dup user, second row ignored by dedup
        {"user_id": "b", "index_store_key": "csi1000", "enabled": True},
    ]
    uids = w._list_subscriber_user_ids(FakeClient(rows), "finance", "csi1000")
    assert uids == ["a", "b"]


def test_build_paper_skips_and_pairs():
    per = [
        {"symbol": "600000.SH", "p0": 10.0, "outcome": "expire", "exit_trade_date": "20260102", "exit_price": 10.1},
        {"symbol": "BAD", "p0": 0, "outcome": "expire", "exit_trade_date": "20260102", "exit_price": 10.1},
        {"symbol": "Y", "p0": 10.0, "outcome": "insufficient_data", "exit_trade_date": "20260102", "exit_price": 10.1},
    ]
    docs, pairs, skipped = w._build_paper_execution_docs("u1", "20260101", "csi1000", per, 10_000.0, 1_700_000_000.0)
    assert pairs == 1
    assert skipped == 2
    assert len(docs) == 2
    assert docs[0]["order_id"].endswith("|BUY")
    assert docs[1]["order_id"].endswith("|SELL")
    assert docs[0]["user_id"] == "u1"
    assert "tl|" in docs[0]["order_id"]


def test_order_id_differs_between_users():
    per = [
        {
            "symbol": "600000.SH",
            "p0": 10.0,
            "outcome": "expire",
            "exit_trade_date": "20260102",
            "exit_price": 10.1,
        }
    ]
    a, _, _ = w._build_paper_execution_docs("user_a", "20260101", "csi1000", per, 10_000.0, 1.0)
    b, _, _ = w._build_paper_execution_docs("user_b", "20260101", "csi1000", per, 10_000.0, 1.0)
    assert a[0]["order_id"] != b[0]["order_id"]


class _FakeDeleteResult:
    def __init__(self, n: int) -> None:
        self.deleted_count = n


class FakeExecColl:
    """记录 delete/insert 顺序与是否传入 session。"""

    def __init__(self, fail_second_insert: bool = False) -> None:
        self.ops: List[Tuple[str, Any]] = []
        self.fail_second_insert = fail_second_insert
        self._insert_count = 0

    def delete_many(self, filt: Dict[str, Any], session: Any = None) -> _FakeDeleteResult:
        self.ops.append(("delete", filt, session))
        return _FakeDeleteResult(1)

    def insert_many(self, docs: List[Dict[str, Any]], session: Any = None) -> None:
        self._insert_count += 1
        if self.fail_second_insert and self._insert_count == 2:
            raise RuntimeError("simulated insert failure")
        self.ops.append(("insert", len(docs), session))


class FakeClientTxnOK:
    """支持 ``with session.start_transaction()`` 的假 client。"""

    def __init__(self) -> None:
        self.in_tx = False

    class _Session:
        def __init__(self, outer: "FakeClientTxnOK") -> None:
            self._outer = outer

        def __enter__(self) -> "FakeClientTxnOK._Session":
            return self

        def __exit__(self, *args: Any) -> None:
            pass

        def start_transaction(self) -> "FakeClientTxnOK._Tx":
            return FakeClientTxnOK._Tx(self._outer)

    class _Tx:
        def __init__(self, outer: "FakeClientTxnOK") -> None:
            self._outer = outer

        def __enter__(self) -> None:
            self._outer.in_tx = True

        def __exit__(self, *args: Any) -> None:
            self._outer.in_tx = False

    def start_session(self) -> "FakeClientTxnOK._Session":
        return FakeClientTxnOK._Session(self)


class FakeClientTxnFail:
    """``start_session`` 即抛错，迫使走顺序路径。"""

    def start_session(self) -> Any:
        raise OperationFailure("Transaction unsupported", code=20)


def test_apply_plans_uses_transaction_when_session_works() -> None:
    ex = FakeExecColl()
    plans: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]] = [
        ({"user_id": "a"}, [{"_id": 1}]),
        ({"user_id": "b"}, [{"_id": 2}]),
    ]
    removed, inserted = w._apply_paper_trade_plans(FakeClientTxnOK(), ex, plans)  # type: ignore[arg-type]
    assert removed == 2
    assert inserted == 2
    assert all(op[2] is not None for op in ex.ops if op[0] == "delete")


def test_apply_plans_sequential_when_transaction_unavailable() -> None:
    ex = FakeExecColl()
    plans = [({"user_id": "a"}, [{"x": 1}])]
    removed, inserted = w._apply_paper_trade_plans(FakeClientTxnFail(), ex, plans)  # type: ignore[arg-type]
    assert removed == 1
    assert inserted == 1
    assert ex.ops[0][2] is None  # 无 session


def test_write_disabled_when_env_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("THEME_LAG_WRITE_SIZED_TRADE_EXECUTIONS", raising=False)
    out = w.write_sized_paper_trades(
        _settings(),
        {"anchor_trade_date": "20260101", "index_store_key": "csi1000", "per_symbol": []},
    )
    assert out.get("enabled") is False


def test_write_no_subscribers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("THEME_LAG_WRITE_SIZED_TRADE_EXECUTIONS", "1")

    class EmptySubs:
        def find(self, *a: Any, **k: Any) -> List[Any]:
            return []

    class DB:
        def __getitem__(self, n: str) -> Any:
            if n == w.SUBS_COLLECTION:
                return EmptySubs()
            raise KeyError(n)

    class Cli:
        def __getitem__(self, _: str) -> DB:
            return DB()

        def close(self) -> None:
            pass

    monkeypatch.setattr(w, "MongoClient", lambda *a, **k: Cli())
    out = w.write_sized_paper_trades(
        _settings(),
        {"anchor_trade_date": "20260101", "index_store_key": "csi1000", "per_symbol": []},
    )
    assert out.get("reason") == "no_subscribers_for_index"


def test_write_invalid_notional(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("THEME_LAG_WRITE_SIZED_TRADE_EXECUTIONS", "1")
    monkeypatch.setenv("THEME_LAG_NOTIONAL_CNY_PER_SYMBOL", "0")

    class Cli:
        def close(self) -> None:
            pass

    monkeypatch.setattr(w, "MongoClient", lambda *a, **k: Cli())
    out = w.write_sized_paper_trades(
        _settings(),
        {"anchor_trade_date": "20260101", "index_store_key": "csi1000", "per_symbol": []},
    )
    assert "error" in out


def test_write_multi_subscriber_fanout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("THEME_LAG_WRITE_SIZED_TRADE_EXECUTIONS", "1")

    subs_rows = [
        {"user_id": "u1", "index_store_key": "csi1000", "enabled": True},
        {"user_id": "u2", "index_store_key": "csi1000", "enabled": True},
    ]
    ex = FakeExecColl()

    class Subs:
        def find(self, query: Dict[str, Any], projection: Any = None) -> List[Dict[str, Any]]:
            sk = query["index_store_key"]
            return [r for r in subs_rows if r.get("index_store_key") == sk and r.get("enabled") is not False]

    class DB:
        def __getitem__(self, name: str) -> Any:
            if name == w.SUBS_COLLECTION:
                return Subs()
            if name == "trade_executions":
                return ex
            raise KeyError(name)

    class Cli:
        def __init__(self) -> None:
            self._fail_tx = True

        def __getitem__(self, _: str) -> DB:
            return DB()

        def start_session(self) -> Any:
            raise OperationFailure("no rs", code=20)

        def close(self) -> None:
            pass

    monkeypatch.setattr(w, "MongoClient", lambda *a, **k: Cli())
    per = [
        {
            "symbol": "600000.SH",
            "p0": 10.0,
            "outcome": "expire",
            "exit_trade_date": "20260102",
            "exit_price": 10.1,
        }
    ]
    out = w.write_sized_paper_trades(
        _settings(),
        {"anchor_trade_date": "20260101", "index_store_key": "csi1000", "per_symbol": per},
    )
    assert out["subscriber_count"] == 2
    assert out["inserted"] == 4  # 2 users × (BUY+SELL)
    deletes = [o for o in ex.ops if o[0] == "delete"]
    assert len(deletes) == 2
    assert {d[1]["user_id"] for d in deletes} == {"u1", "u2"}


def test_invoke_theme_lag_paper_trade_writer_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    from quant_data_engine.tasks import run_theme_lag_candidate_validation as mod

    called: Dict[str, Any] = {}

    def fake_write(settings: Settings, doc: Dict[str, Any]) -> Dict[str, Any]:
        called["anchor"] = doc.get("anchor_trade_date")
        return {"enabled": True, "inserted": 0, "user_ids": []}

    monkeypatch.setattr(
        "quant_data_engine.tasks.theme_lag_trade_execution_writer.write_sized_paper_trades",
        fake_write,
    )
    doc = {"anchor_trade_date": "20260201", "index_store_key": "csi1000"}
    out = mod.invoke_theme_lag_paper_trade_writer(
        _settings(),
        doc,
    )
    assert called["anchor"] == "20260201"
    assert out["enabled"] is True
