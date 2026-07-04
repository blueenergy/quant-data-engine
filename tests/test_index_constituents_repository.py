from quant_data_engine.repositories.index_constituents_repository import IndexConstituentsRepository


class _FakeCollection:
    def __init__(self):
        self.docs = []
        self.deleted = []

    def delete_many(self, query):
        self.deleted.append(query)
        self.docs = [d for d in self.docs if not _matches(d, query)]
        return type("Result", (), {"deleted_count": 0})()

    def insert_many(self, docs):
        self.docs.extend(docs)
        return type("Result", (), {"inserted_ids": list(range(len(docs)))})()

    def create_index(self, *args, **kwargs):
        return None


def _matches(doc, query):
    return all(doc.get(k) == v for k, v in query.items())


def test_replace_for_weight_trade_date_replaces_snapshot_only():
    col = _FakeCollection()
    repo = IndexConstituentsRepository.__new__(IndexConstituentsRepository)
    repo._col = col

    repo.replace_for_weight_trade_date(
        "csi1000",
        "20230131",
        [{"index_code": "csi1000", "weight_trade_date": "20230131", "update_date": "20230201", "symbol": "000001.SZ"}],
    )
    repo.replace_for_weight_trade_date(
        "csi1000",
        "20230630",
        [{"index_code": "csi1000", "weight_trade_date": "20230630", "update_date": "20230701", "symbol": "000002.SZ"}],
    )

    assert len(col.docs) == 2
    assert col.deleted[-1] == {"index_code": "csi1000", "weight_trade_date": "20230630"}

    repo.replace_for_weight_trade_date(
        "csi1000",
        "20230131",
        [{"index_code": "csi1000", "weight_trade_date": "20230131", "update_date": "20230202", "symbol": "000003.SZ"}],
    )
    symbols = sorted(d["symbol"] for d in col.docs if d["weight_trade_date"] == "20230131")
    assert symbols == ["000003.SZ"]
    assert any(d["symbol"] == "000002.SZ" for d in col.docs)
