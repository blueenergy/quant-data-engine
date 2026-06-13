from unittest.mock import MagicMock

import pandas as pd

from quant_data_engine.repositories.institutional_repository import InstitutionalRepository


def test_replace_share_float_hash_upserts_and_dedupes_batch():
    repo = InstitutionalRepository.__new__(InstitutionalRepository)
    repo.share_float = MagicMock()
    result = MagicMock()
    result.upserted_count = 1
    result.modified_count = 0
    repo.share_float.bulk_write.return_value = result

    df = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "ann_date": "20150520",
                "float_date": "20180521",
                "float_share": 252247983.0,
                "holder_name": "中国平安保险(集团)股份有限公司-集团本级-自有资金",
            },
            {
                "ts_code": "000001.SZ",
                "ann_date": "20150520",
                "float_date": "20180521",
                "float_share": 252247983.0,
                "holder_name": "中国平安保险(集团)股份有限公司-集团本级-自有资金",
            },
        ]
    )

    changed = repo.replace_share_float("000001.SZ", df)

    assert changed == 1
    ops = repo.share_float.bulk_write.call_args[0][0]
    assert len(ops) == 1
    assert set(ops[0]._filter.keys()) == {"_uid"}
    assert ops[0]._doc["$set"]["_uid"] == ops[0]._filter["_uid"]
    assert ops[0]._doc["$set"]["ts_code"] == "000001.SZ"
