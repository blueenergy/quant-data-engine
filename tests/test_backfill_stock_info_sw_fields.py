from datetime import datetime

from quant_data_engine.tasks.backfill_stock_info_sw_fields import (
    _stock_info_match,
    _sw_update_doc,
    build_stock_info_sw_ops,
)


def test_stock_info_match_handles_ts_code_and_short_symbol():
    match = _stock_info_match("688111.SH")

    assert match == {
        "$or": [
            {"ts_code": "688111.SH"},
            {"ts_code": "688111"},
            {"symbol": "688111.SH"},
            {"symbol": "688111"},
        ]
    }


def test_sw_update_doc_maps_names_codes_and_metadata():
    now = datetime(2026, 6, 23, 13, 0, 0)
    member = {
        "l1_code": "801750.SI",
        "l1_name": "计算机",
        "l2_code": "801751.SI",
        "l2_name": "软件开发",
        "l3_code": "851751.SI",
        "l3_name": "横向通用软件",
        "is_new": "Y",
        "in_date": "20211213",
        "out_date": None,
    }

    doc = _sw_update_doc(member, src="SW2021", now=now)

    assert doc["sw_l1"] == "计算机"
    assert doc["sw_l1_code"] == "801750.SI"
    assert doc["sw_l2"] == "软件开发"
    assert doc["sw_l2_code"] == "801751.SI"
    assert doc["sw_l3"] == "横向通用软件"
    assert doc["sw_l3_code"] == "851751.SI"
    assert doc["sw_src"] == "SW2021"
    assert doc["sw_updated_at"] is now


def test_build_stock_info_sw_ops_deduplicates_members_and_respects_limit():
    now = datetime(2026, 6, 23, 13, 0, 0)
    members = [
        {
            "con_code": "688111.SH",
            "l1_code": "801750.SI",
            "l1_name": "计算机",
            "l2_code": "801751.SI",
            "l2_name": "软件开发",
            "l3_code": "851751.SI",
            "l3_name": "横向通用软件",
            "is_new": "Y",
        },
        {
            # Duplicate con_code should be ignored; cursor sort already puts latest first.
            "con_code": "688111.SH",
            "l1_code": "OLD",
            "l1_name": "旧行业",
        },
        {
            "con_code": "300750.SZ",
            "l1_code": "801730.SI",
            "l1_name": "电力设备",
        },
    ]

    ops = build_stock_info_sw_ops(members, src="SW2021", now=now, limit=1)

    assert len(ops) == 1
    assert ops[0]._filter["$or"][0] == {"ts_code": "688111.SH"}
    assert ops[0]._doc["$set"]["sw_l1"] == "计算机"
    assert ops[0]._doc["$set"]["sw_l2"] == "软件开发"
