from datetime import datetime

from quant_data_engine.tasks.run_index_dailybasic_sync import _resolve_indices, _year_windows


def test_resolve_indices_accepts_profile_keys_and_raw_codes():
    resolved = _resolve_indices(["hs300", "000905.SH"])
    assert resolved["000300.SH"] == "沪深300"
    assert resolved["000905.SH"] == "000905.SH"


def test_year_windows_split_multi_year_range():
    windows = _year_windows("20231215", "20250201")
    assert windows[0] == ("20231215", "20231231")
    assert windows[1] == ("20240101", "20241231")
    assert windows[2] == ("20250101", "20250201")
    assert _year_windows("20260102", "20260101") == []
    assert datetime.strptime(windows[0][0], "%Y%m%d")
