"""Tests for run_financial_sync --period-range helpers."""
import pytest

from quant_data_engine.tasks.run_financial_sync import quarter_ends_inclusive


def test_quarter_ends_20200630_to_20251231():
    qe = quarter_ends_inclusive("20200630", "20251231")
    assert qe[0] == "20200630"
    assert qe[-1] == "20251231"
    # 2020: 3; 2021-2024: 16; 2025: 4
    assert len(qe) == 23
    assert "20210331" in qe
    assert "20200331" not in qe


def test_quarter_ends_raises_on_inverted_range():
    with pytest.raises(ValueError, match="Start after end"):
        quarter_ends_inclusive("20251231", "20200630")
