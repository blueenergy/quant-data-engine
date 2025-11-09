import pandas as pd
import pytest
from quality.validators import IndexPriceValidator

def test_validator_missing_columns():
    df = pd.DataFrame({"open": [1, 2], "high": [3, 4]})  # missing low, close
    validator = IndexPriceValidator()
    result = validator.validate(df)
    assert not result["is_valid"]
    assert any("missing:low" in issue for issue in result["issues"])
    assert any("missing:close" in issue for issue in result["issues"])

def test_validator_non_positive_values():
    df = pd.DataFrame({"open": [0, 2], "high": [3, -1], "low": [1, 2], "close": [1, 2]})
    validator = IndexPriceValidator()
    result = validator.validate(df)
    assert not result["is_valid"]
    assert any("non_positive_open" in issue for issue in result["issues"])
    assert any("non_positive_high" in issue for issue in result["issues"])

def test_validator_high_lt_low():
    df = pd.DataFrame({"open": [1, 2], "high": [1, 2], "low": [2, 1], "close": [1, 2]})
    validator = IndexPriceValidator()
    result = validator.validate(df)
    assert not result["is_valid"]
    assert any("high_lt_low" in issue for issue in result["issues"])
