from quant_data_engine.constants.index_constituents_sync import DEFAULT_SYNC_PROFILES


def test_default_sync_profiles_include_csi2000():
    by_key = {profile["store_key"]: profile for profile in DEFAULT_SYNC_PROFILES}

    assert by_key["csi2000"]["ts_code"] == "932000.CSI"
    assert "932000.SH" in by_key["csi2000"]["alternates"]
