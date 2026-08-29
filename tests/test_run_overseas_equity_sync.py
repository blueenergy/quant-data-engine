from quant_data_engine.tasks.run_overseas_equity_sync import _interval, _job_type, parse_args


def test_parse_args_market_and_dataset():
    args = parse_args(["--market", "hk", "--dataset", "basic"])
    assert args.market == "hk"
    assert args.dataset == "basic"


def test_job_type():
    assert _job_type("hk", "daily") == "hk_daily"
    assert _job_type("us", "tradecal") == "us_tradecal"


def test_hk_daily_default_interval():
    assert _interval("hk", "daily", None) == 3605.0
    assert _interval("us", "daily", None) == 0.0
    assert _interval("hk", "basic", None) == 0.0
    assert _interval("hk", "daily", 10.0) == 10.0


def test_parse_args_force_and_want_adj():
    args = parse_args(["--market", "hk", "--dataset", "daily", "--force", "--want-adj"])
    assert args.force is True
    assert args.want_adj is True
