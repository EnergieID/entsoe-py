import pandas as pd
from zoneinfo import ZoneInfo

def test_zoneinfo_key_europe_amsterdam_is_valid():
    ZoneInfo("Europe/Amsterdam")

def test_entsoe_import_does_not_fail_due_to_timezone():
    import entsoe.entsoe as e
    assert isinstance(e.QUARTER_MTU_SDAC_GOLIVE, pd.Timestamp)
    assert e.QUARTER_MTU_SDAC_GOLIVE.tz is not None
