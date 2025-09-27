import pytest
import pandas as pd
from unittest.mock import patch, Mock
from entsoe.entsoe import EntsoeRawClient


class TestUtils:
    
    def test_datetime_to_str_utc(self):
        dt = pd.Timestamp('2023-01-01 12:30:00', tz='UTC')
        result = EntsoeRawClient._datetime_to_str(dt)
        assert result == "202301011200"
    
    def test_datetime_to_str_timezone_aware(self):
        dt = pd.Timestamp('2023-01-01 12:30:00', tz='Europe/Berlin')
        result = EntsoeRawClient._datetime_to_str(dt)
        # Berlin is UTC+1 in winter, so 12:30 Berlin = 11:30 UTC
        assert result == "202301011200"
    
    def test_datetime_to_str_naive_datetime(self):
        dt = pd.Timestamp('2023-01-01 12:30:00')
        result = EntsoeRawClient._datetime_to_str(dt)
        # Naive datetime is assumed to be UTC
        assert result == "202301011200"
    
    def test_datetime_to_str_rounding(self):
        # Test that minutes are rounded to the nearest hour
        dt = pd.Timestamp('2023-01-01 12:45:00', tz='UTC')
        result = EntsoeRawClient._datetime_to_str(dt)
        assert result == "202301011300"  # Rounds up to 13:00
        
        dt = pd.Timestamp('2023-01-01 12:15:00', tz='UTC')
        result = EntsoeRawClient._datetime_to_str(dt)
        assert result == "202301011200"  # Rounds down to 12:00
    
    def test_datetime_to_str_edge_cases(self):
        # Test year boundary
        dt = pd.Timestamp('2022-12-31 23:30:00', tz='UTC')
        result = EntsoeRawClient._datetime_to_str(dt)
        assert result == "202301010000"  # Rounds to next year
        
        # Test month boundary
        dt = pd.Timestamp('2023-01-31 23:30:00', tz='UTC')
        result = EntsoeRawClient._datetime_to_str(dt)
        assert result == "202302010000"  # Rounds to next month