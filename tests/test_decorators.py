import pytest
import pandas as pd
from unittest.mock import Mock, patch
from entsoe.decorators import retry, year_limited, day_limited, paginated
from entsoe.exceptions import PaginationError


class TestDecorators:
    
    def test_retry_decorator_success(self):
        @retry
        def success_function(self):
            return "success"
        
        mock_self = Mock()
        mock_self.retry_count = 3
        mock_self.retry_delay = 0.1
        
        result = success_function(mock_self)
        assert result == "success"
    
    def test_retry_decorator_with_retries(self):
        call_count = 0
        
        @retry
        def failing_function(self):
            nonlocal call_count
            call_count += 1
            raise Exception("Always fails")
        
        mock_self = Mock()
        mock_self.retry_count = 3
        mock_self.retry_delay = 0.1
        
        with pytest.raises(Exception, match="Always fails"):
            failing_function(mock_self)
        
        assert call_count >= 1  # At least one call
    
    def test_year_limited_decorator_single_year(self):
        @year_limited
        def mock_query_function(self, country_code, start, end):
            return pd.Series([1, 2, 3], index=pd.date_range(start, periods=3, freq='D'))
        
        mock_self = Mock()
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-31', tz='UTC')
        
        result = mock_query_function(mock_self, 'DE', start=start, end=end)
        assert isinstance(result, pd.Series)
        assert len(result) == 3
    
    def test_year_limited_decorator_multiple_years(self):
        call_count = 0
        
        @year_limited
        def mock_query_function(self, country_code, start, end):
            nonlocal call_count
            call_count += 1
            return pd.Series([call_count], index=[start])
        
        mock_self = Mock()
        start = pd.Timestamp('2022-06-01', tz='UTC')
        end = pd.Timestamp('2024-06-01', tz='UTC')
        
        result = mock_query_function(mock_self, 'DE', start=start, end=end)
        assert isinstance(result, pd.Series)
        assert call_count > 1  # Should be called multiple times for multiple years
    
    def test_day_limited_decorator_single_day(self):
        @day_limited
        def mock_query_function(self, country_code, start, end):
            return pd.DataFrame({'value': [1, 2]}, index=pd.date_range(start, periods=2, freq='h'))
        
        mock_self = Mock()
        start = pd.Timestamp('2023-01-01')
        end = pd.Timestamp('2023-01-01 23:59:59')
        
        result = mock_query_function(mock_self, 'DE', start=start, end=end)
        assert isinstance(result, pd.DataFrame)
    
    def test_paginated_decorator_no_pagination_needed(self):
        @paginated
        def mock_query_function(self, country_code, start, end, offset=0):
            return pd.Series([1, 2, 3])
        
        mock_self = Mock()
        start = pd.Timestamp('2023-01-01')
        end = pd.Timestamp('2023-01-02')
        
        result = mock_query_function(mock_self, 'DE', start=start, end=end)
        assert isinstance(result, pd.Series)
        assert len(result) == 3
    
    def test_paginated_decorator_with_pagination_error(self):
        call_count = 0
        
        @paginated
        def mock_query_function(self, country_code, start, end, offset=0):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise PaginationError("Need pagination")
            return pd.Series([1, 2, 3])
        
        mock_self = Mock()
        start = pd.Timestamp('2023-01-01')
        end = pd.Timestamp('2023-01-02')
        
        result = mock_query_function(mock_self, 'DE', start=start, end=end)
        assert isinstance(result, pd.Series)
        assert call_count >= 2  # Should retry with pagination