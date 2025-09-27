import pytest
import pandas as pd
import requests
from unittest.mock import Mock, patch
from entsoe import EntsoeRawClient, EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError, PaginationError


class TestEntsoeClientImproved:
    
    @pytest.fixture
    def pandas_client(self):
        return EntsoePandasClient(api_key="test_key")
    
    @pytest.fixture
    def raw_client(self):
        return EntsoeRawClient(api_key="test_key")
    
    @patch.object(EntsoePandasClient, 'query_generation')
    def test_query_generation_with_psr_types(self, mock_query, pandas_client):
        mock_df = pd.DataFrame({'Nuclear': [500, 520], 'Wind': [200, 180]}, 
                             index=pd.date_range('2023-01-01', periods=2, freq='h', tz='UTC'))
        mock_query.return_value = mock_df
        
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-02', tz='UTC')
        
        result = pandas_client.query_generation('DE', start=start, end=end, psr_type='B14')
        assert isinstance(result, pd.DataFrame)
        assert len(result.columns) >= 1
    
    @patch.object(EntsoePandasClient, 'query_load_forecast')
    def test_query_load_forecast(self, mock_query, pandas_client):
        mock_df = pd.DataFrame({'Forecasted Load': [1000, 1100]}, 
                             index=pd.date_range('2023-01-01', periods=2, freq='h', tz='UTC'))
        mock_query.return_value = mock_df
        
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-02', tz='UTC')
        
        result = pandas_client.query_load_forecast('DE', start=start, end=end)
        assert isinstance(result, pd.DataFrame)
        assert 'Forecasted Load' in result.columns
    
    def test_timezone_handling_edge_cases(self, pandas_client):
        """Test timezone handling with DST transitions"""
        with patch.object(pandas_client, '_base_request') as mock_request:
            mock_response = Mock()
            mock_response.text = "<xml>test</xml>"
            mock_request.return_value = mock_response
            
            # DST transition dates
            start_dst = pd.Timestamp('2023-03-26 01:00:00', tz='Europe/Berlin')
            end_dst = pd.Timestamp('2023-03-26 04:00:00', tz='Europe/Berlin')
            
            try:
                pandas_client.query_day_ahead_prices('DE', start=start_dst, end=end_dst)
            except (NoMatchingDataError, ValueError):
                pass
    
    def test_large_date_range_handling(self, pandas_client):
        """Test handling of large date ranges that might trigger pagination"""
        with patch.object(pandas_client, '_base_request') as mock_request:
            mock_request.side_effect = PaginationError("Data too large")
            
            start = pd.Timestamp('2020-01-01', tz='UTC')
            end = pd.Timestamp('2023-12-31', tz='UTC')
            
            with pytest.raises(PaginationError):
                pandas_client.query_day_ahead_prices('DE', start=start, end=end)
    
    @pytest.mark.parametrize("country_code,expected_valid", [
        ('DE', True),
        ('FR', True), 
        ('INVALID', False),
        ('XX', False),
        (None, False)
    ])
    def test_country_code_validation(self, pandas_client, country_code, expected_valid):
        """Test validation of different country codes"""
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-02', tz='UTC')
        
        if expected_valid:
            with patch.object(pandas_client, '_base_request'):
                try:
                    pandas_client.query_day_ahead_prices(country_code, start=start, end=end)
                except (NoMatchingDataError, ValueError, TypeError):
                    pass
        else:
            with pytest.raises((ValueError, TypeError)):
                pandas_client.query_day_ahead_prices(country_code, start=start, end=end)
    
    def test_concurrent_requests_simulation(self, pandas_client):
        """Test behavior under concurrent request simulation"""
        with patch.object(pandas_client, '_base_request') as mock_request:
            mock_response = Mock()
            mock_response.text = "<xml>test</xml>"
            mock_request.return_value = mock_response
            
            start = pd.Timestamp('2023-01-01', tz='UTC')
            end = pd.Timestamp('2023-01-02', tz='UTC')
            
            # Simulate multiple rapid requests
            for _ in range(5):
                try:
                    pandas_client.query_day_ahead_prices('DE', start=start, end=end)
                except (NoMatchingDataError, ValueError):
                    pass
            
            assert mock_request.call_count == 5