import pytest
import pandas as pd
import requests
from unittest.mock import Mock, patch, MagicMock
from entsoe import EntsoeRawClient, EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError, PaginationError, InvalidPSRTypeError
from entsoe.mappings import Area


class TestEntsoeRawClient:
    
    @pytest.fixture
    def client(self):
        return EntsoeRawClient(api_key="test_key")
    
    def test_init_with_api_key(self):
        client = EntsoeRawClient(api_key="test_key")
        assert client.api_key == "test_key"
    
    def test_init_without_api_key_raises_error(self):
        with pytest.raises(TypeError, match="API key cannot be None"):
            EntsoeRawClient(api_key=None)
    
    @patch.dict('os.environ', {'ENTSOE_API_KEY': 'env_key'})
    def test_init_with_env_api_key(self):
        client = EntsoeRawClient()
        assert client.api_key == "env_key"
    
    def test_datetime_to_str(self):
        dt = pd.Timestamp('2023-01-01 12:30:00', tz='UTC')
        result = EntsoeRawClient._datetime_to_str(dt)
        assert result == "202301011200"
    
    def test_datetime_to_str_timezone_conversion(self):
        dt = pd.Timestamp('2023-01-01 12:30:00', tz='Europe/Berlin')
        result = EntsoeRawClient._datetime_to_str(dt)
        assert result == "202301011200"  # Berlin is UTC+1 in winter
    
    @patch('entsoe.entsoe.requests.Session.get')
    def test_base_request_success(self, mock_get, client):
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.headers = {'content-type': 'application/xml'}
        mock_response.text = '<xml>test</xml>'
        mock_get.return_value = mock_response
        
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-02', tz='UTC')
        params = {'documentType': 'A44'}
        
        result = client._base_request(params, start, end)
        assert result == mock_response
    
    @patch('entsoe.entsoe.requests.Session.get')
    def test_base_request_no_matching_data(self, mock_get, client):
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError()
        mock_response.text = '<text>No matching data found</text>'
        mock_get.return_value = mock_response
        
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-02', tz='UTC')
        params = {'documentType': 'A44'}
        
        with pytest.raises(NoMatchingDataError):
            client._base_request(params, start, end)
    
    @patch('entsoe.entsoe.requests.Session.get')
    def test_base_request_pagination_error(self, mock_get, client):
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError()
        mock_response.text = '<text>amount of requested data exceeds allowed limit 100 200</text>'
        mock_get.return_value = mock_response
        
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-02', tz='UTC')
        params = {'documentType': 'A44'}
        
        with pytest.raises(PaginationError):
            client._base_request(params, start, end)


class TestEntsoePandasClient:
    
    @pytest.fixture
    def client(self):
        return EntsoePandasClient(api_key="test_key")
    
    @patch.object(EntsoePandasClient, '_query_day_ahead_prices')
    def test_query_day_ahead_prices(self, mock_query, client):
        mock_series = pd.Series([50.0, 60.0], 
                               index=pd.date_range('2023-01-01', periods=2, freq='h', tz='UTC'))
        mock_query.return_value = mock_series
        
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-02', tz='UTC')
        
        result = client.query_day_ahead_prices('DE', start, end)
        assert isinstance(result, pd.Series)
        assert len(result) == 2
    
    @patch.object(EntsoePandasClient, 'query_day_ahead_prices')
    def test_query_day_ahead_prices_no_data(self, mock_query, client):
        mock_query.side_effect = NoMatchingDataError()
        
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-02', tz='UTC')
        
        with pytest.raises(NoMatchingDataError):
            client.query_day_ahead_prices('DE', start, end)
    
    @patch.object(EntsoePandasClient, 'query_load')
    def test_query_load(self, mock_query, client):
        mock_df = pd.DataFrame({'Actual Load': [100, 110]}, 
                             index=pd.date_range('2023-01-01', periods=2, freq='h', tz='UTC'))
        mock_query.return_value = mock_df
        
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-02', tz='UTC')
        
        result = client.query_load('DE', start=start, end=end)
        assert isinstance(result, pd.DataFrame)
        assert 'Actual Load' in result.columns
    
    @patch.object(EntsoePandasClient, 'query_generation')
    def test_query_generation(self, mock_query, client):
        mock_df = pd.DataFrame({'Nuclear': [500, 520], 'Wind': [200, 180]}, 
                             index=pd.date_range('2023-01-01', periods=2, freq='h', tz='UTC'))
        mock_query.return_value = mock_df
        
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-02', tz='UTC')
        
        result = client.query_generation('DE', start=start, end=end, psr_type='B14')
        assert isinstance(result, pd.DataFrame)
        assert len(result.columns) >= 1
    
    @patch.object(EntsoePandasClient, 'query_load_forecast')
    def test_query_load_forecast(self, mock_query, client):
        mock_df = pd.DataFrame({'Forecasted Load': [1000, 1100]}, 
                             index=pd.date_range('2023-01-01', periods=2, freq='h', tz='UTC'))
        mock_query.return_value = mock_df
        
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-02', tz='UTC')
        
        result = client.query_load_forecast('DE', start=start, end=end)
        assert isinstance(result, pd.DataFrame)
        assert 'Forecasted Load' in result.columns
    
    @patch.object(EntsoePandasClient, 'query_installed_generation_capacity')
    def test_query_installed_capacity(self, mock_query, client):
        mock_df = pd.DataFrame({'Nuclear': [10000], 'Wind': [15000]}, 
                             index=pd.date_range('2023-01-01', periods=1, freq='D', tz='UTC'))
        mock_query.return_value = mock_df
        
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-02', tz='UTC')
        
        result = client.query_installed_generation_capacity('DE', start=start, end=end)
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 1
    
    @pytest.mark.parametrize("country_code,expected_valid", [
        ('DE', True),
        ('FR', True), 
        ('INVALID', False),
        ('XX', False)
    ])
    def test_country_code_validation(self, client, country_code, expected_valid):
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-02', tz='UTC')
        
        if expected_valid:
            with patch.object(client, '_base_request') as mock_request:
                mock_response = Mock()
                mock_response.text = "<xml>test</xml>"
                mock_request.return_value = mock_response
                try:
                    client.query_day_ahead_prices(country_code, start=start, end=end)
                except (NoMatchingDataError, ValueError):
                    pass
        else:
            with pytest.raises(ValueError):
                client.query_day_ahead_prices(country_code, start=start, end=end)
    
    def test_large_date_range_handling(self, client):
        """Test handling of large date ranges that might trigger pagination"""
        with patch.object(client, '_base_request') as mock_request:
            mock_request.side_effect = PaginationError("Data too large")
            
            start = pd.Timestamp('2020-01-01', tz='UTC')
            end = pd.Timestamp('2023-12-31', tz='UTC')
            
            with pytest.raises(PaginationError):
                client.query_day_ahead_prices('DE', start=start, end=end)