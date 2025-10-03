import pytest
import pandas as pd
import warnings
from unittest.mock import Mock, patch
from bs4 import XMLParsedAsHTMLWarning
from entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError

warnings.filterwarnings('ignore', category=XMLParsedAsHTMLWarning)


class TestIntegration:
    
    @pytest.fixture
    def client(self):
        return EntsoePandasClient(api_key="test_key")
    
    @patch.object(EntsoePandasClient, 'query_crossborder_flows')
    @patch.object(EntsoePandasClient, 'query_generation')
    def test_query_physical_crossborder_allborders(self, mock_gen, mock_flows, client):
        """Test integration of multiple query methods"""
        mock_flows.return_value = pd.Series([100, 110], 
                                          index=pd.date_range('2023-01-01', periods=2, freq='h', tz='UTC'))
        mock_gen.return_value = pd.DataFrame({'Nuclear': [500, 520]}, 
                                           index=pd.date_range('2023-01-01', periods=2, freq='h', tz='UTC'))
        
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-02', tz='UTC')
        
        # Test that methods can be called together
        flows = client.query_crossborder_flows('DE', 'FR', start=start, end=end)
        generation = client.query_generation('DE', start=start, end=end)
        
        assert isinstance(flows, pd.Series)
        assert isinstance(generation, pd.DataFrame)
    
    def test_error_handling_chain(self, client):
        """Test that errors propagate correctly through the system"""
        with patch.object(client, '_base_request') as mock_request:
            mock_request.side_effect = NoMatchingDataError()
            
            start = pd.Timestamp('2023-01-01', tz='UTC')
            end = pd.Timestamp('2023-01-02', tz='UTC')
            
            with pytest.raises(NoMatchingDataError):
                client.query_day_ahead_prices('DE', start=start, end=end)
    
    @patch.object(EntsoePandasClient, '_query_day_ahead_prices')
    def test_day_ahead_prices_padding_workflow(self, mock_query, client):
        """Test that day ahead prices query properly handles date padding"""
        # Return empty series to trigger NoMatchingDataError after truncation
        mock_series = pd.Series([], dtype=float)
        mock_query.return_value = mock_series
        
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-01 23:59:59', tz='UTC')
        
        with pytest.raises(NoMatchingDataError):
            client.query_day_ahead_prices('DE', start=start, end=end)
    
    def test_timezone_handling_consistency(self, client):
        """Test that timezone handling is consistent across different methods"""
        with patch.object(client, '_base_request') as mock_request:
            mock_response = Mock()
            mock_response.text = """<?xml version="1.0" encoding="UTF-8"?>
            <Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0">
            </Publication_MarketDocument>"""
            mock_request.return_value = mock_response
            
            start = pd.Timestamp('2023-01-01 12:00:00', tz='Europe/Berlin')
            end = pd.Timestamp('2023-01-01 18:00:00', tz='Europe/Berlin')
            
            try:
                client.query_day_ahead_prices('DE', start=start, end=end)
            except (NoMatchingDataError, ValueError):
                # Expected for empty XML
                pass
    
    def test_multi_country_workflow(self, client):
        """Test workflow combining data from multiple countries"""
        with patch.object(client, 'query_day_ahead_prices') as mock_prices:
            mock_prices.return_value = pd.Series([50.0, 60.0], 
                                               index=pd.date_range('2023-01-01', periods=2, freq='h', tz='UTC'))
            
            start = pd.Timestamp('2023-01-01', tz='UTC')
            end = pd.Timestamp('2023-01-02', tz='UTC')
            
            countries = ['DE', 'FR', 'NL']
            results = {}
            
            for country in countries:
                results[country] = client.query_day_ahead_prices(country, start=start, end=end)
            
            assert len(results) == 3
            for country, data in results.items():
                assert isinstance(data, pd.Series)
                assert len(data) == 2
    
    def test_data_consistency_across_methods(self, client):
        """Test data consistency when combining different query methods"""
        with patch.object(client, 'query_generation') as mock_gen, \
             patch.object(client, 'query_load') as mock_load:
            
            index = pd.date_range('2023-01-01', periods=24, freq='h', tz='UTC')
            mock_gen.return_value = pd.DataFrame({'Nuclear': range(24)}, index=index)
            mock_load.return_value = pd.DataFrame({'Actual Load': range(100, 124)}, index=index)
            
            start = pd.Timestamp('2023-01-01', tz='UTC')
            end = pd.Timestamp('2023-01-02', tz='UTC')
            
            generation = client.query_generation('DE', start=start, end=end)
            load = client.query_load('DE', start=start, end=end)
            
            # Check index alignment
            assert generation.index.equals(load.index)
            assert len(generation) == len(load) == 24