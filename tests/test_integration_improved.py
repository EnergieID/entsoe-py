import pytest
import pandas as pd
from unittest.mock import Mock, patch
from entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError


class TestIntegrationImproved:
    
    @pytest.fixture
    def client(self):
        return EntsoePandasClient(api_key="test_key")
    
    def test_multi_country_data_workflow(self, client):
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
    
    def test_error_recovery_workflow(self, client):
        """Test error handling and recovery in data workflows"""
        with patch.object(client, 'query_day_ahead_prices') as mock_prices:
            # First call fails, second succeeds
            mock_prices.side_effect = [NoMatchingDataError(), 
                                     pd.Series([50.0], index=pd.date_range('2023-01-01', periods=1, freq='h', tz='UTC'))]
            
            start = pd.Timestamp('2023-01-01', tz='UTC')
            end = pd.Timestamp('2023-01-02', tz='UTC')
            
            # First attempt fails
            with pytest.raises(NoMatchingDataError):
                client.query_day_ahead_prices('DE', start=start, end=end)
            
            # Second attempt succeeds
            result = client.query_day_ahead_prices('DE', start=start, end=end)
            assert isinstance(result, pd.Series)
            assert len(result) == 1
    
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
    
    def test_time_series_aggregation_workflow(self, client):
        """Test aggregating time series data from multiple sources"""
        with patch.object(client, 'query_crossborder_flows') as mock_flows:
            # Mock flows between different country pairs
            mock_flows.return_value = pd.Series([100, 110, 120], 
                                              index=pd.date_range('2023-01-01', periods=3, freq='h', tz='UTC'))
            
            start = pd.Timestamp('2023-01-01', tz='UTC')
            end = pd.Timestamp('2023-01-02', tz='UTC')
            
            flows = {}
            country_pairs = [('DE', 'FR'), ('DE', 'NL'), ('FR', 'BE')]
            
            for from_country, to_country in country_pairs:
                flows[f"{from_country}-{to_country}"] = client.query_crossborder_flows(
                    from_country, to_country, start=start, end=end)
            
            # Aggregate total flows
            total_flows = pd.concat(flows.values(), axis=1).sum(axis=1)
            assert len(total_flows) == 3
            assert total_flows.iloc[0] == 300  # 100 * 3 country pairs
    
    def test_performance_with_large_datasets(self, client):
        """Test performance characteristics with large datasets"""
        with patch.object(client, 'query_generation') as mock_gen:
            # Simulate large dataset (1 year of hourly data)
            large_index = pd.date_range('2023-01-01', '2023-12-31 23:00:00', freq='h', tz='UTC')
            large_data = pd.DataFrame({'Nuclear': range(len(large_index))}, index=large_index)
            mock_gen.return_value = large_data
            
            start = pd.Timestamp('2023-01-01', tz='UTC')
            end = pd.Timestamp('2023-12-31', tz='UTC')
            
            result = client.query_generation('DE', start=start, end=end)
            
            # Verify large dataset handling
            assert len(result) == len(large_index)
            assert isinstance(result, pd.DataFrame)
            
            # Test basic operations on large dataset
            monthly_avg = result.resample('ME').mean()
            assert len(monthly_avg) == 12