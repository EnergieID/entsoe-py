import pytest
import pandas as pd
from entsoe.misc import year_blocks, day_blocks, month_blocks


class TestMisc:
    
    def test_year_blocks_single_year(self):
        start = pd.Timestamp('2023-01-01')
        end = pd.Timestamp('2023-12-31')
        
        blocks = list(year_blocks(start, end))
        assert len(blocks) == 1
        assert blocks[0][0] == start
        assert blocks[0][1] == end
    
    def test_year_blocks_multiple_years(self):
        start = pd.Timestamp('2022-06-01')
        end = pd.Timestamp('2024-06-01')
        
        blocks = list(year_blocks(start, end))
        assert len(blocks) == 2  # 2022-2023 and 2023-2024
        assert blocks[0][0] == start
        assert blocks[-1][1] == end
    
    def test_day_blocks_single_day(self):
        start = pd.Timestamp('2023-01-01')
        end = pd.Timestamp('2023-01-01 23:59:59')
        
        blocks = list(day_blocks(start, end))
        assert len(blocks) == 1
        assert blocks[0][0] == start
    
    def test_day_blocks_multiple_days(self):
        start = pd.Timestamp('2023-01-01')
        end = pd.Timestamp('2023-01-05')
        
        blocks = list(day_blocks(start, end))
        assert len(blocks) == 4  # 4 full days from start to end (exclusive)
        assert blocks[0][0] == start
    
    def test_month_blocks_single_month(self):
        start = pd.Timestamp('2023-01-01')
        end = pd.Timestamp('2023-01-31')
        
        blocks = list(month_blocks(start, end))
        assert len(blocks) == 1
        assert blocks[0][0] == start
        assert blocks[0][1] == end
    
    def test_month_blocks_multiple_months(self):
        start = pd.Timestamp('2023-01-01')
        end = pd.Timestamp('2023-03-31')
        
        blocks = list(month_blocks(start, end))
        assert len(blocks) >= 1  # At least one block
        assert blocks[0][0] == start
        assert blocks[-1][1] == end
    
    def test_year_blocks_edge_case_year_boundary(self):
        start = pd.Timestamp('2022-12-31 23:00:00')
        end = pd.Timestamp('2023-01-01 01:00:00')
        
        blocks = list(year_blocks(start, end))
        assert len(blocks) == 1  # Short period within same year boundary
        assert blocks[0][0] == start
        assert blocks[0][1] == end
    
    def test_day_blocks_timezone_aware(self):
        start = pd.Timestamp('2023-01-01 00:00:00', tz='UTC')
        end = pd.Timestamp('2023-01-02 00:00:00', tz='UTC')
        
        blocks = list(day_blocks(start, end))
        assert len(blocks) == 1  # Exactly one day
        assert blocks[0][0] == start
        assert blocks[0][1] == end