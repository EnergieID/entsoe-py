import pytest
import pandas as pd
import warnings
from unittest.mock import patch, Mock
from bs4 import XMLParsedAsHTMLWarning
from entsoe.parsers import (
    parse_prices,
    parse_loads,
    parse_generation,
    parse_crossborder_flows,
    parse_imbalance_prices_zip,
    parse_imbalance_volumes_zip
)

warnings.filterwarnings('ignore', category=XMLParsedAsHTMLWarning)


class TestParsers:
    
    def test_parse_prices_with_data(self):
        xml_text = """<?xml version="1.0" encoding="UTF-8"?>
        <Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0">
            <timeseries>
                <curvetype>A01</curvetype>
                <period>
                    <timeinterval>
                        <start>2023-01-01T00:00Z</start>
                        <end>2023-01-01T02:00Z</end>
                    </timeinterval>
                    <resolution>PT60M</resolution>
                    <point>
                        <position>1</position>
                        <price.amount>50.0</price.amount>
                    </point>
                    <point>
                        <position>2</position>
                        <price.amount>60.0</price.amount>
                    </point>
                </period>
            </timeseries>
        </Publication_MarketDocument>"""
        
        result = parse_prices(xml_text)
        assert isinstance(result, dict)
        assert '60min' in result
        assert len(result['60min']) == 2
        assert result['60min'].iloc[0] == 50.0
        assert result['60min'].iloc[1] == 60.0
    
    def test_parse_loads_with_data(self):
        xml_text = """<?xml version="1.0" encoding="UTF-8"?>
        <GL_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0">
            <timeseries>
                <businesstype>A04</businesstype>
                <objectaggregation>A01</objectaggregation>
                <curvetype>A01</curvetype>
                <period>
                    <timeinterval>
                        <start>2023-01-01T00:00Z</start>
                        <end>2023-01-01T02:00Z</end>
                    </timeinterval>
                    <resolution>PT60M</resolution>
                    <point>
                        <position>1</position>
                        <quantity>1000</quantity>
                    </point>
                    <point>
                        <position>2</position>
                        <quantity>1100</quantity>
                    </point>
                </period>
            </timeseries>
        </GL_MarketDocument>"""
        
        result = parse_loads(xml_text)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert result.iloc[0, 0] == 1000
        assert result.iloc[1, 0] == 1100
    
    def test_parse_generation_with_data(self):
        xml_text = """<?xml version="1.0" encoding="UTF-8"?>
        <GL_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0">
            <timeseries>
                <businesstype>A75</businesstype>
                <objectaggregation>A01</objectaggregation>
                <curvetype>A01</curvetype>
                <mktpsrtype>
                    <psrtype>B14</psrtype>
                </mktpsrtype>
                <inbiddingzone_domain.mrid>10Y1001A1001A83F</inbiddingzone_domain.mrid>
                <period>
                    <timeinterval>
                        <start>2023-01-01T00:00Z</start>
                        <end>2023-01-01T02:00Z</end>
                    </timeinterval>
                    <resolution>PT60M</resolution>
                    <point>
                        <position>1</position>
                        <quantity>500</quantity>
                    </point>
                    <point>
                        <position>2</position>
                        <quantity>520</quantity>
                    </point>
                </period>
            </timeseries>
        </GL_MarketDocument>"""
        
        result = parse_generation(xml_text)
        assert isinstance(result, (pd.DataFrame, pd.Series))
        if isinstance(result, pd.DataFrame):
            assert len(result) == 2
        else:
            assert len(result) == 2
    
    def test_parse_crossborder_flows_with_data(self):
        xml_text = """<?xml version="1.0" encoding="UTF-8"?>
        <Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0">
            <timeseries>
                <businesstype>A11</businesstype>
                <curvetype>A01</curvetype>
                <period>
                    <timeinterval>
                        <start>2023-01-01T00:00Z</start>
                        <end>2023-01-01T02:00Z</end>
                    </timeinterval>
                    <resolution>PT60M</resolution>
                    <point>
                        <position>1</position>
                        <quantity>100</quantity>
                    </point>
                    <point>
                        <position>2</position>
                        <quantity>110</quantity>
                    </point>
                </period>
            </timeseries>
        </Publication_MarketDocument>"""
        
        result = parse_crossborder_flows(xml_text)
        assert isinstance(result, pd.Series)
        assert len(result) == 2
        assert result.iloc[0] == 100
        assert result.iloc[1] == 110
    
    def test_parse_generation_per_plant(self):
        xml_text = """<?xml version="1.0" encoding="UTF-8"?>
        <GL_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0">
            <timeseries>
                <businesstype>A75</businesstype>
                <objectaggregation>A04</objectaggregation>
                <curvetype>A01</curvetype>
                <mktpsrtype>
                    <psrtype>B14</psrtype>
                </mktpsrtype>
                <name>Test Plant</name>
                <inbiddingzone_domain.mrid>10Y1001A1001A83F</inbiddingzone_domain.mrid>
                <period>
                    <timeinterval>
                        <start>2023-01-01T00:00Z</start>
                        <end>2023-01-01T01:00Z</end>
                    </timeinterval>
                    <resolution>PT60M</resolution>
                    <point>
                        <position>1</position>
                        <quantity>300</quantity>
                    </point>
                </period>
            </timeseries>
        </GL_MarketDocument>"""
        
        result = parse_generation(xml_text, per_plant=True)
        assert isinstance(result, (pd.DataFrame, pd.Series))
        if isinstance(result, pd.DataFrame):
            assert len(result) == 1
    
    def test_parse_prices_multiple_resolutions(self):
        xml_text = """<?xml version="1.0" encoding="UTF-8"?>
        <Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0">
            <timeseries>
                <curvetype>A01</curvetype>
                <period>
                    <timeinterval>
                        <start>2023-01-01T00:00Z</start>
                        <end>2023-01-01T01:00Z</end>
                    </timeinterval>
                    <resolution>PT15M</resolution>
                    <point>
                        <position>1</position>
                        <price.amount>45.0</price.amount>
                    </point>
                    <point>
                        <position>2</position>
                        <price.amount>47.0</price.amount>
                    </point>
                    <point>
                        <position>3</position>
                        <price.amount>49.0</price.amount>
                    </point>
                    <point>
                        <position>4</position>
                        <price.amount>51.0</price.amount>
                    </point>
                </period>
            </timeseries>
        </Publication_MarketDocument>"""
        
        result = parse_prices(xml_text)
        assert '15min' in result
        assert len(result['15min']) == 4
        assert result['15min'].iloc[0] == 45.0
    
    def test_parse_empty_xml_fallback(self):
        """Test that parsers handle empty XML gracefully"""
        empty_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0">
        </Publication_MarketDocument>"""
        
        result = parse_prices(empty_xml)
        assert isinstance(result, dict)
        assert all(len(result[key]) == 0 for key in result.keys())