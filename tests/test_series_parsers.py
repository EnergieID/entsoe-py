import pytest
import pandas as pd
import warnings
from bs4 import XMLParsedAsHTMLWarning
from entsoe.parsers import parse_prices

warnings.filterwarnings('ignore', category=XMLParsedAsHTMLWarning)


class TestSeriesParsers:
    
    def test_parse_prices_empty_xml(self):
        xml_text = """<?xml version="1.0" encoding="UTF-8"?>
        <Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0">
        </Publication_MarketDocument>"""
        
        result = parse_prices(xml_text)
        assert isinstance(result, dict)
        assert '15min' in result
        assert '30min' in result
        assert '60min' in result
    
    def test_parse_prices_with_data(self):
        xml_text = """<?xml version="1.0" encoding="UTF-8"?>
        <Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0">
            <timeseries>
                <curvetype>A01</curvetype>
                <period>
                    <timeinterval>
                        <start>2023-01-01T00:00Z</start>
                        <end>2023-01-01T01:00Z</end>
                    </timeinterval>
                    <resolution>PT60M</resolution>
                    <point>
                        <position>1</position>
                        <price.amount>50.0</price.amount>
                    </point>
                </period>
            </timeseries>
        </Publication_MarketDocument>"""
        
        result = parse_prices(xml_text)
        assert isinstance(result, dict)
        assert '60min' in result
        assert len(result['60min']) > 0
    
    def test_parse_prices_multiple_points(self):
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