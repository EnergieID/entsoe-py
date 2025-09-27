import pytest
from entsoe.mappings import Area, lookup_area, PSRTYPE_MAPPINGS, DOCSTATUS


class TestArea:
    
    def test_area_properties(self):
        area = Area.DE
        assert area.code == '10Y1001A1001A83F'
        assert area.meaning == 'Germany'
        assert area.tz == 'Europe/Berlin'
    
    def test_area_str_representation(self):
        area = Area.DE
        assert str(area) == '10Y1001A1001A83F'
    
    def test_has_code_valid(self):
        assert Area.has_code('DE')
        assert Area.has_code('FR')
    
    def test_has_code_invalid(self):
        assert not Area.has_code('INVALID')
        assert not Area.has_code('XX')


class TestLookupArea:
    
    def test_lookup_area_with_area_object(self):
        area = Area.DE
        result = lookup_area(area)
        assert result == area
    
    def test_lookup_area_with_country_code(self):
        result = lookup_area('DE')
        assert result == Area.DE
    
    def test_lookup_area_with_lowercase_code(self):
        result = lookup_area('de')
        assert result == Area.DE
    
    def test_lookup_area_with_direct_code(self):
        result = lookup_area('10Y1001A1001A83F')
        assert result == Area.DE
    
    def test_lookup_area_invalid_code(self):
        with pytest.raises(ValueError, match='Invalid country code'):
            lookup_area('INVALID')
    
    def test_lookup_area_none(self):
        with pytest.raises(ValueError, match='Invalid country code'):
            lookup_area(None)


class TestMappings:
    
    def test_psrtype_mappings_exist(self):
        assert 'B01' in PSRTYPE_MAPPINGS
        assert PSRTYPE_MAPPINGS['B01'] == 'Biomass'
        assert PSRTYPE_MAPPINGS['B14'] == 'Nuclear'
    
    def test_docstatus_mappings_exist(self):
        assert 'A01' in DOCSTATUS
        assert DOCSTATUS['A01'] == 'Intermediate'
        assert DOCSTATUS['A02'] == 'Final'
    
    def test_all_areas_have_required_attributes(self):
        for area in Area:
            assert hasattr(area, 'code')
            assert hasattr(area, 'meaning')
            assert hasattr(area, 'tz')
            assert isinstance(area.code, str)
            assert isinstance(area.meaning, str)
            assert isinstance(area.tz, str)