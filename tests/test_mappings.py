"""
Tests for entsoe.mappings module.

Covers:
- lookup_area unit tests (Task 6.1)
- Property 11: Area enum lookup round-trip (Task 6.2)
- Property 12: Invalid strings raise ValueError in lookup_area (Task 6.3)
- Property 13: Area enum member properties (Task 6.4)
- Property 21: PSRTYPE_MAPPINGS key pattern and value completeness (Task 6.5)
- Property 22: DOCSTATUS key pattern and value completeness (Task 6.6)
- Property 23: BSNTYPE key pattern and value completeness (Task 6.7)
- Property 24: NEIGHBOURS mapping validity (Task 6.8)
"""

import re

import pytest
from hypothesis import given, settings, assume
from hypothesis import strategies as st

from entsoe.mappings import (
    Area,
    lookup_area,
    PSRTYPE_MAPPINGS,
    DOCSTATUS,
    BSNTYPE,
    NEIGHBOURS,
)

# Precompute valid names and values for filtering in property tests
_VALID_AREA_NAMES = {m.name.upper() for m in Area}
_VALID_AREA_VALUES = {m.value for m in Area}


# ---------------------------------------------------------------------------
# Task 6.1 — Unit tests for lookup_area
# Requirements: 11.1, 11.2, 11.3, 11.4, 11.5
# ---------------------------------------------------------------------------

class TestLookupArea:

    def test_area_enum_returns_same_object(self):
        """Requirement 11.1: Area enum object returns the same object."""
        area = Area.DE
        assert lookup_area(area) is area

    def test_valid_uppercase_country_code(self):
        """Requirement 11.2: Valid uppercase country code returns matching Area."""
        assert lookup_area('DE') is Area.DE
        assert lookup_area('FR') is Area.FR

    def test_valid_lowercase_country_code(self):
        """Requirement 11.3: Valid lowercase country code returns matching Area."""
        assert lookup_area('de') is Area.DE
        assert lookup_area('fr') is Area.FR

    def test_valid_direct_eic_code(self):
        """Requirement 11.4: Valid direct EIC code returns matching Area."""
        assert lookup_area('10Y1001A1001A83F') is Area.DE
        assert lookup_area('10YFR-RTE------C') is Area.FR

    def test_invalid_string_raises_valueerror(self):
        """Requirement 11.5: Invalid string raises ValueError."""
        with pytest.raises(ValueError, match='Invalid country code'):
            lookup_area('INVALID_CODE_XYZ')

    def test_empty_string_raises_valueerror(self):
        """Requirement 11.5: Empty string raises ValueError."""
        with pytest.raises(ValueError, match='Invalid country code'):
            lookup_area('')


# ---------------------------------------------------------------------------
# Task 6.2 — Property 11: Area enum lookup round-trip
# ---------------------------------------------------------------------------

@given(area=st.sampled_from(list(Area)))
@settings(max_examples=100)
def test_area_enum_lookup_round_trip(area):
    """For all Area enum members, lookup_area resolves from object, name,
    lowercase name, and EIC value back to the same member."""
    # 11.1 — enum object returns same object
    assert lookup_area(area) is area
    # 11.2 — uppercase name returns same member
    assert lookup_area(area.name) is area
    # 11.3 — lowercase name returns same member
    assert lookup_area(area.name.lower()) is area
    # 11.4 — EIC value returns same member
    assert lookup_area(area.value) is area


# ---------------------------------------------------------------------------
# Task 6.3 — Property 12: Invalid strings raise ValueError in lookup_area
# ---------------------------------------------------------------------------

@given(s=st.text())
@settings(max_examples=100)
def test_invalid_strings_raise_valueerror(s):
    """For all strings that are not a valid Area name or value,
    lookup_area shall raise ValueError."""
    assume(s.upper() not in _VALID_AREA_NAMES)
    assume(s not in _VALID_AREA_VALUES)
    with pytest.raises(ValueError, match='Invalid country code'):
        lookup_area(s)


# ---------------------------------------------------------------------------
# Task 6.4 — Property 13: Area enum member properties
# ---------------------------------------------------------------------------

@given(area=st.sampled_from(list(Area)))
@settings(max_examples=100)
def test_area_enum_member_properties(area):
    """For all Area enum members, code equals the enum value,
    and meaning and tz are non-empty strings."""
    assert area.code == area.value
    assert isinstance(area.meaning, str) and len(area.meaning) > 0
    assert isinstance(area.tz, str) and len(area.tz) > 0


# ---------------------------------------------------------------------------
# Task 6.5 — Property 21: PSRTYPE_MAPPINGS key pattern and value completeness
# ---------------------------------------------------------------------------

@given(entry=st.sampled_from(list(PSRTYPE_MAPPINGS.items())))
@settings(max_examples=100)
def test_psrtype_mappings_key_pattern_and_value(entry):
    """For all entries in PSRTYPE_MAPPINGS, key matches ^[AB]\\d{2}$
    and value is a non-empty string."""
    key, value = entry
    assert re.match(r'^[AB]\d{2}$', key), f"Key {key!r} does not match pattern"
    assert isinstance(value, str) and len(value) > 0


# ---------------------------------------------------------------------------
# Task 6.6 — Property 22: DOCSTATUS key pattern and value completeness
# ---------------------------------------------------------------------------

@given(entry=st.sampled_from(list(DOCSTATUS.items())))
@settings(max_examples=100)
def test_docstatus_key_pattern_and_value(entry):
    """For all entries in DOCSTATUS, key matches ^[AX]\\d{2}$
    and value is a non-empty string."""
    key, value = entry
    assert re.match(r'^[AX]\d{2}$', key), f"Key {key!r} does not match pattern"
    assert isinstance(value, str) and len(value) > 0


# ---------------------------------------------------------------------------
# Task 6.7 — Property 23: BSNTYPE key pattern and value completeness
# ---------------------------------------------------------------------------

@given(entry=st.sampled_from(list(BSNTYPE.items())))
@settings(max_examples=100)
def test_bsntype_key_pattern_and_value(entry):
    """For all entries in BSNTYPE, key matches ^[ABC]\\d{2}$
    and value is a non-empty string."""
    key, value = entry
    assert re.match(r'^[ABC]\d{2}$', key), f"Key {key!r} does not match pattern"
    assert isinstance(value, str) and len(value) > 0


# ---------------------------------------------------------------------------
# Task 6.8 — Property 24: NEIGHBOURS mapping validity
# ---------------------------------------------------------------------------

@given(entry=st.sampled_from(list(NEIGHBOURS.items())))
@settings(max_examples=100)
def test_neighbours_mapping_validity(entry):
    """For all entries in NEIGHBOURS, the key is a valid Area enum name,
    and each value in the list is a valid Area enum name."""
    key, neighbours = entry
    assert key in _VALID_AREA_NAMES, f"Key {key!r} is not a valid Area name"
    for neighbour in neighbours:
        assert neighbour in _VALID_AREA_NAMES, (
            f"Neighbour {neighbour!r} of {key!r} is not a valid Area name"
        )
