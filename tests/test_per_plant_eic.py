"""Offline regression: per-plant generation must keep the EIC column level.

`_calc_nett_and_drop_redundant_columns` drops the innermost column level when it
has a single distinct value. With ``include_eic=True`` that innermost level is the
EIC, so a window that returns a single generation unit silently loses the EIC -
collapsing the per-plant columns from (plant, psr, metric, eic) to
(plant, psr, metric). The EIC is the join key, so it must be preserved regardless of
how many units the window returns.

No network or API key required.
"""
import pandas as pd

from entsoe.parsers import parse_generation


# One generation unit, one metric -> the single-eic window that triggered the drop.
SINGLE_UNIT = """
<GL_MarketDocument>
  <TimeSeries>
    <mRID>1</mRID>
    <businessType>A01</businessType>
    <registeredResource.mRID codingScheme="A01">18WPVENT2--12342</registeredResource.mRID>
    <MktPSRType>
      <psrType>B04</psrType>
      <PowerSystemResources>
        <mRID codingScheme="A01">18WPVENT2-1234-P</mRID>
        <name>PdV2</name>
      </PowerSystemResources>
    </MktPSRType>
    <curveType>A03</curveType>
    <Period>
      <timeInterval><start>2026-06-11T22:00Z</start><end>2026-06-12T01:00Z</end></timeInterval>
      <resolution>PT60M</resolution>
      <Point><position>1</position><quantity>10</quantity></Point>
      <Point><position>2</position><quantity>20</quantity></Point>
      <Point><position>3</position><quantity>30</quantity></Point>
    </Period>
  </TimeSeries>
</GL_MarketDocument>
"""


def test_single_unit_keeps_eic_level():
    df = parse_generation(SINGLE_UNIT, per_plant=True, include_eic=True)
    assert isinstance(df.columns, pd.MultiIndex)
    # 4 levels: (plant, psr, metric, eic) - the eic level must survive.
    assert df.columns.nlevels == 4, f"eic level dropped: {df.columns.tolist()}"
    col = df.columns[0]
    assert col[0] == "PdV2"
    assert col[-1] == "18WPVENT2-1234-P"


def test_single_unit_without_eic_still_drops_redundant_metric():
    # Without include_eic the old redundant-drop behaviour is unchanged:
    # (plant, psr, metric) with a single metric -> metric level dropped -> 2 levels.
    df = parse_generation(SINGLE_UNIT, per_plant=True, include_eic=False)
    assert df.columns.nlevels == 2
    assert df.columns[0] == ("PdV2", "Fossil Gas")
