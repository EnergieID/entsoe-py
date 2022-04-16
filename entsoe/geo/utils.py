import geopandas as gpd
import pandas as pd
import json
from geojson_rewind import rewind
import os


def geojson_rewind_file(fname):
    # warning: this overwrites the file!
    with open(fname, 'r') as stream:
        data = json.load(stream)

    data = rewind(data, rfc7946=False)

    with open(fname, 'w') as stream:
        stream.write(json.dumps(data))


def extract_naturalearthdata(shpfile, countrylist, outputfolder):
    # parses and extracts list of countries from ne_10m_admin_0_countries.shp from naturalearthdata
    # countrylist is a list of iso2 countrycodes to extract
    geo_df_all = gpd.read_file(shpfile)

    # fixes for countries that are missing iso2 code
    geo_df_all.loc[geo_df_all['ADMIN'] == 'France', 'ISO_A2'] = 'FR'

    geo_df_all = geo_df_all[['ISO_A2', 'geometry']].rename(columns={'ISO_A2': 'zoneName'})

    df_selected = geo_df_all[geo_df_all['zoneName'].isin(countrylist)]

    for i in range(len(df_selected)):
        s=df_selected.iloc[i:i+1, :]
        s.to_file(os.path.join(outputfolder, f"{s['zoneName'].iloc[0]}.geojson"), driver='GeoJSON')


def load_zones(zones: list, d: pd.Timestamp):
    # make sure to select the right files for changed bidding zones
    # this is probably a little bit over explicit but that makes the confusing bidding zone situation a bit more clearer

    zones_corrected = []
    if d < pd.Timestamp('2021-01-01'):
        for zone in zones:
            if zone in ['IT_CNOR', 'IT_CSUD', 'IT_SUD']:
                zones_corrected.append(zone + '_2020')
            elif zone in ['IT_CALA']:
                raise ValueError(f'Zones {["IT_CALA"]} does not exist at this date')
            else:
                zones_corrected.append(zone)
    else:
        zones_corrected = zones

    return pd.concat([gpd.read_file(os.path.join(os.path.dirname(__file__), 'geojson', f'{x}.geojson')) for x in zones_corrected]).set_index('zoneName').sort_index()