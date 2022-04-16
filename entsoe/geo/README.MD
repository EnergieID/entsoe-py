# ENTSO-E py geo files
This folder provides geojson files for all bidding zones on an best effort basis.  
Below examples are given on how to load it into python and plot it.  
If you want to use this subpackage please install the dependencies ```geopandas``` ```plotly``` and ```geojson-rewind```.
These were left out of the main package to not overburden dependencies for people who are not using this.

Not all bidding zones are supported yet, if you find good geojson files to add please open a pull request.  
A helper script ```utils.py``` that was used to parse some of the inputs is provided.

The files have been made compliant with RFC 7946 ring winding order as plotly requires, with help of the ```geojson-rewind``` package

The ```load_zones``` function automatically selects the configuration that was active on the given date. This then handles bidding zone changes.

## Data sources
For countries geojson in general the natural earth database is used, downloadable from here: https://www.naturalearthdata.com/downloads/10m-cultural-vectors/  
The ```parse.py``` script provides function to then extract all european countries from it with single bidding zone.

Then for countries that have multiple bidding zones we import from specific sites:
* Norway: https://temakart.nve.no/link/?link=vannkraft (direct download)
* Sweden: https://www.natomraden.se/ (direct downloading gives weird shapes so manually redrawn it)
* Italy: added together regions as specified here: https://www.mercatoelettrico.org/en/mercati/mercatoelettrico/zone.aspx

Thanks to [this comment](https://github.com/electricitymap/electricitymap-contrib/issues/917#issuecomment-364766643) which gave tips for this

## Plotting examples
Below assigns a unique number to all selected zones and plots a chloropleth map
```python
from utils import load_zones
import plotly.express as px
import pandas as pd

zones = ['some', 'ISO2', 'codes']

geo_df = load_zones(zones, pd.Timestamp('somedate'))
geo_df['value'] = range(1,len(geo_df)+1)

fig = px.choropleth(geo_df,
                   geojson=geo_df.geometry,
                   locations=geo_df.index,
                   color="value",
                   projection="mercator",
                   color_continuous_scale='rainbow')
fig.update_geos(fitbounds="locations", visible=False)
fig.show()

```