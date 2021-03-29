import enum
from typing import Union


def lookup_area(s: Union['Area', str]) -> 'Area':
    if isinstance(s, Area):
        # If it already is an Area object, we're happy
        area = s
    else:  # It is a string
        try:
            # If it is a "country code" string, we do a lookup
            area = Area[s]
        except KeyError:
            # It is not, it may be a direct code
            area = [area for area in Area if area.value == s][0]
    return area


class Area(enum.Enum):
    """
    ENUM containing 3 things about an Area: CODE, Meaning, Timezone
    """
    def __new__(cls, *args, **kwds):
        obj = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    # ignore the first param since it's already set by __new__
    def __init__(self, _: str, meaning: str, tz: str):
        self._meaning = meaning
        self._tz = tz

    def __str__(self):
        return self.value

    @property
    def meaning(self):
        return self._meaning

    @property
    def tz(self):
        return self._tz

    @property
    def code(self):
        return self.value

    # List taken directly from the API Docs
    DE_50HZ =       '10YDE-VE-------2', '50Hertz CA, DE(50HzT) BZA',                    'Europe/Berlin',
    AL =            '10YAL-KESH-----5', 'Albania, OST BZ / CA / MBA',                   'Europe/Tirane',
    DE_AMPRION =    '10YDE-RWENET---I', 'Amprion CA',                                   'Europe/Berlin',
    AT =            '10YAT-APG------L', 'Austria, APG BZ / CA / MBA',                   'Europe/Vienna',
    BY =            '10Y1001A1001A51S', 'Belarus BZ / CA / MBA',                        'Europe/Minsk',
    BE =            '10YBE----------2', 'Belgium, Elia BZ / CA / MBA',                  'Europe/Brussels',
    BA =            '10YBA-JPCC-----D', 'Bosnia Herzegovina, NOS BiH BZ / CA / MBA',    'Europe/Sarajevo',
    BG =            '10YCA-BULGARIA-R', 'Bulgaria, ESO BZ / CA / MBA',                  'Europe/Sofia',
    CZ_DE_SK =      '10YDOM-CZ-DE-SKK', 'BZ CZ+DE+SK BZ / BZA',                         'Europe/Prague',
    HR =            '10YHR-HEP------M', 'Croatia, HOPS BZ / CA / MBA',                  'Europe/Zagreb',
    CWE =           '10YDOM-REGION-1V', 'CWE Region',                                   'Europe/Brussels',
    CY =            '10YCY-1001A0003J', 'Cyprus, Cyprus TSO BZ / CA / MBA',             'Asia/Nicosia',
    CZ =            '10YCZ-CEPS-----N', 'Czech Republic, CEPS BZ / CA/ MBA',            'Europe/Prague',
    DE_AT_LU =      '10Y1001A1001A63L', 'DE-AT-LU BZ',                                  'Europe/Berlin',
    DE_LU =         '10Y1001A1001A82H', 'DE-LU BZ / MBA',                               'Europe/Berlin',
    DK =            '10Y1001A1001A65H', 'Denmark',                                      'Europe/Copenhagen',
    DK_1 =          '10YDK-1--------W', 'DK1 BZ / MBA',                                 'Europe/Copenhagen',
    DK_2 =          '10YDK-2--------M', 'DK2 BZ / MBA',                                 'Europe/Copenhagen',
    DK_CA =         '10Y1001A1001A796', 'Denmark, Energinet CA',                        'Europe/Copenhagen',
    EE =            '10Y1001A1001A39I', 'Estonia, Elering BZ / CA / MBA',               'Europe/Tallinn',
    FI =            '10YFI-1--------U', 'Finland, Fingrid BZ / CA / MBA',               'Europe/Helsinki',
    MK =            '10YMK-MEPSO----8', 'Former Yugoslav Republic of Macedonia, MEPSO BZ / CA / MBA', 'Europe/Skopje',
    FR =            '10YFR-RTE------C', 'France, RTE BZ / CA / MBA',                    'Europe/Paris',
    DE =            '10Y1001A1001A83F', 'Germany',                                      'Europe/Berlin'
    GR =            '10YGR-HTSO-----Y', 'Greece, IPTO BZ / CA/ MBA',                    'Europe/Athens',
    HU =            '10YHU-MAVIR----U', 'Hungary, MAVIR CA / BZ / MBA',                 'Europe/Budapest',
    IS =            'IS',               'Iceland',                                      'Atlantic/Reykjavik',
    IE_SEM =        '10Y1001A1001A59C', 'Ireland (SEM) BZ / MBA',                       'Europe/Dublin',
    IE =            '10YIE-1001A00010', 'Ireland, EirGrid CA',                          'Europe/Dublin',
    IT =            '10YIT-GRTN-----B', 'Italy, IT CA / MBA',                           'Europe/Rome',
    IT_SACO_AC =    '10Y1001A1001A885', 'Italy_Saco_AC',                                'Europe/Rome',
    IT_CALA =   '10Y1001C--00096J', 'IT-Calabria BZ',                                'Europe/Rome',
    IT_SACO_DC =    '10Y1001A1001A893', 'Italy_Saco_DC',                                'Europe/Rome',
    IT_BRNN =       '10Y1001A1001A699', 'IT-Brindisi BZ',                               'Europe/Rome',
    IT_CNOR =       '10Y1001A1001A70O', 'IT-Centre-North BZ',                           'Europe/Rome',
    IT_CSUD =       '10Y1001A1001A71M', 'IT-Centre-South BZ',                           'Europe/Rome',
    IT_FOGN =       '10Y1001A1001A72K', 'IT-Foggia BZ',                                 'Europe/Rome',
    IT_GR =         '10Y1001A1001A66F', 'IT-GR BZ',                                     'Europe/Rome',
    IT_MACRO_NORTH = '10Y1001A1001A84D', 'IT-MACROZONE NORTH MBA',                      'Europe/Rome',
    IT_MACRO_SOUTH = '10Y1001A1001A85B', 'IT-MACROZONE SOUTH MBA',                      'Europe/Rome',
    IT_MALTA =      '10Y1001A1001A877', 'IT-Malta BZ',                                  'Europe/Rome',
    IT_NORD =       '10Y1001A1001A73I', 'IT-North BZ',                                  'Europe/Rome',
    IT_NORD_AT =    '10Y1001A1001A80L', 'IT-North-AT BZ',                               'Europe/Rome',
    IT_NORD_CH =    '10Y1001A1001A68B', 'IT-North-CH BZ',                               'Europe/Rome',
    IT_NORD_FR =    '10Y1001A1001A81J', 'IT-North-FR BZ',                               'Europe/Rome',
    IT_NORD_SI =    '10Y1001A1001A67D', 'IT-North-SI BZ',                               'Europe/Rome',
    IT_PRGP =       '10Y1001A1001A76C', 'IT-Priolo BZ',                                 'Europe/Rome',
    IT_ROSN =       '10Y1001A1001A77A', 'IT-Rossano BZ',                                'Europe/Rome',
    IT_SARD =       '10Y1001A1001A74G', 'IT-Sardinia BZ',                               'Europe/Rome',
    IT_SICI =       '10Y1001A1001A75E', 'IT-Sicily BZ',                                 'Europe/Rome',
    IT_SUD =        '10Y1001A1001A788', 'IT-South BZ',                                  'Europe/Rome',
    RU_KGD =        '10Y1001A1001A50U', 'Kaliningrad BZ / CA / MBA',                    'Europe/Kaliningrad',
    LV =            '10YLV-1001A00074', 'Latvia, AST BZ / CA / MBA',                    'Europe/Riga',
    LT =            '10YLT-1001A0008Q', 'Lithuania, Litgrid BZ / CA / MBA',             'Europe/Vilnius',
    LU =            '10YLU-CEGEDEL-NQ', 'Luxembourg, CREOS CA',                         'Europe/Luxembourg',
    MT =            '10Y1001A1001A93C', 'Malta, Malta BZ / CA / MBA',                   'Europe/Malta',
    ME =            '10YCS-CG-TSO---S', 'Montenegro, CGES BZ / CA / MBA',               'Europe/Podgorica',
    GB =            '10YGB----------A', 'National Grid BZ / CA/ MBA',                   'Europe/London',
    NL =            '10YNL----------L', 'Netherlands, TenneT NL BZ / CA/ MBA',          'Europe/Amsterdam',
    NO_1 =          '10YNO-1--------2', 'NO1 BZ / MBA',                                 'Europe/Oslo',
    NO_2 =          '10YNO-2--------T', 'NO2 BZ / MBA',                                 'Europe/Oslo',
    NO_3 =          '10YNO-3--------J', 'NO3 BZ / MBA',                                 'Europe/Oslo',
    NO_4 =          '10YNO-4--------9', 'NO4 BZ / MBA',                                 'Europe/Oslo',
    NO_5 =          '10Y1001A1001A48H', 'NO5 BZ / MBA',                                 'Europe/Oslo',
    NO =            '10YNO-0--------C', 'Norway, Norway MBA, Stattnet CA',              'Europe/Oslo',
    PL_CZ =         '10YDOM-1001A082L', 'PL-CZ BZA / CA',                               'Europe/Warsaw',
    PL =            '10YPL-AREA-----S', 'Poland, PSE SA BZ / BZA / CA / MBA',           'Europe/Warsaw',
    PT =            '10YPT-REN------W', 'Portugal, REN BZ / CA / MBA',                  'Europe/Lisbon',
    MD =            '10Y1001A1001A990', 'Republic of Moldova, Moldelectica BZ/CA/MBA',  'Europe/Chisinau',
    RO =            '10YRO-TEL------P', 'Romania, Transelectrica BZ / CA/ MBA',         'Europe/Bucharest',
    RU =            '10Y1001A1001A49F', 'Russia BZ / CA / MBA',                         'Europe/Moscow',
    SE_1 =          '10Y1001A1001A44P', 'SE1 BZ / MBA',                                 'Europe/Stockholm',
    SE_2 =          '10Y1001A1001A45N', 'SE2 BZ / MBA',                                 'Europe/Stockholm',
    SE_3 =          '10Y1001A1001A46L', 'SE3 BZ / MBA',                                 'Europe/Stockholm',
    SE_4 =          '10Y1001A1001A47J', 'SE4 BZ / MBA',                                 'Europe/Stockholm',
    RS =            '10YCS-SERBIATSOV', 'Serbia, EMS BZ / CA / MBA',                    'Europe/Belgrade',
    SK =            '10YSK-SEPS-----K', 'Slovakia, SEPS BZ / CA / MBA',                 'Europe/Bratislava',
    SI =            '10YSI-ELES-----O', 'Slovenia, ELES BZ / CA / MBA',                 'Europe/Ljubljana',
    GB_NIR =        '10Y1001A1001A016', 'Northern Ireland, SONI CA',                    'Europe/Belfast',
    ES =            '10YES-REE------0', 'Spain, REE BZ / CA / MBA',                     'Europe/Madrid',
    SE =            '10YSE-1--------K', 'Sweden, Sweden MBA, SvK CA',                   'Europe/Stockholm',
    CH =            '10YCH-SWISSGRIDZ', 'Switzerland, Swissgrid BZ / CA / MBA',         'Europe/Zurich',
    DE_TENNET =     '10YDE-EON------1', 'TenneT GER CA',                                'Europe/Berlin',
    DE_TRANSNET =   '10YDE-ENBW-----N', 'TransnetBW CA',                                'Europe/Berlin',
    TR =            '10YTR-TEIAS----W', 'Turkey BZ / CA / MBA',                         'Europe/Istanbul',
    UA =            '10Y1001C--00003F', 'Ukraine, Ukraine BZ, MBA',                     'Europe/Kiev',
    UA_DOBTPP =     '10Y1001A1001A869', 'Ukraine-DobTPP CTA',                           'Europe/Kiev',
    UA_BEI =        '10YUA-WEPS-----0', 'Ukraine BEI CTA',                              'Europe/Kiev',
    UA_IPS =        '10Y1001C--000182', 'Ukraine IPS CTA',                              'Europe/Kiev'


PSRTYPE_MAPPINGS = {
    'A03': 'Mixed',
    'A04': 'Generation',
    'A05': 'Load',
    'B01': 'Biomass',
    'B02': 'Fossil Brown coal/Lignite',
    'B03': 'Fossil Coal-derived gas',
    'B04': 'Fossil Gas',
    'B05': 'Fossil Hard coal',
    'B06': 'Fossil Oil',
    'B07': 'Fossil Oil shale',
    'B08': 'Fossil Peat',
    'B09': 'Geothermal',
    'B10': 'Hydro Pumped Storage',
    'B11': 'Hydro Run-of-river and poundage',
    'B12': 'Hydro Water Reservoir',
    'B13': 'Marine',
    'B14': 'Nuclear',
    'B15': 'Other renewable',
    'B16': 'Solar',
    'B17': 'Waste',
    'B18': 'Wind Offshore',
    'B19': 'Wind Onshore',
    'B20': 'Other',
    'B21': 'AC Link',
    'B22': 'DC Link',
    'B23': 'Substation',
    'B24': 'Transformer'}

DOCSTATUS = {'A01': 'Intermediate',
             'A02': 'Final',
             'A05': 'Active',
             'A09': 'Cancelled',
             'X01': 'Estimated'}

BSNTYPE = {'A29': 'Already allocated capacity (AAC)',
           'A43': 'Requested capacity (without price)',
           'A46': 'System Operator redispatching',
           'A53': 'Planned maintenance',
           'A54': 'Unplanned outage',
           'A85': 'Internal redispatch',
           'A95': 'Frequency containment reserve',
           'A96': 'Automatic frequency restoration reserve',
           'A97': 'Manual frequency restoration reserve',
           'A98': 'Replacement reserve',
           'B01': 'Interconnector network evolution',
           'B02': 'Interconnector network dismantling',
           'B03': 'Counter trade',
           'B04': 'Congestion costs',
           'B05': 'Capacity allocated (including price)',
           'B07': 'Auction revenue',
           'B08': 'Total nominated capacity',
           'B09': 'Net position',
           'B10': 'Congestion income',
           'B11': 'Production unit'}

MARKETAGREEMENTTYPE = {'A01': 'Daily',
                       'A02': 'Weekly',
                       'A03': 'Monthly',
                       'A04': 'Yearly',
                       'A05': 'Total',
                       'A06': 'Long term',
                       'A07': 'Intraday',
                       'A13': 'Hourly'}

DOCUMENTTYPE = {'A09': 'Finalised schedule',
                'A11': 'Aggregated energy data report',
                'A15': 'Acquiring system operator reserve schedule',
                'A24': 'Bid document',
                'A25': 'Allocation result document',
                'A26': 'Capacity document',
                'A31': 'Agreed capacity',
                'A38': 'Reserve allocation result document',
                'A44': 'Price Document',
                'A61': 'Estimated Net Transfer Capacity',
                'A63': 'Redispatch notice',
                'A65': 'System total load',
                'A68': 'Installed generation per type',
                'A69': 'Wind and solar forecast',
                'A70': 'Load forecast margin',
                'A71': 'Generation forecast',
                'A72': 'Reservoir filling information',
                'A73': 'Actual generation',
                'A74': 'Wind and solar generation',
                'A75': 'Actual generation per type',
                'A76': 'Load unavailability',
                'A77': 'Production unavailability',
                'A78': 'Transmission unavailability',
                'A79': 'Offshore grid infrastructure unavailability',
                'A80': 'Generation unavailability',
                'A81': 'Contracted reserves',
                'A82': 'Accepted offers',
                'A83': 'Activated balancing quantities',
                'A84': 'Activated balancing prices',
                'A85': 'Imbalance prices',
                'A86': 'Imbalance volume',
                'A87': 'Financial situation',
                'A88': 'Cross border balancing',
                'A89': 'Contracted reserve prices',
                'A90': 'Interconnection network expansion',
                'A91': 'Counter trade notice',
                'A92': 'Congestion costs',
                'A93': 'DC link capacity',
                'A94': 'Non EU allocations',
                'A95': 'Configuration document',
                'B11': 'Flow-based allocations'}

PROCESSTYPE = {
    'A01': 'Day ahead',
    'A02': 'Intra day incremental',
    'A16': 'Realised',
    'A18': 'Intraday total',
    'A31': 'Week ahead',
    'A32': 'Month ahead',
    'A33': 'Year ahead',
    'A39': 'Synchronisation process',
    'A40': 'Intraday process',
    'A46': 'Replacement reserve',
    'A47': 'Manual frequency restoration reserve',
    'A51': 'Automatic frequency restoration reserve',
    'A52': 'Frequency containment reserve',
    'A56': 'Frequency restoration reserve'
}

# neighbouring bidding zones that have cross_border flows
NEIGHBOURS = {
    'BE': ['NL', 'DE_AT_LU', 'FR', 'GB', 'DE_LU'],
    'NL': ['BE', 'DE_AT_LU', 'DE_LU', 'GB', 'NO_2', 'DK_1'],
    'DE_AT_LU': ['BE', 'CH', 'CZ', 'DK_1', 'DK_2', 'FR', 'IT_NORD', 'IT_NORD_AT', 'NL', 'PL', 'SE_4', 'SI'],
    'FR': ['BE', 'CH', 'DE_AT_LU', 'DE_LU', 'ES', 'GB', 'IT_NORD', 'IT_NORD_FR'],
    'CH': ['AT', 'DE_AT_LU', 'DE_LU', 'FR', 'IT_NORD', 'IT_NORD_CH'],
    'AT': ['CH', 'CZ', 'DE_LU', 'HU', 'IT_NORD', 'SI'],
    'CZ': ['AT', 'DE_AT_LU', 'DE_LU', 'PL', 'SK'],
    'GB': ['BE', 'FR', 'IE_SEM', 'NL'],
    'NO_2': ['DK_1', 'NL', 'NO_5'],
    'HU': ['AT', 'HR', 'RO', 'RS', 'SK', 'UA'],
    'IT_NORD': ['CH', 'DE_AT_LU', 'FR', 'SI', 'AT', 'IT_CNOR'],
    'ES': ['FR', 'PT'],
    'SI': ['AT', 'DE_AT_LU', 'HR', 'IT_NORD'],
    'RS': ['AL', 'BA', 'BG', 'HR', 'HU', 'ME', 'MK', 'RO'],
    'PL': ['CZ', 'DE_AT_LU', 'DE_LU', 'LT', 'SE_4', 'SK', 'UA'],
    'ME': ['AL', 'BA', 'RS'],
    'DK_1': ['DE_AT_LU', 'DE_LU', 'DK_2', 'NO_2', 'SE_3', 'NL'],
    'RO': ['BG', 'HU', 'RS', 'UA'],
    'LT': ['BY', 'LV', 'PL', 'RU_KGD', 'SE_4'],
    'BG': ['GR', 'MK', 'RO', 'RS', 'TR'],
    'SE_3': ['DK_1', 'FI', 'NO_1', 'SE_4'],
    'LV': ['EE', 'LT', 'RU'],
    'IE_SEM': ['GB'],
    'BA': ['HR', 'ME', 'RS'],
    'NO_1': ['NO_2', 'NO_3', 'NO_5', 'SE_3'],
    'SE_4': ['DE_AT_LU', 'DE_LU', 'DK_2', 'LT', 'PL'],
    'NO_5': ['NO_1', 'NO_2', 'NO_3'],
    'SK': ['CZ', 'HU', 'PL', 'UA'],
    'EE': ['FI', 'LV', 'RU'],
    'DK_2': ['DE_AT_LU', 'DE_LU', 'SE_4'],
    'FI': ['EE', 'NO_4', 'RU', 'SE_1', 'SE_3'],
    'NO_4': ['SE_2', 'FI', 'SE_1'],
    'SE_1': ['FI', 'NO_4', 'SE_2'],
    'SE_2': ['NO_3', 'NO_4', 'SE_3'],
    'DE_LU': ['AT', 'BE', 'CH', 'CZ', 'DK_1', 'DK_2', 'FR', 'NL', 'PL', 'SE_4'],
    'MK': ['BG', 'GR', 'RS'],
    'PT': ['ES'],
    'GR': ['AL', 'BG', 'IT_BRNN', 'IT_GR', 'MK', 'TR'],
    'NO_3': ['NO_4', 'NO_5', 'SE_2'],
    'IT': ['AT', 'FR', 'GR', 'MT', 'ME', 'SI', 'CH'],
    'IT_BRNN': ['GR', 'IT_SUD'],
    'IT_SUD': ['IT_BRNN', 'IT_CSUD', 'IT_FOGN', 'IT_ROSN', 'IT_CALA'],
    'IT_FOGN': ['IT_SUD'],
    'IT_ROSN': ['IT_SICI', 'IT_SUD'],
    'IT_CSUD': ['IT_CNOR', 'IT_SARD', 'IT_SUD'],
    'IT_CNOR': ['IT_NORD', 'IT_CSUD', 'IT_SARD'],
    'IT_SARD': ['IT_CNOR', 'IT_CSUD'],
    'IT_SICI': ['IT_CALA', 'IT_ROSN', 'MT'],
    'IT_CALA': ['IT_SICI', 'IT_SUD'],
    'MT': ['IT_SICI']
}
