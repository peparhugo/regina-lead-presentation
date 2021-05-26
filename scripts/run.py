from regina_open_gis_data import ReginaGISData
from stats_can_data import StatsCanData
from saskatchewan_day_care import SaskatchewanDayCare
import pandas as pd
import geopandas as gpd
import numpy as np
from keplergl import KeplerGl

# custom income mapping by geo id
mapping_income_data = pd.DataFrame(
    [
        ['2016S05077050004.00', '$60709 - $81899'],
        ['2016S05077050005.00', '$60709 - $81899'],
        ['2016S05077050008.01', '$50322 - $60709'],
        ['2016S05077050009.01', '$50322 - $60709'],
        ['2016S05077050010.00', '$40264 - $50322'],
        ['2016S05077050011.00', '$24640 - $40264'],
        ['2016S05077050012.00', '$50322 - $60709'],
        ['2016S05077050013.00', '$24640 - $40264'],
        ['2016S05077050014.00', '$24640 - $40264'],
        ['2016S05077050017.00', '$60709 - $81899'],
        ['2016S05077050018.00', '$24640 - $40264'],
        ['2016S05077050019.00', '$40264 - $50322'],
        ['2016S05077050020.00', '$50322 - $60709'],
        ['2016S05077050022.01', '$40264 - $50322']
    ],
    columns=['GEO_UID', 'After Tax Income Range']
)

# get data
regina_data = ReginaGISData()
regina_data.get_data()

stats_can_data = StatsCanData()
stats_can_data.get_stats_can_data(mapping_income_data.GEO_UID.tolist())

day_care = SaskatchewanDayCare()
day_care.get_day_cares(regina_data.addresses)

# set the metrics to pull from stats can
dims = stats_can_data.census_tracts_stats[
    stats_can_data.census_tracts_stats.HIER_ID.str.strip().isin(
        [
            '1.1.4',
            '4.4.1.1',
            '4.4.1.2',
            '4.4.1.3',
            '4.2.1.2'
        ]
    )
][
    ['GEO_UID', 'TEXT_NAME_NOM', 'T_DATA_DONNEE']
].pivot(
    index='GEO_UID',
    columns='TEXT_NAME_NOM',
    values='T_DATA_DONNEE'
).reset_index()

dims.columns = [
    'GEO_UID',
    'Low Income - 0 to 17 years old',
    'Low Income - 18 to 64 years old',
    'Low Income - 65 years old and over ',
    'Median after-tax income of households in 2015',
    'Total private dwellings'
]

# merge connection data and stats can data
census_joined_data = gpd.sjoin(
    stats_can_data.census_tracts_geo,
    regina_data.geo_data,
    how="inner",
    op='contains',
    lsuffix='',
    rsuffix='PB'
)
# merge in income mapping
census_joined_data = census_joined_data.merge(mapping_income_data, on='GEO_UID')

# summarize lead data
pb_census_count_df = census_joined_data[
    ['GEO_UID', 'GEO_ID_CODE', 'geometry', 'GLOBALID', 'After Tax Income Range']
].dissolve(
    by=['GEO_UID', 'GEO_ID_CODE', 'After Tax Income Range'],
    aggfunc='count'
).reset_index().rename(
    columns={'GLOBALID': 'Lead Infrastructure Count'}
)

pb_census_count_df = gpd.sjoin(
    pb_census_count_df, regina_data.geo_data_replaced[
        regina_data.geo_data_replaced.replace_month >= "2019-12"
    ],
    how="left",
    op='contains'
)[
    [
        'GEO_UID',
        'GEO_ID_CODE',
        'geometry',
        'GLOBALID',
        'Lead Infrastructure Count',
        'After Tax Income Range'
    ]
].dissolve(
    by=['GEO_UID', 'GEO_ID_CODE', 'Lead Infrastructure Count', 'After Tax Income Range'],
    aggfunc='count'
).reset_index()
pb_census_count_df = pb_census_count_df.merge(dims, on='GEO_UID')

# remove this overlapping census track
pb_census_count_df = pb_census_count_df[pb_census_count_df.GEO_ID_CODE != '7050100.14']

# summarize data by median income range
pb_census_count_df = pb_census_count_df.drop(
    columns=['GEO_UID', 'GEO_ID_CODE']
).dissolve(
    by=['After Tax Income Range'],
    aggfunc='sum'
).reset_index()

# create calculated fields
pb_census_count_df[
    '% Replaced Since Dec 2019'
] = np.round(
    100 * pb_census_count_df['GLOBALID'] / (
        pb_census_count_df['GLOBALID'] + \
        pb_census_count_df['Lead Infrastructure Count']
    ),
    1
).astype(str).map(
    lambda x: '~' + x + "%"
)

pb_census_count_df[
    'Averge Low Income 0 to 17 years old per Dwelling'
] = np.round(
    pb_census_count_df['Low Income - 0 to 17 years old'] / pb_census_count_df['Total private dwellings'],
    2
)

pb_census_count_df[
    'Averge Low Income Individual per Dwelling'
] = (
    pb_census_count_df['Low Income - 0 to 17 years old'] + \
    pb_census_count_df['Low Income - 18 to 64 years old'] + \
    pb_census_count_df['Low Income - 65 years old and over ']
) / pb_census_count_df['Total private dwellings']

# merge day cares and school data
day_care_geo_df = day_care.day_care_geo_df[
    day_care.day_care_geo_df.intersects(pb_census_count_df.unary_union)
]
joined_school_data = regina_data.joined_school_data[
    regina_data.joined_school_data.intersects(pb_census_count_df.unary_union)
]

# generate map
map_2 = KeplerGl(height=500)
map_2.add_data(data=regina_data.geo_data[regina_data.geo_data.intersects(pb_census_count_df.unary_union)],
               name='Lead Infrastructure')
map_2.add_data(data=regina_data.geo_data_replaced[(regina_data.geo_data_replaced.replace_month >= "2019-12") & (
    regina_data.geo_data_replaced.intersects(pb_census_count_df.unary_union))].drop(columns=['replace_month']),
               name='replaced')
map_2.add_data(data=pb_census_count_df.rename(columns={'GLOBALID': 'Lead Infrastructure Replaced'}),
               name='Census Tracts')
map_2.add_data(data=day_care_geo_df[day_care_geo_df.intersects(pb_census_count_df.unary_union)], name='Day Care')
map_2.add_data(data=joined_school_data[joined_school_data.intersects(pb_census_count_df.unary_union)], name='Schools')
# TODO: configuration file for map formatting. Much easier to do this in jupyter.
map_2.save_to_html(file_name="regina_map.html")

pb_census_count_df.to_csv('lead_summary.csv')
