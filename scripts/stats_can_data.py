import requests
import geopandas as gpd
import pandas as pd
import shapely
import json


class StatsCanData:

    def get_census_tracts(self, geos='CT', cpt='47'):
        data = requests.get(
            'https://www12.statcan.gc.ca/rest/census-recensement/CR2016Geo.json',
            params=dict(
                lang='E',
                geos=geos,
                cpt=cpt
            )
        )
        data_dict = json.loads(data.content.decode().replace('//', ''))
        df = pd.DataFrame(data_dict['DATA'], columns=data_dict['COLUMNS'])
        return df

    def get_census_tract_geometry(self, dguid):
        data = requests.get(
            'https://geoprod.statcan.gc.ca/arcgis/rest/services/MD2DM2016_2021/MapServer/11/query',
            params=dict(f='json',
                        where="DGuid in ('{}')".format(dguid),
                        returnGeometry=True,
                        spatialRel='esriSpatialRelIntersects',
                        outFields='*',
                        outSR=4326)
        )
        print('Got geometry ', dguid)
        return shapely.geometry.Polygon(data.json()['features'][0]['geometry']['rings'][0])

    def get_census_tracts_data(self, dguid):
        data = requests.get(
            'https://www12.statcan.gc.ca/rest/census-recensement/CPR2016.json',
            params=dict(
                lang='E',
                dguid=dguid
            )
        )
        print('Got stats ', dguid)
        return pd.DataFrame(json.loads(data.content.decode().replace('//', ''))['DATA'],
                            columns=json.loads(data.content.decode().replace('//', ''))['COLUMNS'])

    def get_stats_can_data(self, target_geo_uid):
        census_tracts = self.get_census_tracts()
        print('Got census tracts')
        self.census_tracts_geo = gpd.GeoDataFrame(
            census_tracts[
                census_tracts.GEO_UID.isin(target_geo_uid)
            ],
            geometry=census_tracts[
                census_tracts.GEO_UID.isin(target_geo_uid)
            ].GEO_UID.map(self.get_census_tract_geometry).tolist()
        )
        print('Got census tracts geometery')
        self.census_tracts_stats = pd.concat(
            census_tracts[census_tracts.GEO_UID.isin(target_geo_uid)].GEO_UID.map(self.get_census_tracts_data).tolist()
            , axis=0
        )
        print('Got census tracts stats')
        self.census_tracts_geo.crs = {'init': 'epsg:4326'}
