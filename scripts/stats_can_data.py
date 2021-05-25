import requests
import geopandas as gpd
import pandas as pd
import shapely
import json


class StatsCanData:

    def get_census_tracts(self, geos='CT', cpt='47'):
        data = requests.get(
            'https://www12.statcan.gc.ca/rest/census-recensement/CR2016Geo.json?lang=E&geos={}&cpt={}'.format(geos,
                                                                                                              cpt),
            verify=False)
        data_dict = json.loads(data.content.decode().replace('//', ''))
        df = pd.DataFrame(data_dict['DATA'], columns=data_dict['COLUMNS'])
        return df

    def get_census_tract_geometry(self, dguid):
        data = requests.get(
            'https://geoprod.statcan.gc.ca/arcgis/rest/services/MD2DM2016_2021/MapServer/11/query?f=json&where=DGuid%20in%20(%27{}%27)&returnGeometry=true&spatialRel=esriSpatialRelIntersects&outFields=*&outSR=4326'.format(
                dguid), verify=False)
        print('Got geometry ', dguid)
        return shapely.geometry.Polygon(data.json()['features'][0]['geometry']['rings'][0])

    def get_census_tracts_data(self, dguid):
        data = requests.get(
            'https://www12.statcan.gc.ca/rest/census-recensement/CPR2016.json?lang=E&dguid={}'.format(dguid),
            verify=False)
        print('Got stats ', dguid)
        return pd.DataFrame(json.loads(data.content.decode().replace('//', ''))['DATA'],
                            columns=json.loads(data.content.decode().replace('//', ''))['COLUMNS'])

    def get_stats_can_data(self):
        census_tracts = self.get_census_tracts()
        print('Got census tracts')
        self.census_tracts_geo = gpd.GeoDataFrame(census_tracts,
                                                  geometry=census_tracts.GEO_UID.map(
                                                      self.get_census_tract_geometry).tolist())
        print('Got cenesus tracts geometery')
        self.census_tracts_stats = pd.concat(census_tracts.GEO_UID.map(self.get_census_tracts_data).tolist(), axis=0)
        print('Got census tracts stats')
        self.census_tracts_geo.crs = {'init': 'epsg:4326'}
