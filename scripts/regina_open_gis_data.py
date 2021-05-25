import requests
import zipfile
import io
import geopandas as gpd
import pandas as pd
import os
import shapely


class ReginaGISData:

    def get_connection_current_data(self):
        i = 0
        lead_survey = requests.get(
            'https://opengis.regina.ca/arcgis/rest/services/CGISViewer/DomesticWaterNetworkTrace/MapServer/4/query?f=json&where=(1%3D1)&returnGeometry=True&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=OBJECTID%20ASC&outSR=4326&resultOffset={}&resultRecordCount=10000'.format(
                i)

        )
        self.resp_data = lead_survey.json()['features']
        print(i)
        while len(lead_survey.json()['features']) != 0:
            i += len(lead_survey.json()['features'])
            lead_survey = requests.get(
                'https://opengis.regina.ca/arcgis/rest/services/CGISViewer/DomesticWaterNetworkTrace/MapServer/4/query?f=json&where=(1%3D1)&returnGeometry=True&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=OBJECTID%20ASC&outSR=4326&resultOffset={}&resultRecordCount=10000'.format(
                    i))
            self.resp_data.extend(lead_survey.json()['features'])
            print(i)
        self.attributes = pd.DataFrame([i['attributes'] for i in self.resp_data])

    def get_connection_snapshot_data(self):
        '''
        This method get the snapshot data for water connectors in Regina
        The snapshot is needed
        :return:
        '''
        i = 0
        lead_survey = requests.get(
            'https://opengis.regina.ca/arcgis/rest/services/Collector/CBMH_Survey_Map/MapServer/12/query?f=json&where=(1%3D1)&returnGeometry=True&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=OBJECTID%20ASC&outSR=4326&resultOffset={}&resultRecordCount=10000'.format(
                i))
        old_resp_data = lead_survey.json()['features']
        print(i)
        while len(lead_survey.json()['features']) != 0:
            i += len(lead_survey.json()['features'])
            lead_survey = requests.get(
                'https://opengis.regina.ca/arcgis/rest/services/Collector/CBMH_Survey_Map/MapServer/12/query?f=json&where=(1%3D1)&returnGeometry=True&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=OBJECTID%20ASC&outSR=4326&resultOffset={}&resultRecordCount=10000'.format(
                    i))
            old_resp_data.extend(lead_survey.json()['features'])
            print(i)

        self.old_attributes = pd.DataFrame([i['attributes'] for i in old_resp_data])

    def merge_connection_current_snapshot_comparisons(self):
        '''
        This method merges the snapshot connector data and current data to see what has been replaced overtime.
        :return:
        '''
        merged = self.attributes.merge(self.old_attributes[['MATERIAL', 'GISID', 'UPDATE_DATE']], on='GISID')
        target_data = merged[((merged.MATERIAL_x != merged.MATERIAL_y) & (merged.MATERIAL_y == 'Pb')) |
                             ((merged.MATERIAL_x == 'Pb') & (merged.UPDATE_DATE_x != merged.UPDATE_DATE_y))]
        target_data['replace_month'] = target_data.UPDATE_DATE_x.apply(pd.to_datetime, unit='ms').dt.to_period('M')
        to_replace = self.attributes[
            (self.attributes.MATERIAL == 'Pb') & (self.attributes.STATUS == 'ACTIVE') & (self.attributes.UPDATE_DATE == 1253836800000)]
        self.geo_data = gpd.GeoDataFrame(pd.DataFrame(
            [i['attributes'] for i in self.resp_data if i['attributes']['GISID'] in to_replace.GISID.to_list()]),
                                    geometry=[shapely.geometry.LineString(i['geometry']['paths'][0]) for i in self.resp_data
                                              if i['attributes']['GISID'] in to_replace.GISID.to_list()])
        self.geo_data_replaced = gpd.GeoDataFrame(pd.DataFrame(
            [i['attributes'] for i in self.resp_data if i['attributes']['GISID'] in target_data.GISID.to_list()]),
                                             geometry=[shapely.geometry.LineString(i['geometry']['paths'][0]) for i in
                                                       self.resp_data if
                                                       i['attributes']['GISID'] in target_data.GISID.to_list()])
        self.geo_data_replaced['replace_month'] = self.geo_data_replaced.UPDATE_DATE.apply(pd.to_datetime,
                                                                                 unit='ms').dt.to_period('M')
        self.geo_data.crs = {'init': 'epsg:4326'}
        self.geo_data_replaced.crs = {'init': 'epsg:4326'}
        self.geo_data['lon'] = self.geo_data.centroid.x
        self.geo_data['lat'] = self.geo_data.centroid.y
        self.geo_data_replaced['lon'] = self.geo_data_replaced.centroid.x
        self.geo_data_replaced['lat'] = self.geo_data_replaced.centroid.y

    def get_subdivision_data(self):
        resp_subdivision = requests.get(
            'https://opengis.regina.ca/arcgis/rest/services/OpenData/Subdivisions/MapServer/5/query?f=json&where=(1%3D1)&returnGeometry=True&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=OBJECTID%20ASC&outSR=4326&resultOffset=0&resultRecordCount=10000').json()[
            'features']
        sub_attributes = pd.DataFrame([i['attributes'] for i in resp_subdivision])
        self.sub_geo_data = gpd.GeoDataFrame(sub_attributes,
                                        geometry=[shapely.geometry.Polygon(i['geometry']['rings'][0]) for i in
                                                  resp_subdivision])
        self.sub_geo_data.crs = {'init': 'epsg:4326'}

    def merge_connection_subdivision_data(self):
        self.joined_data = gpd.sjoin(self.sub_geo_data, self.geo_data, how="inner", op='contains', lsuffix='', rsuffix='PB')
        self.pb_sub_count_df = self.joined_data[['OBJECTID_', 'SUB_NAME', 'geometry', 'GLOBALID_']].dissolve(
            by=['OBJECTID_', 'SUB_NAME'], aggfunc='count')

    def get_address_data(self):
        if not os.path.isdir('../data/shp.AddressParcels'):
            r = requests.get('https://ckanprodstorage.blob.core.windows.net/opendata/Address/SHP_ZIP/shp.AddressParcels.zip')
            z = zipfile.ZipFile(io.BytesIO(r.content))
            z.extractall("./shp.AddressParcels")
        self.addresses = gpd.read_file('../../Downloads/shp.AddressParcels/AddressParcels.shp')
        self.addresses.crs = {'init': 'epsg:26913'}
        self.addresses.to_crs(epsg=4326, inplace=True)
        self.addresses.crs = {'init': 'epsg:4326'}

    def get_schools(self):
        schools = requests.get(
            'https://opengis.regina.ca/arcgis/rest/services/OpenData/Schools/MapServer/0/query?f=json&where=(1%3D1)&returnGeometry=True&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=OBJECTID%20ASC&outSR=4326&resultOffset=0&resultRecordCount=10000')
        school_df = pd.DataFrame(schools.json()['features'])
        school_geo_df = gpd.GeoDataFrame(school_df.attributes.tolist(), geometry=school_df.geometry.map(
            lambda x: shapely.geometry.Point(x['x'], x['y'])))
        school_geo_df.crs = {'init': 'epsg:4326'}
        school_geo_df = school_geo_df[['NAME', 'ADDRESS', 'geometry']].dissolve(by=['NAME', 'ADDRESS']).reset_index()
        self.joined_school_data = gpd.sjoin(self.addresses, school_geo_df, how="left", op='contains')
        self.joined_school_data = self.joined_school_data[self.joined_school_data.NAME.isna()==False][['geometry','NAME','ADDRESS']]

    def get_data(self):
        self.get_connection_snapshot_data()
        self.get_connection_current_data()
        self.merge_connection_current_snapshot_comparisons()
        self.get_subdivision_data()
        self.merge_connection_subdivision_data()
        self.get_address_data()
        self.get_schools()