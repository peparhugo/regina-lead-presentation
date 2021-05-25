import requests
import geopandas as gpd
import pandas as pd
from bs4 import BeautifulSoup


class SaskatchewanDayCare:

    def get_day_cares(self, regina_addresses):
        temp_day_care_list = []
        page = 2
        day_care = requests.get(
            'https://www.saskatchewan.ca/residents/family-and-social-support/child-care/find-a-child-care-provider-in-my-community',
            params=dict(
                userLat=50.4791774,
                userLong=-104.6442839,
                searchRadius=50,
                address="Regina, SK S4R 4P7, Canada",
                page=1
            )
        )
        soup = BeautifulSoup(day_care.content, 'html.parser')
        while len(soup.find("div", class_="map-result").find_all('li')) != 0:
            print(page, len(soup.find("div", class_="map-result").find_all('li')))
            soup = BeautifulSoup(day_care.content, 'html.parser')
            for i in soup.find_all("div", class_="map-result"):
                for li in i.find_all('li'):
                    temp_day_care_dict = {}
                    temp_day_care_dict['type'] = li.text.replace('\r', '').split('\n')[1].strip()
                    temp_day_care_dict['name'] = li.find("strong").text
                    temp_day_care_dict['url'] = li.find("a")['href']
                    temp_day_care_dict['complete_address'] = li.find("a")['href'].rsplit('=', 1)[1]
                    temp_day_care_dict['FULLADDRES'] = li.find("a")['href'].rsplit('=', 1)[1].split(',')[0].upper()
                    if 'REGINA' in temp_day_care_dict['complete_address'].upper():
                        temp_day_care_list.append(temp_day_care_dict)
            day_care = requests.get(
                'https://www.saskatchewan.ca/residents/family-and-social-support/child-care/find-a-child-care-provider-in-my-community',
                params=dict(
                    userLat=50.4791774,
                    userLong=-104.6442839,
                    searchRadius=50,
                    address="Regina, SK S4R 4P7, Canada",
                    page=page
                )
                )
            page += 1
        day_care_df = pd.DataFrame(temp_day_care_list)
        day_care_df['FULLADDRES'] = day_care_df.FULLADDRES.map(self.fix_address)
        day_care_df = day_care_df.merge(regina_addresses, on='FULLADDRES', how='inner')
        self.day_care_geo_df = gpd.GeoDataFrame(day_care_df.drop(columns='geometry'), geometry=day_care_df.geometry)

    def fix_address(self, address):
        temp = address.split(' ', 1)
        print(temp)
        if 'NORTH' in address:
            append = 'N'
            temp = ' '.join([temp[0], append, temp[1].replace(' NORTH', '')])
        elif 'EAST' in address:
            append = 'E'
            temp = ' '.join([temp[0], append, temp[1].replace(' EAST', '')])
        else:
            temp = address
        return temp.replace('– ', '')
