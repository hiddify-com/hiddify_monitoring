# -*- coding: utf-8 -*-

import re
import collections
import csv
import logging
import os
import sys
import zipfile
if sys.platform == 'win32':
    csv.field_size_limit(2**31-1)
else:
    csv.field_size_limit(sys.maxsize)
try:
    from urllib import urlretrieve
except ImportError:
    from urllib.request import urlretrieve
from scipy.spatial import cKDTree as KDTree

# location of geocode data to download
GEOCODE_URL = 'http://download.geonames.org/export/dump/cities500.zip'
GEOCODE_FILENAME = 'cities500.txt'

# GEOCODE_URL = 'http://download.geonames.org/export/dump/IR.zip'
# GEOCODE_FILENAME = 'IR.txt'

PROVINCE_URL = 'https://download.geonames.org/export/dump/admin1CodesASCII.txt'
PROVINCE_FILENAME = 'admin1CodesASCII.txt'


def singleton(cls):
    """Singleton pattern to avoid loading class multiple times
    """
    instances = {}

    def getinstance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]
    return getinstance


@singleton
class GeocodeData:

    def __init__(self):
        coordinates, self.locations = self.extract()
        self.tree = KDTree(coordinates)
        self.load_countries(rel_path('countries.csv'))

    def load_countries(self, country_filename):
        """Load a map of country code to name
        """
        self.countries = {}
        for code, name in csv.reader(open(country_filename)):
            self.countries[code] = name

    def query(self, coordinates):
        """Find closest match to this list of coordinates
        """
        try:
            distances, indices = self.tree.query(coordinates, k=1)
        except ValueError as e:
            logging.info('Unable to parse coordinates: {}'.format(coordinates))
            raise e
        else:
            results = [self.locations[index] for index in indices]
            for result in results:
                result['country'] = self.countries.get(result['country_code'], '')
            return results

    def download(self):
        """Download geocode file
        """
        pffile=rel_path(PROVINCE_FILENAME)
        if not os.path.exists(pffile):
            logging.info('Downloading: {}'.format(PROVINCE_URL))
            urlretrieve(PROVINCE_URL, pffile)
        
        local_filename = rel_path(os.path.basename(GEOCODE_URL))
        print('dddddddd',local_filename)
        if not os.path.exists(local_filename):
            logging.info('Downloading: {}'.format(GEOCODE_URL))
            urlretrieve(GEOCODE_URL, local_filename)

        return local_filename

    def extract(self):
        """Extract geocode data from zip
        """
        compact_file=rel_path('geocode.csv')
        if not os.path.exists(compact_file):
            
            # remove GEOCODE_FILENAME to get updated data
            filename = self.download()
            z = zipfile.ZipFile(filename)
            logging.info('Extracting: {}'.format(rel_path(GEOCODE_FILENAME)))
            with open(rel_path(GEOCODE_FILENAME), 'wb') as f:
                f.write(z.read(GEOCODE_FILENAME))

            # extract coordinates into more compact CSV for faster loading
            with open(compact_file, 'w', encoding='utf-8', newline='') as writerfile:
                writer = csv.writer(writerfile)
                rows = []
                code2name = {}
                with open(rel_path(PROVINCE_FILENAME), 'r', encoding='utf-8') as f:
                    for row in csv.reader(f, delimiter='\t'):
                        code2name[row[0]] = row[2]
                with open(rel_path(GEOCODE_FILENAME), 'r', encoding='utf-8') as f:
                    for row in csv.reader(f, delimiter='\t'):
                        latitude, longitude = row[4:6]
                        country_code = row[8]
                        if latitude and longitude and country_code:
                            city = row[2]
                            if country_code == 'IR':
                                cityfa = get_persian(row[3]) or city
                            else:
                                cityfa = ''

                            province = code2name.get(f'{country_code}.{row[10]}', '?')
                            if province == '-':
                                print(f'province for {row[0]} {city} {cityfa} not found {row[10]}')
                            row = latitude, longitude, country_code, city, cityfa, province
                            writer.writerow(row,)
                            rows.append(row)

        # load a list of known coordinates and corresponding locations
        with open(compact_file, 'r', encoding='utf-8') as f:
            # rows = [row for row in ]

            coordinates, locations = [], []
            for latitude, longitude, country_code, city, cityfa, province in csv.reader(f):
                coordinates.append((latitude, longitude))
                locations.append(dict(country_code=country_code, city=city, cityfa=cityfa, province=province))
        return coordinates, locations


persian = re.compile(r"[\u0600-\u06FF\s]{2,}")


def get_persian(strlist):
    for s in strlist.split(','):
        if persian.search(s):
            return s


def rel_path(filename):
    """Return the path of this filename relative to the current script
    """
    res= f'{os.path.dirname(__file__)}/geodb/{filename}'
    # print(filename,res)
    
    return res


def get(coordinate):
    """Search for closest known location to this coordinate
    """
    gd = GeocodeData()
    return gd.query([coordinate])[0]


def search(coordinates):
    """Search for closest known locations to these coordinates
    """
    gd = GeocodeData()
    return gd.query(coordinates)


if __name__ == '__main__':
    # test some coordinate lookups
    city1 = -37.81, 144.96
    city2 = 31.76, 35.21
    print(get(city1))
    print(search([city1, city2]))
