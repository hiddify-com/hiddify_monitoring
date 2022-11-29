from datetime import datetime
import maxminddb
from . import geolocator
import os

def rel_path(filename):
    """Return the path of this filename relative to the current script
    """
    res= f'{os.path.dirname(__file__)}/geodb/{filename}'
    return res
    # return os.path.join(os.getcwd(), os.path.dirname(__file__), f'../geodb/{filename}')


ipcity = maxminddb.open_database(rel_path('GeoLite2-City.mmdb'))
ipasn = maxminddb.open_database(rel_path('GeoLite2-ASN.mmdb'))

cache = {}


def get(ip):
    if ip in cache:
        return cache[ip]
    if len(cache) > 1000:
        cache.clear()

    ipres = ipcity.get(ip)
    # ciso = ipres['country']['iso_code']
    # timezone=ipcity.get('5.160.250.1')['location']['timezone']
    lng = ipres['location']['longitude']
    lat = ipres['location']['latitude']
    ip_info = geolocator.get((lat, lng))
    del ip_info['country']
    asnres = ipasn.get(ip)
    ip_info['asn'] = asnres['autonomous_system_number']
    ip_info['asn_name'] = asnres['autonomous_system_organization']
    cache[ip] = ip_info
    return ip_info
