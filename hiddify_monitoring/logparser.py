import re
from datetime import datetime
from . import ipresolver
import hashlib

upstream_map = {'127.0.0.1:447': 'ss-faketls', '127.0.0.1:449': 'telegram-faketls', '127.0.0.1:448': 'v2ray', '127.0.0.1:445': 'vmess', '8.8.8.8:443': 'dns'}
lineformat = re.compile(
    r"""\[(?P<dateandtime>\d{2}\/[a-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} (\+|\-)\d{4})\] (?P<ipaddress>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) (?P<upstream>[0-9.:\-\W]+) (?P<status>\d+) (?P<download>\d+) (?P<upload>\d+) (?P<connectiontime>\d+)""", re.IGNORECASE)


def parse(line):
    data = re.search(lineformat, line)
    if data:
        datadic = data.groupdict()
        datadic['dateandtime'] = datetime.strptime(datadic['dateandtime'], "%d/%b/%Y:%H:%M:%S %z")
        date = datadic['dateandtime'].strftime('%Y%m%d')
        datadic['upstream'] = upstream_map.get(datadic['upstream'], datadic['upstream'])
        datadic['ipinfo'] = ipresolver.get(datadic['ipaddress'])
        datadic['connectiontime'] = float(datadic['connectiontime'])
        datadic['download'] = int(datadic['download'])
        datadic['upload'] = int(datadic['upload'])
        datadic['status'] = int(datadic['status'])

        datadic['haship'] = haship(f"{datadic['ipinfo']['asn_name']}{date}{haship(datadic['ipaddress'])}")
        del datadic['ipaddress']
        return datadic

def haship(str):
    return int(hashlib.sha1(str.encode("utf-8")).hexdigest(), 16)% (10 ** 8)