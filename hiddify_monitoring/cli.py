import pandas as pd
import psutil
import argparse
import os
import tempfile
from . import logparser
import signal


def main():  # pragma: no cover
    from . import geolocator
    parser = argparse.ArgumentParser(
        prog='Hiddify Monitoring',
        description='parse nginx log, anonymize it and provide meaningful info',
        epilog='Hiddify')

    parser.add_argument('nginx_log_file')
    parser.add_argument('out_folder', default='out', nargs='?')
    args = parser.parse_args()
    print(args)
    process(args.nginx_log_file, args.out_folder)
    print('done')


def process(orig_nginx_log, out_folder):
    if not os.path.isfile(orig_nginx_log):
        raise Exception('Error! logfile not found')
    is_test = 'tests/test.log' in orig_nginx_log

    processing_file = f'{orig_nginx_log}'
    if not is_test:
        processing_file += f'.processing'

    try:
        os.rename(orig_nginx_log, processing_file)
        pass
    except:
        raise
        pass
    send_signal_to_nginx()
    analyse(processing_file, out_folder)

    if not is_test:
        os.remove(processing_file)


def analyse(logfile, out_folder):
    df = convertlog(logfile)

    # df.rolling('1d').sum()

    def defgroups(ddf):
        return [ddf.index.year, ddf.index.month, ddf.index.day, ddf.index.hour]

    def calc_items(ddf):
        res = ddf[['download', 'upload', 'connectiontime','haship']].agg({'download':'sum','upload':'sum','connectiontime': 'sum','haship':'count'}).rename(columns={'haship':'connection_count'})
        # names=list(res.index.names)
        # names[0]='year';names[1]='month';names[2]='day';names[3]='hour'
        # res.index.names=names
        return res

    # df=df.loc[(df['download']>100000) |(df['upload']>100000)]
    per_hour_df = calc_items(df.groupby(df.index.floor('1h')))

    h1 = pd.to_timedelta('1h')

    for row in per_hour_df.index:
        # row=row.round('1h')
        print(row)
        df2 = df.loc[(df.index >= row) & (df.index < row+h1)]
        per_proto_df = calc_items(df2.groupby(['status', 'upstream']))
        per_city_df = calc_items(df2.groupby(
            ['status', 'upstream', 'country_code', 'province', 'city']))
        per_asn_df = calc_items(df2.groupby(
            ['status', 'upstream', 'asn_name']))
        add_log(row, 'proto', per_proto_df, out_folder)
        add_log(row, 'city', per_city_df, out_folder)
        add_log(row, 'asn', per_asn_df, out_folder)

        full_df = calc_items(df2.groupby(['status', 'upstream', 'country_code', 'province', 'city', 'asn_name', 'haship']))
        
        add_log(row, 'full', full_df, out_folder)

        uniqueusers = df2.groupby(['status', 'upstream', 'haship'])[
            ['haship']].count().rename(columns={'haship': 'connection_count'})

        add_log(row, 'users', uniqueusers, out_folder)


def add_log(dateh, typ, df, out_folder):

    folder = f'{out_folder}/{typ}/{dateh.strftime("%Y%m%d")}'
    os.makedirs(folder, exist_ok=True)
    filepath = f'{folder}/{dateh.strftime("%H")}.csv'
    old_df = None
    if os.path.isfile(filepath):
        old_df = pd.read_csv(filepath, index_col=df.index.names, dtype={
                             'haship': 'Int64'})

    # display(df)
    # display(old_df)
    if old_df is not None:
        # if typ=='users':
        #     df=pd.concat([old_df,df], ignore_index=True, sort=False).drop_duplicates(subset=['haship'])
        # else:
        df = df.add(old_df, fill_value=0)
    # df = df.drop(['up/s', 'dl/s'], axis=1, errors='ignore')
    # df['up/s'] = (df['upload']/df['connectiontime']).fillna(0).astype(int)
    # df['dl/s'] = (df['download']/df['connectiontime']).fillna(0).astype(int)
    if len(df.index) > 0:
        df.to_csv(filepath)


def send_signal_to_nginx():
    PROCNAME = "nginx"
    for proc in psutil.process_iter():
        if PROCNAME in proc.name():
            os.kill(proc.pid, signal.SIGUSR1)


def convertlog(logpath):
    print('converting ', logpath)
    with open(logpath, 'r', encoding='utf-8') as f:
        alldata = [logparser.parse(l) for l in f.readlines()]
        alldata = [l for l in alldata if l is not None]

    import pandas as pd

    df = pd.DataFrame(alldata)
    # all
    df = pd.concat([df.drop('ipinfo', axis=1),
                   pd.DataFrame(df['ipinfo'].tolist())], axis=1)
    df = df.set_index('dateandtime')

    return df
