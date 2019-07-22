#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import pandas as pd
import requests


class api():

    def __init__(self, id=None, password=None):
        ''' Get Token '''

        if id and password:
            url = 'https://api.e-kakashi.com/api/v1/auth'
            data = {'id': id, 'password': password}
            response = requests.post(
                url=url,
                data=data,
            )
            token = response.json()['token']
            self.token = token
        else:
            self.token = None
            print('No token is set.')

    def get_snlist(self):
        ''' Get list of sensor nodes '''

        url = 'https://api.e-kakashi.com/api/v1/sn'
        params = {'token': self.token}
        response = requests.get(
            url=url,
            params=params,
        )

        return response.json()

    def get_measure(self, snids, datetime_from, datetime_until, timezone='Asia/Tokyo', include_corrected=False):
        ''' Get measure data via api  '''

        if not isinstance(snids, list):
            raise Exception('`snids` must be list')

        #set timezone to from/until
        datetime_from = pd.to_datetime(datetime_from)
        datetime_from_utc = datetime_from.tz_localize(timezone).tz_convert('UTC')
        datetime_until = pd.to_datetime(datetime_until)
        datetime_until_utc = datetime_until.tz_localize(timezone).tz_convert('UTC')

        url = 'https://api.e-kakashi.com/api/v1/measure'
        params = {
            'token': self.token,
            'sn': ','.join(snids),
            'from': datetime_from_utc.strftime('%Y-%m-%d %H:%M:%S'),
            'to': datetime_until_utc.strftime('%Y-%m-%d %H:%M:%S'),
        }

        response = requests.get(
            url=url,
            params=params,
        )

        #error
        if not response.ok:
            raise Exception('API request error.')
        elif 'measures' not in response.json().keys():
            raise Exception('No measure data in response.')

        #parse json
        data = []
        for i, measure in enumerate(response.json()['measures']):
            snid = measure['sn']
            df = pd.DataFrame.from_dict(measure['measure'], dtype=float)

            if df.shape[0] == 0:
                print('No data found for', snid)
                continue

            df.set_index('datetime', inplace=True)
            df.index = pd.to_datetime(df.index)
            df.index = df.index.tz_convert(timezone)
            df['snid'] = snid
            df['mj'] = df['solarIrradiance'] * 60 * 10 / 10**6   # solar irradiance in MJ

            data.append(df)

        #concat data
        df = pd.concat(data)
        df.sort_index(inplace=True)

        #remove corrected data
        if not include_corrected:
            df = df[df.columns.drop(list(df.filter(regex='corrected')))]

        return df


if __name__ == '__main__':
    import os

    id = os.environ['API_ID']
    password = os.environ['API_PASS']
    api = api(id, password)

    df = api.get_measure(snids=['M03100000278', 'M03100000271'],
                         datetime_from='2018/11/30 00:00:00',
                         datetime_until='2018/12/02 23:59:59')
    print(df)
    print(df.groupby('snid').temperature.count())
