#!/usr/bin/env python3

import argparse
import copy
import json
import sys
import time
from datetime import datetime, timedelta

import backoff
import requests
import singer
import pandas as pd
import numpy as np

LOGGER = singer.get_logger()
base_url = 'http://api.exchangeratesapi.io/v1/'
history_url = 'https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip'

logger = singer.get_logger()
session = requests.Session()
utcnow = datetime.utcnow()

DATE_FORMAT = '%Y-%m-%d'


def parse_response(r, base):
    rates = r['rates']

    # Only EUR is supported as base currency hence conversion is needed
    base_currency_eur_multiplier = 1 / rates[base]
    for k, v in rates.items():
        rates[k] = v * base_currency_eur_multiplier    

    rates[base] = 1.0
    rates['date'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.strptime(r['date'], DATE_FORMAT))
    return rates


schema = {'type': 'object',
          'properties':
              {'date': {'type': 'string',
                        'format': 'date-time'}}}


def giveup(error):
    logger.error(error.response.text)
    response = error.response
    return not (response.status_code == 429 or
                response.status_code >= 500)


@backoff.on_exception(backoff.constant,
                      (requests.exceptions.RequestException),
                      jitter=backoff.random_jitter,
                      max_tries=5,
                      giveup=giveup,
                      interval=30)
def request(url, params):
    response = requests.get(url=url, params=params)
    response.raise_for_status()
    return response


def get_historical_records(start_date, end_date, base):
    df = pd.read_csv(history_url, compression='zip')
    df = df[df['Date'] >= start_date]
    df = df[df['Date'] < end_date]
    df = df.replace({np.nan: None})
    df = df.drop('Unnamed: 42', axis=1, errors='ignore')
    df.rename(columns={'Date': 'date'}, inplace=True)

    if base == "EUR":
        df["EUR"] = 1.0

    records = df.to_dict("records")
    updated_records = []
    if base not in df and base != "EUR":
        LOGGER.info("Selected Base Currency(%s) column not found in historical data", base)
        LOGGER.info("Skipping historical data sync...")
        return []

    for rates in records:
        base_currency_eur_multiplier = ((1 / rates[base]) if rates[base] else rates[base])\
            if base != "EUR"\
            else 1
        if not base_currency_eur_multiplier:
            LOGGER.info("Skipping Row, Can't do conversion for date '%s', Base currency(%s) has value => NONE", rates["date"], base)
            continue

        data = {k: (v * base_currency_eur_multiplier) if isinstance(v, (float, int)) else v
                for k, v in rates.items()}
        data["date"] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.strptime(data["date"], DATE_FORMAT))
        updated_records.append(data)

    return df.columns, updated_records


def do_sync(config, start_date):
    base = config.get('base', 'USD')
    historical_end_date = utcnow - timedelta(days=14)
    if datetime.strptime(start_date, "%Y-%m-%d") < historical_end_date:
        headers, records = get_historical_records(start_date, historical_end_date.strftime('%Y-%m-%d'), base)
        for h in headers:
            if h != "date":
                schema['properties'][h] = {'type': ['null', 'number']}
        singer.write_schema('exchange_rate', schema, 'date')
        singer.write_records('exchange_rate', records)
        start_date = historical_end_date.strftime('%Y-%m-%d')

    state = {'start_date': start_date}
    next_date = start_date
    prev_schema = {}
    try:
        while datetime.strptime(next_date, DATE_FORMAT) <= datetime.utcnow():
            logger.info('Replicating exchange rate data from %s using base %s',
                        next_date,
                        base)

            response = request(base_url + next_date, {'access_key': config['access_key']})
            payload = response.json()

            # Update schema if new currency/currencies exist
            for rate in payload['rates']:
                if rate not in schema['properties']:
                    schema['properties'][rate] = {'type': ['null', 'number']}

            # Only write schema if it has changed
            if schema != prev_schema:
                singer.write_schema('exchange_rate', schema, 'date')

            if payload['date'] == next_date:
                singer.write_records('exchange_rate', [parse_response(payload, base)])

            state = {'start_date': next_date}
            next_date = (datetime.strptime(next_date, DATE_FORMAT) + timedelta(days=1)).strftime(DATE_FORMAT)
            prev_schema = copy.deepcopy(schema)

    except requests.exceptions.RequestException as e:
        logger.fatal('Error on ' + e.request.url +
                     '; received status ' + str(e.response.status_code) +
                     ': ' + e.response.text)
        singer.write_state(state)
        sys.exit(-1)

    singer.write_state(state)
    logger.info('Tap exiting normally')


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=False)
    parser.add_argument(
        '-s', '--state', help='State file', required=False)

    parser.add_argument(
        '-d', '--discover', action="store_true")

    args, _ = parser.parse_known_args()
    
    if args.discover:
        print("{}")
        return

    if args.config:
        with open(args.config) as file:
            config = json.load(file)
    else:
        config = {}

    if args.state:
        with open(args.state) as file:
            state = json.load(file)
    else:
        state = {}

    start_date = state.get('start_date') or config.get('start_date') or datetime.utcnow().strftime(DATE_FORMAT)
    start_date = singer.utils.strptime_with_tz(start_date).date().strftime(DATE_FORMAT)

    do_sync(config, start_date)


if __name__ == '__main__':
    main()
