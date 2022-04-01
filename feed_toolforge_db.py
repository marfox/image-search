#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Load the CSV dataset output by `collect_synonyms_search.py` and feed the given Toolforge DB table.
This script must run on `media-search-signal-test` in Toolforge.

DB credentials should be passed as a JSON file like this:
{
    "user": "USER",
    "password": "PASSWORD",
    "db": "DB",
    "table": "TABLE",
    "engine": "ENGINE",
    "host": "HOST"
}

"engine" is optional and defaults to "mysql+pymysql";
"host" is optional and defaults to "tools.db.svc.eqiad.wmflabs".
"""

import json
from sys import argv, exit

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Float, Integer, SmallInteger, String

ENGINE_TEMPLATE = '{engine}://{user}:{password}@{host}/{db}'
DEFAULT_ENGINE = 'mysql+pymysql'
DEFAULT_HOST = 'tools.db.svc.eqiad.wmflabs'


def main(args):
    if len(args) != 3:
        print('Usage: python {} INPUT_CSV CREDENTIALS_JSON'.format(__file__))
        return 1

    df = pd.read_csv(args[1])
    # Determine max length for string columns
    max_lengths = {
        'term': df['term'].str.len().max(),
        'language': df['language'].str.len().max(),
        'result': df['result'].str.len().max()
    }

    with open(args[2]) as fin:
        creds = json.load(fin)

    engine = create_engine(
        ENGINE_TEMPLATE.format(
            engine=creds.get('engine', DEFAULT_ENGINE),
            user=creds['user'],
            password=creds['password'],
            host=creds.get('host', DEFAULT_HOST),
            db=creds['db']
        )
    )

    schema = {
        'id': Integer(), 'search_id': Integer(),
        'term': String(max_lengths['term']),
        'language': String(max_lengths['language']),
        'result': String(max_lengths['result']),
        'score': Float(), 'rating': SmallInteger()
    }
    df['rating'] = None # Add rating column
    df.to_sql(name=creds['table'], con=engine, if_exists='replace', index_label='id', dtype=schema)

    return 0


if __name__ == '__main__':
    exit(main(argv))

