#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# !pip install sqlalchemy pymysql

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

from sys import argv, exit

import pandas as pd
from sqlalchemy import create_engine

ENGINE_TEMPLATE = '{engine}://{user}:{password}@{host}/{db}'
DEFAULT_ENGINE = 'mysql+pymysql'
DEFAULT_HOST = 'tools.db.svc.eqiad.wmflabs'


def main(args):
    if len(args) != 3:
        print(f'Usage: python {__file__} INPUT_CSV CREDENTIALS_JSON')
        return 1

    df = pd.read_csv(args[1])
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

    # Drop score
    df.drop(columns='score').to_sql(name=creds['table'], con=engine, if_exists='append', index=False)

    return 0


if __name__ == '__main__':
    exit(main(argv))

