#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Collect a dataset to be labeled for Commons synonyms search evaluation.
Queries come from the production Commons search logs.
This script must run in an Analytics client machine, tested on stat1008.
It outputs a CSV file ready to be consumed by `feed_toolforge_db.py`."""

import random
from sys import argv, exit
from typing import Iterator

import requests

from pandas import concat, DataFrame as PandasDataFrame
from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
from wmfdata.spark import get_session

# Queries
N_SAMPLES = 1000
RANDOM_SEED = 1984
PHAB_TASK = 'T293878'
STOPWORDS = {
    'anal', 'circumcision', 'coitus', 'creampie', 'cum', 'cumshot',
    'ejaculation', 'erection', 'fellatio', 'genitalia', 'incest',
    'intercourse', 'masturbation', 'nude', 'orgasm', 'penis', 'porn',
    'sex', 'squirt', 'tits', 'urination', 'vagina', 'vulva'
}

# Results
TOP = 50
K = 12  # Amount of images that will be shown
API_ENDPOINT = 'https://commons.wikimedia.org/w/index.php'
COLUMNS = ('search_id', 'term', 'language', 'result', 'score',)


# Collect HTTP requests, parameters and geo info made against Commons search services
# Filters:
# - non-null HTTP parameters
# - non-null search results
# - non-null language parameter
# - Web source VS API (`source` = {'api', 'web'})
# - `title` parameter contains 'Special:Search' or 'Special:MediaSearch'
# - `referer` header contains 'index.php', as suggested in https://phabricator.wikimedia.org/T293878#7665381
def collect_searches(spark: SparkSession) -> SparkDataFrame:
    initial_query = """SELECT http, params, geocoded_data
    FROM event.mediawiki_cirrussearch_request
    WHERE database='commonswiki' AND params IS NOT NULL AND hits IS NOT NULL AND params.uselang IS NOT NULL AND source='web'
    """
    ddf = spark.sql(initial_query)
    filtered = (
        ddf
        .where(
            ddf.params.title.contains('Special:Search') | ddf.params.title.contains('Special:MediaSearch')
        )
        .where(
            ddf.http.request_headers.referer.contains('index.php')
        )
    )

    return filtered


# Sample queries based on traffic to get a mix of popular and rare ones,
# i.e., high & low query frequencies
def sample_queries(searches: SparkDataFrame) -> PandasDataFrame:
    ddf = searches.select(
        searches.params.search, searches.http.client_ip, searches.params.uselang
    )
    df = ddf.toPandas()  # Shouldn't be a big dataset, so local computation is OK
    df = df.rename(columns={
        'params[search]': 'term', 'http.client_ip': 'ip', 'params[uselang]': 'lang'
    })

    # Basic query terms normalization
    df.term = df.term.str.lower()
    df.term = df.term.str.strip()

    # Sort terms by descending order of frequency, IP-based
    # Include the set of languages per term
    # Grouping will merge terms that have become duplicates after normalization
    freqs = (
        df
        .groupby(df.term)
        .agg({'ip': 'count', 'lang': lambda lang: ','.join(set(lang))})
        .sort_values(by='ip', ascending=False)
        .reset_index()  # `groupby` turns the group into an index, we use a column instead
    )

    # Sampling weighted on traffic (frequencies): popular queries are more likely to be sampled
    # NOTE consider manual sampling from top & bottom slices, e.g.:
    #      threshold = 50
    #      top = freqs[freqs.ip >= threshold]
    #      bottom = freq[freqs.ip < threshold]
    #      top_sample = top.sample(n=N_SAMPLES / 2, random_state=RANDOM_SEED)
    #      bottom_sample = bottom.sample(n=N_SAMPLES / 2, random_state=RANDOM_SEED)
    sample = freqs.sample(n=N_SAMPLES, weights='ip', random_state=RANDOM_SEED)

    # Best effort to filter stopwords
    to_filter = []
    for word in STOPWORDS:
        for row in sample.itertuples():
            if word in row.term:
                to_filter.append(row.term)

    return sample[~sample.term.isin(to_filter)]



def fetch_results(queries: PandasDataFrame) -> Iterator:
    # Ensure to fetch `TOP` results
    params = {
        'mediasearch_synonyms': 1, 'cirrusDumpResult': 1, 'ns6': 1,
        'limit': TOP, 'search': None, 'uselang': None
    }

    search_id = 1
    for row in queries.itertuples():
        term = row.term
        lang = random.choice(row.lang.split(','))  # Pick a random language
        params['search'] = term
        params['uselang'] = lang

        response = requests.get(API_ENDPOINT, params=params)

        if not response.ok:
            print(f'Skipping request failed with HTTP {response.status_code} ...')
            continue

        try:
            json = response.json()
        except requests.exceptions.JSONDecodeError as jde:
            print('Skipping unexpected non-JSON response ...')
            continue

        try:
            results = json['__main__']['result']['hits']['hits']
        except KeyError as ke:
            print(f'Skipping response with missing JSON keys: {ke}')
            continue

        if not results:
            print(f'Skipping {lang} query with no results: {term}')
            continue

        # Sample `K` results from the ideal `TOP` to get a mix of likely positive and negative samples
        n_results = len(results)
        print(n_results)
        k = K if n_results >= K else n_results  # In case of too few results, just shuffle them
        yield sorted(
            random.sample(
                [(search_id, term, lang, r['_source']['title'], r['_score'],) for r in results], k
            ),
            key=lambda x: x[-1], reverse=True  # Sort by descending score
        )

        search_id += 1


def dump_output(results: Iterator, output_csv: str) -> None:
    df = concat([PandasDataFrame(r, columns=COLUMNS) for r in results])
    df.to_csv(output_csv, index=False)


def main(args):
    if len(args) != 2:
        print(f'Usage: python {__file__} OUTPUT_CSV')
        return 1

    spark = get_session(app_name=PHAB_TASK)

    searches = collect_searches(spark)
    sample = sample_queries(searches)

    spark.stop()

    results = fetch_results(sample)
    dump_output(results, args[1])

    return 0


if __name__ == '__main__':
    exit(main(argv))

