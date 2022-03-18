#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# For each query:
#   TODO get synonyms search results via
#   https://commons.wikimedia.org/w/index.php?search=TERM&ns6=1&uselang=LANGUAGE&mediasearch_synonyms
#   TODO sample the top K results to get a mix of likely positive and negative samples
# TODO comply with the DB schema expected by the evaluation task front-end

from sys import argv, exit

from pandas import DataFrame as PandasDataFrame, concat
from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
from wmfdata.spark import get_session

N_SAMPLES = 1000
RANDOM_SEED = 1984
PHAB_TASK = 'T293878'
STOPWORDS = {
    'anal', 'circumcision', 'coitus', 'creampie', 'cum', 'cumshot',
    'ejaculation', 'erection', 'fellatio', 'genitalia', 'incest',
    'intercourse', 'masturbation', 'nude', 'orgasm', 'penis', 'porn',
    'sex', 'squirt', 'tits', 'urination', 'vagina', 'vulva'
}

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

    # Sampling weighted on frequencies, i.e., popular queries are more likely to be sampled
    # NOTE consider manual sampling from top & bottom slices, e.g.:
    # threshold = 50
    # top = freqs[freqs.ip >= threshold]
    # bottom = freq[freqs.ip < threshold]
    # top_sample = top.sample(n=N_SAMPLES / 2, random_state=RANDOM_SEED)
    # bottom_sample = bottom.sample(n=N_SAMPLES / 2, random_state=RANDOM_SEED)
    sample = freqs.sample(n=N_SAMPLES, weights='ip', random_state=RANDOM_SEED)

    # Best effort to filter stopwords
    to_filter = []
    for word in STOPWORDS:
        for row in sample.itertuples():
            if word in row.term:
                to_filter.append(row.term)

    return sample[~sample.term.isin(to_filter)]


def main(args):
    if len(args) != 2:
        print(f'Usage: python {__file__} OUTPUT_CSV')
        return 1

    spark = get_session(app_name=PHAB_TASK)

    searches = collect_searches(spark)
    sample = sample_queries(searches)
    sample.to_csv(args[1])

    spark.stop()
    return 0


if __name__ == '__main__':
    exit(main(argv))

