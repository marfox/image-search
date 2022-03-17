#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# TODO sample queries based on traffic to get a mix of popular and rare ones, i.e., high & low query frequencies
# For each query:
#   TODO get synonyms search results
#   TODO sample the top K results to get a mix of likely positive and negative samples
# TODO comply with the DB schema expected by the evaluation task front-end

from sys import argv, exit

from wmfdata.spark import get_session

PHAB_TASK = 'T293878'


# Collect HTTP requests, parameters and geo info made against Commons search services
# Filters:
# - non-null HTTP parameters
# - Web source VS API (`source` = {'api', 'web'})
# - `title` parameter contains 'Special:Search' or 'Special:MediaSearch'
# - `referer` header contains 'index.php', as suggested in https://phabricator.wikimedia.org/T293878#7665381
def collect_searches(spark_session):
    initial_query = """SELECT http, params, geocoded_data
    FROM event.mediawiki_cirrussearch_request
    WHERE database='commonswiki' AND params IS NOT NULL AND source='web'
    """
    ddf = spark_session.sql(initial_query)
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


def main(args):
    if len(args) != 2:
        print(f'Usage: python {__file__} OUTPUT_PARQUET')
        return 1

    spark = get_session(app_name=PHAB_TASK)

    ddf = collect_searches(spark)
    ddf.write.parquet(args[1])

    spark.stop()
    return 0


if __name__ == '__main__':
    exit(main(argv))

