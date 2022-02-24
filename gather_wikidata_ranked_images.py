#1/usr/bin/env python3
# coding: utf-8

"""Gather a dataset of Commons images ranked in Wikidata as per https://phabricator.wikimedia.org/T293878#7727270.
It can be consumed by the MediaSearch evaluation, see https://github.com/cormacparle/media-search-signal-test"""

from wmfdata.spark import get_session, run
from pyspark.sql import functions as F

PHAB_TASK = 'T293878'
SNAPSHOT = '2021-12-13'
OUTPUT = 'preferred_deprecated_p18'

# The expected input is a SQL DB, so comply with its schema:
# https://github.com/cormacparle/media-search-signal-test/blob/24e01363e6ce5e80a9440a4faa40d52274286500/sql/ratedSearchResult.sql
# https://github.com/cormacparle/media-search-signal-test/blob/24e01363e6ce5e80a9440a4faa40d52274286500/sql/ratedSearchResult.latest.sql
# Evaluation script:
# https://github.com/cormacparle/media-search-signal-test/blob/24e01363e6ce5e80a9440a4faa40d52274286500/jobs/AnalyzeResults.php
#
# SQL schema dump:
# `id` int(11) NOT NULL AUTO_INCREMENT,
# `searchTerm` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
# `searchTermExactMatchWikidataId` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
# `language` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
# `result` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
# `rating` tinyint(4) DEFAULT NULL,
# PRIMARY KEY (`id`),
# UNIQUE KEY `searchTerm` (`searchTerm`,`language`,`result`),
# KEY `term_rating` (`searchTerm`,`rating`)
#
# All columns are strings except `id` (standard primary key) and `rating` (either 1 or -1)
COLUMNS = ['id', 'searchTerm', 'searchTermExactMatchWikidataId', 'language', 'result', 'rating']

spark = get_session(app_name=PHAB_TASK)

query = """SELECT id AS qid, claim.mainSnak.dataValue.value AS p18_image_title, sitelink.site AS wiki, sitelink.title AS page_title
FROM wmf.wikidata_entity
LATERAL VIEW OUTER EXPLODE(claims) AS claim
LATERAL VIEW EXPLODE(siteLinks) AS sitelink
WHERE snapshot='{snapshot}' AND typ='item' AND claim.mainSnak.property='P18' AND claim.rank='{rank}' AND sitelink.site!='commonswiki'
"""
deprecated_p18 = spark.sql(query.format(snapshot=SNAPSHOT, rank='deprecated'))
preferred_p18 = spark.sql(query.format(snapshot=SNAPSHOT, rank='preferred'))

dataset = (
    deprecated_p18
    .withColumn(COLUMNS[5], F.lit(-1))
    .union(
        preferred_p18.withColumn(COLUMNS[5], F.lit(1))
    )
    .withColumn(COLUMNS[0], F.monotonically_increasing_id())
    .withColumnRenamed('page_title', COLUMNS[1])
    .withColumnRenamed('qid', COLUMNS[2])
    # Get the language tag as the substring before 'wik' (should work for all wikis, wiktionary included)
    .withColumn(COLUMNS[3], F.substring_index('wiki', 'wik', 1))
    # This column contains leading and trailing double quotes. A stripping function doesn't seem to exist in pyspark
    # - `F.trim` only strips white spaces
    # - `F.translate(col, '"', '')` would delete eventual double quotes inside the string
    # - `F.regexp_replace` looks overcomplicated
    # - `F.substring(col, 2, len) won't work, since we can't compute the length of each string as in a vectorized function
    .withColumnRenamed('p18_image_title', COLUMNS[4])
    .select(*COLUMNS)
)

# TODO Can we write to the `s54568__fulltextSearchResults` SQL DB on Toolforge, owned by `media-search-signal-test`?
#      dataset,write.jdbc(url, table, mode=None, properties=None)
dataset.write.parquet(OUTPUT)

spark.stop()

