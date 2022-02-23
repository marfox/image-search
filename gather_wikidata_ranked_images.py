#1/usr/bin/env python3
# coding: utf-8

from wmfdata.spark import get_session, run
from pyspark.sql import functions as F

SNAPSHOT = '2021-12-13'
OUTPUT = 'preferred_deprecated_p18'

spark = get_session(app_name='T293878')

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
    .withColumn('rank', F.lit('deprecated'))
    .union(
        preferred_p18.withColumn('rank', F.lit('preferred'))
    )
)
dataset.write.parquet(OUTPUT)

spark.stop()

