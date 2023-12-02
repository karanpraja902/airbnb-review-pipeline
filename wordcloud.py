from pyspark.sql import functions as F
from pyspark.sql import SQLContext
from pyspark import SparkContext

import matplotlib.pyplot as plt
from wordcloud import WordCloud, wordcloud

from collections import ChainMap


sc = SparkContext()

sql_context = SQLContext(sc)

df = sql_context.read.parquet("hdfs://localhost:9000/data")

# +----------+------------------+----------+-----------+-------------+--------------------+---------+
# |listing_id|                id|      date|reviewer_id|reviewer_name|            comments|datamonth|
# +----------+------------------+----------+-----------+-------------+--------------------+---------+
# |    590681|882463965265288239|2023-05-02|    5314612|      Nikolas|Wonderful place. ...|  2023-05|
# |      2818|881762416558697470|2023-05-01|  249593178|        Clara|Daniel est un hôt...|  2023-05|


wordcloud = WordCloud(background_color="white")

# Create a list of all the comments
comments = df.select("comments").rdd.flatMap(lambda x: x).collect()

# Create a wordcloud of the comments
wordcloud.generate(" ".join(comments))

# Display the generated image:
# the matplotlib way:
# save the image
wordcloud.to_file("wordcloud.png")

sc.stop()
