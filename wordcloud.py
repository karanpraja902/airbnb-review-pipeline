import matplotlib.pyplot as plt
from pyspark import SparkContext
from pyspark.sql import SQLContext

from wordcloud import WordCloud

sc = SparkContext()

sql_context = SQLContext(sc)

df = sql_context.read.parquet("hdfs://localhost:9000/airbnb_reviews")

wordcloud = WordCloud(width=800, height=800, background_color="white", min_font_size=10)

comments = df.select("comments").rdd.flatMap(lambda x: x).collect()

wordcloud.generate(" ".join(comments))

wordcloud.to_file("wordcloud.png")

sc.stop()
