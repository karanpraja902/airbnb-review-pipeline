import polars as pl

df = pl.read_parquet('hdfs://localhost:9000/data')

print(df)
