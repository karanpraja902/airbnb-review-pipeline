# %%
from pathlib import Path
import polars as pl

# %%
data_path = Path("../data")
reviews_df = pl.read_csv(data_path / "reviews" / "reviews.csv")

reviews_df.write_parquet(data_path / "parquet" / "reviews.parquet")
