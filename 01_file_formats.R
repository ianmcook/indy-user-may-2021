# Example 1: File formats and compression algorithms

# Install required packages
#install.packages("arrow")
#install.packages("dplyr")
#install.packages("nycflights13")


# Load required packages
library(arrow)
library(dplyr)
library(nycflights13)


# Create a result dataset
result <- flights %>%
  filter(dest == "IND") %>%
  select(year, month, day, dep_delay, arr_delay, carrier, origin) %>%
  mutate(carrier = factor(carrier), origin = factor(origin))


# Write the result to Parquet files using various different options
write_parquet(
  result,
  "data/ind_flights_snappy.parquet"
)

write_parquet(
  result,
  "data/ind_flights_uncomp.parquet",
  compression = "uncompressed"
)

write_parquet(
  result,
  "data/ind_flights_zstd.parquet",
  compression = "zstd"
)

write_parquet(
  result,
  "data/ind_flights_uncomp_nodict.parquet",
  compression = "uncompressed",
  use_dictionary = FALSE
)

write_parquet(
  result,
  "data/ind_flights_tinychunks.parquet",
  chunk_size = 10
)

# For more Parquet writing options, see ?write_parquet

# For writing files in other formats, see ?write_feather and ?write_csv_arrow
