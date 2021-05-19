# Example 3: Working across filesystems

# Install required packages
#install.packages("arrow")
#install.packages("dplyr")


# Load required packages
library(arrow)
library(dplyr)


# Copy files from S3 to the local filesystem
dir.create("nyc-taxi/2013/12", recursive = TRUE)
arrow::copy_files("s3://ursa-labs-taxi-data/2013/12", "nyc-taxi/2013/12")


# Work with single-file datasets in local filesystems or in Amazon S3
taxi_local <- read_parquet("nyc-taxi/2013/12/data.parquet", as_data_frame = FALSE)
taxi_s3 <- read_parquet("s3://ursa-labs-taxi-data/2013/12/data.parquet")

# For more details, see https://arrow.apache.org/docs/r/articles/fs.html


# Work with large multi-file datasets in local filesystems or in Amazon S3
ds_local <- open_dataset("nyc-taxi", partitioning = c("year", "month"))
ds_s3 <- open_dataset("s3://ursa-labs-taxi-data/", partitioning = c("year", "month"))


# Manipulate and analyze large multi-file datasets with dplyr
ds_local %>%
  filter(total_amount > 100, year == 2015) %>%
  select(tip_amount, total_amount, passenger_count) %>%
  mutate(tip_pct = 100 * tip_amount / total_amount) %>%
  group_by(passenger_count) %>%
  collect() %>%
  summarise(
    median_tip_pct = median(tip_pct),
    n = n()
  ) %>%
  print()

# For more details, see https://arrow.apache.org/docs/r/articles/dataset.html
