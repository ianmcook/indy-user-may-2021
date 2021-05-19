# Example 2: Floating-point errors

# Install and set up required packages
#install.packages("arrow")
#install.packages("dplyr")
#install.packages("sparklyr")
#sparklyr::spark_install()


# Load required packages
library(arrow)
library(dplyr)
library(sparklyr)


# Start a Spark session
sc <- spark_connect(master = "local")


# Demonstrate floating-point errors in R
txns <- tibble(amount = c(0.1, 0.1, 0.1, -0.3))
txns
txns %>% summarise(balance = sum(amount, na.rm = TRUE))


# Demonstrate floating-point errors in Spark
write_parquet(txns, "data/txns_float.parquet")
txns <- spark_read_parquet(sc, "txns", "data/txns_float.parquet")
txns

txns %>% summarise(balance = sum(amount, na.rm = TRUE))
tbl(sc, sql("SELECT SUM(amount) AS balance FROM txns"))


# Fix it by using fixed-precision decimal numbers
txns <- Table$create(amount = c(0.1, 0.1, 0.1, -0.3))
txns$amount <- txns$amount$cast(decimal(3,2))
txns
write_parquet(txns, "data/txns_decimal.parquet")


# Demonstrate the fix in Spark
txns <- spark_read_parquet(sc, "txns", "data/txns_decimal.parquet")
txns
txns %>% summarise(balance = sum(amount, na.rm = TRUE))
tbl(sc, sql("SELECT SUM(amount) AS balance FROM txns"))
