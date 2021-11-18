library(arrow)
library(duckdb)
library(tidyverse)

## https://arrow.apache.org/docs/r/articles/dataset.html
setwd("~")
arrow::arrow_with_s3()


# local_setup -------------------------------------------------------------

# Retrieving data from a public Amazon S3 bucket

# arrow::copy_files(from = "s3://ursa-labs-taxi-data", to = to_path)



# open_data ---------------------------------------------------------------
to_path <- "Desktop/arrow_tests/nyc-taxi"
partions <-
  c("year", "month")

ds <-
  arrow::open_dataset(to_path, partitioning = partions,format = "parquet")

ds
names(ds)

# Querying the dataset
# Up to this point, you haven’t loaded any data. You’ve walked directories to find files, you’ve parsed file paths to identify partitions, and you’ve read the headers of the Parquet files to inspect their schemas so that you can make sure they all are as expected.
# In the current release, arrow supports the dplyr verbs mutate(), transmute(), select(), rename(), relocate(), filter(), and arrange(). Aggregation is not yet supported, so before you call summarise() or other verbs with aggregate functions, use collect() to pull the selected subset of the data into an in-memory R data frame.
# Suppose you attempt to call unsupported dplyr verbs or unimplemented functions in your query on an Arrow Dataset. In that case, the arrow package raises an error. However, for dplyr queries on Arrow Table objects (which are already in memory), the package automatically calls collect() before processing that dplyr verb.
# Here’s an example: suppose that you are curious about tipping behavior among the longest taxi rides. Let’s find the median tip percentage for rides with fares greater than $100 in 2015, broken down by the number of passengers:


## Focus on One Year

system.time(
  ds %>%
    filter(total_amount > 100, year == 2015) %>%
    select(tip_amount, total_amount, passenger_count) %>%
    mutate(tip_pct = 100 * tip_amount / total_amount) %>%
    group_by(passenger_count) %>%
    collect() %>%
    summarise(median_tip_pct = median(tip_pct),
              n = n()) %>%
    print()
)

## Focus on All Years
system.time(
  ds %>%
    filter(total_amount > 100) %>%
    select(year, tip_amount, total_amount, passenger_count) %>%
    mutate(tip_pct = 100 * tip_amount / total_amount) %>%
    group_by(year, passenger_count) %>%
    collect() %>%
    summarise(median_tip_pct = median(tip_pct),
              n = n()) %>%
    print()
)


# munging -----------------------------------------------------------------


q <- ds %>%
  filter(total_amount > 100, year == 2015) %>%
  select(tip_amount, total_amount, passenger_count) %>%
  mutate(tip_pct = 100 * tip_amount / total_amount) %>%
  group_by(passenger_count)




# writing -----------------------------------------------------------------

### Write the grouped passanger count
write_dataset(q, "Desktop/arrow_tests/sample/sample", format = "feather")

ds %>%
  group_by(payment_type) %>%
  write_dataset("Desktop/arrow_tests/nyc_test", format = "feather") # to_path <- "Desktop/arrow_tests/nyc-taxi"

ds %>%
  group_by(payment_type) %>%
  write_dataset("Desktop/arrow_tests/nyc_test_oq", format = "parquet") # to_path <- "Desktop/arrow_tests/nyc-taxi"

glue::glue()

system("tree Desktop/arrow_tests/nyc_test")



# package_intro -----------------------------------------------------------
## https://arrow.apache.org/blog/2021/11/08/r-6.0.0/
q <- ds %>%
  filter(passenger_count > 0,
         passenger_count < 6,
         grepl("csh", payment_type, ignore.case = TRUE)) %>%
  group_by(passenger_count) %>%
  summarize(avg = mean(total_amount, na.rm = TRUE),
            count = n()) %>%
  arrange(desc(count))

q %>%
  collect()

## Joins

fl <- arrow_table(nycflights13::flights)
al <- arrow_table(nycflights13::airlines)

fl %>%
  filter(year == 2013,
         month == 10,
         day == 9,
         origin == "JFK",
         dest == "LAS") %>%
  select(dep_time, arr_time, carrier) %>%
  left_join(al) %>%
  collect()


# Integration with Duck ---------------------------------------------------

flights_filtered <-
  fl %>%
  select(carrier, origin, dest, arr_delay) %>%
  # arriving early doesn't matter, so call negative delays 0
  mutate(arr_delay = pmax(arr_delay, 0)) %>%
  to_duckdb() %>%
  # for each carrier-origin-dest, take the worst 5% of delays
  group_by(carrier, origin, dest) %>%
  mutate(arr_delay_rank = percent_rank(arr_delay)) %>%
  filter(arr_delay_rank > 0.95)

flights_filtered


flights_filtered %>%
  to_arrow() %>%
  # now summarise to get mean/min
  group_by(carrier, origin, dest) %>%
  summarise(
    arr_delay_mean = mean(arr_delay),
    arr_delay_min = min(arr_delay),
    num_flights = n()
  ) %>%
  filter(dest %in% c("ORD", "MDW")) %>%
  arrange(desc(arr_delay_mean)) %>%
  collect()



# altrep ------------------------------------------------------------------

library(microbenchmark)
library(arrow)

tbl <-
  arrow_table(data.frame(
    x = rnorm(10000000),
    y = sample(c(letters, NA), 10000000, replace = TRUE)
  ))

with_altrep <- function(data) {
  options(arrow.use_altrep = TRUE)
  as.data.frame(data)
}

without_altrep <- function(data) {
  options(arrow.use_altrep = FALSE)
  as.data.frame(data)
}

microbenchmark(without_altrep(tbl),
               with_altrep(tbl))
