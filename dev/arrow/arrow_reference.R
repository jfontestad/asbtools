library(arrow)
library(tidyverse)


# to_duck -----------------------------------------------------------------

ds <- InMemoryDataset$create(mtcars)

names(ds)
pryr::object_size(ds)
pryr::object_size(mtcars)

ds %>%
  filter(mpg < 30) %>%
  to_duckdb() %>%
  group_by(cyl) %>%
  summarize(mean_mpg = mean(mpg, na.rm = TRUE))


# to_arrow ----------------------------------------------------------------

ds %>%
  filter(mpg < 30) %>%
  to_duckdb() %>%
  group_by(cyl) %>%
  summarize(mean_mpg = mean(mpg, na.rm = TRUE)) %>%
  to_arrow() %>%
  collect()


# schema ------------------------------------------------------------------


df <- tibble(col1 = 2:4, col2 = c(0.1, 0.3, 0.5))
df
tab1 <- arrow_table(df)
tab1
tab1 %>% collect()

tab1$schema


tab2 <- arrow_table(df, schema = schema(col1 = int8(), col2 = float32()))
tab2$schema


# open_dataset ------------------------------------------------------------

# Set up directory for examples
tf <- tempfile()
dir.create(tf)
on.exit(unlink(tf))

data <- dplyr::group_by(mtcars, cyl)
write_dataset(data, tf)

# You can specify a directory containing the files for your dataset and
# open_dataset will scan all files in your directory.
open_dataset(tf)


# You can also supply a vector of paths
open_dataset(c(
  file.path(tf, "cyl=4/part-0.parquet"),
  file.path(tf, "cyl=8/part-0.parquet")
))

open_dataset(c(
  file.path(tf, "cyl=4/part-0.parquet"),
  file.path(tf, "cyl=8/part-0.parquet")
)) %>%
  collect()



## You must specify the file format if using a format other than parquet.
tf2 <- tempfile()
dir.create(tf2)
on.exit(unlink(tf2))
write_dataset(data, tf2, format = "ipc")

# This line will results in errors when you try to work with the data
if (FALSE) {
  open_dataset(tf2)
}
# This line will work
open_dataset(tf2, format = "ipc")


#>
tf3 <- tempfile()
dir.create(tf3)
on.exit(unlink(tf3))
write_dataset(airquality, tf3, partitioning = c("Month", "Day"), hive_style = FALSE)

# View files - you can see the partitioning means that files have been written
# to folders based on Month/Day values

tf3_files <- list.files(tf3, recursive = TRUE)
tf3_files

# With no partitioning specified, dataset contains all files but doesn't include
# directory names as field names
open_dataset(tf3)
#> FileSystemDataset with 153 Parquet files
#> Ozone: int32
#> Solar.R: int32
#> Wind: double
#> Temp: int32
#>
#> See $metadata for additional Schema metadata

# Now that partitioning has been specified, your dataset contains columns for Month and Day
open_dataset(tf3, partitioning = c("Month", "Day"))



# If you want to specify the data types for your fields, you can pass in a Schema
open_dataset(tf3, partitioning = schema(Month = int8(), Day = int8()))



# arrow_table -------------------------------------------------------------


tbl <- arrow_table(name = rownames(mtcars), mtcars)
dim(tbl)
dim(head(tbl))
names(tbl)
tbl$mpg
tbl[["cyl"]]
as.data.frame(tbl[4:8, c("gear", "hp", "wt")])

tbl %>%
  filter(cyl == 3)


# schema ------------------------------------------------------------------

df <- tibble(col1 = 2:4, col2 = c(0.1, 0.3, 0.5))
df
tab1 <- arrow_table(df)
tab1$schema
pryr::object_size(tab1)
tab2 <-
  arrow_table(df, schema = schema(col1 = int8(), col2 = float32()))
tab2$schema
pryr::object_size(tab2)
tab2$schema$


# arrow-datatype ----------------------------------------------------------

bool()
struct(a = int32(), b = double())
timestamp("ms", timezone = "CEST")
time64("ns")


# dictionary --------------------------------------------------------------


## TBD


# field -------------------------------------------------------------------

field("x", int32())


# FileFormat {arrow}	 -----------------------------------------------------


## Semi-colon delimited files
# Set up directory for examples
tf <- tempfile()
dir.create(tf)
on.exit(unlink(tf))
write.table(mtcars,
            file.path(tf, "file1.txt"),
            sep = ";",
            row.names = FALSE)

# Create FileFormat object
format <- FileFormat$create(format = "text", delimiter = ";")

con <- open_dataset(tf, format = format)
con %>% collect()
con %>%
  group_by(cyl) %>%
  summarise(n = n()) %>%
  collect()
asbtools::arrow_open_data(tf, format = format) %>% collect()

open_dataset(tf, format = format)


# array -------------------------------------------------------------------


my_array <- Array$create(1:10)
my_array$type
my_array$cast(int8())

# Check if value is null; zero-indexed
na_array <- Array$create(c(1:5, NA))
na_array$IsNull(0)
na_array$IsNull(5)
na_array$IsValid(5)
na_array$null_count

# zero-copy slicing; the offset of the new Array will be the same as the index passed to $Slice
new_array <- na_array$Slice(5)
new_array$offset

# Compare 2 arrays
na_array2 <- na_array
na_array2 == na_array # element-wise comparison
na_array2$Equals(na_array) # overall comparison


# match-arrow -------------------------------------------------------------


# note that the returned value is 0-indexed
cars_tbl <- arrow_table(name = rownames(mtcars), mtcars)
cars_tbl
match_arrow(Scalar$create("Mazda RX4 Wag"), cars_tbl$name)
cars_tbl %>% filter(name == "Mazda RX4 Wag") %>% collect()
cars_tbl %>% filter(name %in% "Mazda RX4 Wag") %>% collect()
cars_tbl %>% filter(name %>% str_detect("Mazda RX4 Wag")) %>% collect()

is_in(Array$create("Mazda RX4 Wag"), cars_tbl$name)

# Although there are multiple matches, you are returned the index of the first
# match, as with the base R equivalent
match(4, mtcars$cyl) # 1-indexed

match_arrow(Scalar$create(4), cars_tbl$cyl) # 0-indexed

# If `x` contains multiple values, you are returned the indices of the first
# match for each value.
match(c(4, 6, 8), mtcars$cyl)
cars_tbl %>% filter(cyl %in% c(4,6,8)) %>% count(cyl) %>% collect()
match_arrow(Array$create(c(4, 6, 8)), cars_tbl$cyl)

# Return type matches type of `x`
is_in(c(4, 6, 8), mtcars$cyl) # returns vector
is_in(Scalar$create(4), mtcars$cyl) # returns Scalar
is_in(Array$create(c(4, 6, 8)), cars_tbl$cyl) # returns Array
is_in(ChunkedArray$create(c(4, 6), 8), cars_tbl$cyl) # returns ChunkedArray


# record_batch ------------------------------------------------------------


batch <- record_batch(name = rownames(mtcars), mtcars)
dim(batch)
dim(head(batch))
names(batch)
batch$mpg
batch[["cyl"]]
as.data.frame(batch[4:8, c("gear", "hp", "wt")])


# arrow_open_asb_test -----------------------------------------------------

asbtools::arrow_open_data(sources  = "Desktop/abresler.github.io/r_packages/govtrackR/data/thousand_talents.tsv.gz", format = "csv") %>% count(nameSponsor, sort =  T) %>% collect()
arrow_open_data(sources  = "Desktop/abresler.github.io/r_packages/govtrackR/data/thousand_talents.tsv.gz", format = "csv", to_duck = T)

# to_ducl -----------------------------------------------------------------


library(dplyr)

ds <- InMemoryDataset$create(mtcars)

to_arrow_table()
ds %>%
  filter(mpg < 30) %>%
  to_duckdb() %>%
  group_by(cyl) %>%
  summarize(mean_mpg = mean(mpg, na.rm = TRUE))


# unify_schema ------------------------------------------------------------



a <- schema(b = double(), c = bool())
z <- schema(b = double(), k = utf8())
a
z
unify_schemas(a, z)


