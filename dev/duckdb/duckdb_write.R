library(duckdb)
library(asbtools)
library(DBI)
library(tidyverse)

con <-
  db_duck_connect(db_path = "Desktop/databases/duck_db/duck_test", file_name = "test")
mt_indicies <- create_db_variable_indicies(
  table_name = "mtcars",
  variables = c("cyl", "am", "gear", "carb"),
  data = mtcars
)

db_duck_write_table(
  con = con,
  data = mtcars,
  table_name = "mtcars",
  drop_existing_table = T,
  db_indicies =  mt_indicies,
  append_table = T,
  overwrite_table = F,
  row.names = F
)

con %>% tbl("mtcars") %>% distinct(cyl)

diamond_indicies <-
  create_db_variable_indicies(table_name = "diamonds",
                              variables = c("cut", "clarity", "color"))

db_duck_write_table(
  con = con,
  data = ggplot2::diamonds,
  table_name = "diamonds",
  drop_existing_table = T,
  db_indicies =  diamond_indicies,
  append_table = T,
  overwrite_table = F,
  row.names = F
)


# text --------------------------------------------------------------------

tbl_text_data <-
  tibble(
    document_identifier = c('doc1', 'doc2'),
    text_content = c(
      'The mallard is a dabbling duck that breeds throughout the temperate.',
      'The cat is a domestic species of small carnivorous mammal.'
    ),
    author = c('Hannes Mühleisen', 'Laurens Kuiper'),
    doc_version = c(3, 2)
  )

db_duck_write_table(
  con = con,
  data = tbl_text_data,
  table_name = "documents",
  drop_existing_table = T,
  db_indicies =  NULL,
  append_table = T,
  overwrite_table = F,
  row.names = F
)

# con %>%
#   dbSendQuery(statement = "PRAGMA create_fts_index('documents', 'document_identifier', 'text_content', 'author');")

con %>% db_duck_disconnect(shutdown = T)
gc() # clear the memory
rm(con)
con <-
  db_duck_connect(db_path = "Desktop/databases/duck_db/duck_test", file_name = "test")

con %>% tbl("mtcars")

db_diamonds <- con %>% tbl("diamonds")

sample_query <-
  db_diamonds %>%
  group_by(carat, cut, color) %>%
  summarise(
    mean_price = mean(price, na.rm = T),
    distinct_clarity = n_distinct(clarity),
    count = n()
  ) %>%
  ungroup()

sample_query %>%
  show_query()

sample_query %>% collect()


con %>% db_duck_disconnect(shutdown = T)

asbviz::tbl_summarise()
