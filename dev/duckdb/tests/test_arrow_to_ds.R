library(duckdb)
library(asbtools)

arrow_dataset_to_duck_db(
  duck_path = "Desktop/databases/govtrackr/",
  table_name = "contract_solicitations",
  arrow_source = "Desktop/data/usa_spending/contract_archives/solicitations/",
  arrow_schema_file = "Desktop/data/usa_spending/contract_archives/solicitations/2020.gz.parquet",
  index_variables = "id_notice"
)

con <- asbtools::db_duck_connect(db_path = "Desktop/databases/govtrackr/", file_name = "contract_solicitations")
tbl_features <- con %>% db_table_features(table_name = "contract_solicitations")

tictoc::tic()
df <-
  con %>% db_filter_regexp(
    table_name = "contract_solicitations",
    features = c(
      "description_solicitation",
      "description_award",
      "name_solicitation"
    ),
    variables = "DRONE"
  )
tictoc::toc()

tictoc::tic()
ds <-
  arrow_open_data(sources  = "Desktop/data/usa_spending/contract_archives/solicitations/",
                  schema_file = "Desktop/data/usa_spending/contract_archives/solicitations/2020.gz.parquet")
ds <- ds %>%
  filter(
    description_solicitation %>% str_detect("DRONE") |
      description_award %>% str_detect("DRONE") |
      name_solicitation %>% str_detect("DRONE"),
  )
ds <- ds %>% collect()
tictoc::toc()
