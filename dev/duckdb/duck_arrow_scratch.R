setwd("~")
library(duckdb)
con <-
  db_duck_connect(db_path = "Desktop/databases/duck_db/", file_name = "contract_solicitations")
idx <-
  create_db_variable_indicies(
    table_name = "contract_solicitations",
    variables = c(
      "id_notice, name_command_sub",
      "year_data",
      "name_product_service"
    ),
    is_unique = F
  )


df <-
  pq_read(x  = "Desktop/data/usa_spending/contract_archives/solicitations/2019.gz.parquet", as_data_frame = T)

df1 <-
  pq_read(x  = "Desktop/data/usa_spending/contract_archives/solicitations/2020.gz.parquet", as_data_frame = T)

db_duck_write_table(
  con = con,
  data = df,
  table_name = "contract_solicitations",
  db_indicies = idx
)
db_duck_write_table(con = con,
                    data = df1,
                    table_name = "contract_solicitations")

## https://duckdb.org/docs/sql/functions/patternmatching
dbGetQuery(con, "SELECT * FROM contract_solicitations where name_command_sub like '%ARMY'") %>% as_tibble()
dbGetQuery(con, "SELECT * FROM contract_solicitations where name_command_sub like '%ARMY' OR name_solicitation like '%ARMY'") %>% as_tibble()

dbGetQuery(con, "SELECT * FROM contract_solicitations where name_command_sub like 'DEPARTMENT OF THE ARMY'") %>% as_tibble()
dbGetQuery(con, "SELECT * FROM contract_solicitations where  regexp_matches(name_command_sub, 'ARMY|NAVY')") %>% as_tibble()
dbGetQuery(con, "SELECT * FROM contract_solicitations where  regexp_matches(name_solicitation, 'DRONE')") %>% as_tibble()


con %>% tbl("contract_solicitations") %>%
  group_by(name_command_sub) %>%
  summarise(
    distinct_years = n_distinct(year_data),
    count = n(),
    distinct_psc = n_distinct(name_product_service),
    first_year = min(year_data)
  ) %>%
  collect()



db_duck_disconnect(con = con)

con <-
  db_duck_connect(db_path = "Desktop/databases/duck_db/", file_name = "contract_solicitations")

con %>% tbl("contract_solicitations") %>%
  group_by(name_command_sub) %>%
  summarise(
    distinct_years = n_distinct(year_data),
    count = n(),
    distinct_psc = n_distinct(name_product_service),
    first_year = min(year_data)
  ) %>%
  collect()

delete_item(path = "Desktop/databases/duck_db/")
