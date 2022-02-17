library(tidyverse)
library(asbtools)
x = "Desktop/data/usa_spending/fpds/1978/1978.gz.parquet"
data <- pq_read(x = x, as_data_frame = F, to_duck = F)
char_var <- c("name_agency_award", "name_office_award", "name_vendor")
amt_var <- "amount_obligation"

data %>%
  tbl_arrow_summarise(
    distinct_variables = char_var,
    amount_variables =  amt_var,
    top_variables = char_var,
    which_max_variables = char_var,
    calculation_variable = amt_var,
    median_variables = amt_var,
    mean_variables = amt_var,
    unique_variables = char_var[[1]],
    min_variables = amt_var,
    variance_variables = amt_var,
    sd_variables = amt_var,
    max_variables = amt_var,
    which_min_variables = char_var,
    count_variable = "count_actions",
    to_arrow_table = T
  )

data %>%
  tbl_arrow_summarise(
    distinct_variables = char_var,
    amount_variables =  amt_var,
    top_variables = char_var,
    which_max_variables = char_var,
    calculation_variable = amt_var,
    median_variables = amt_var,
    mean_variables = amt_var,
    unique_variables = char_var[[1]],
    min_variables = amt_var,
    variance_variables = amt_var,
    sd_variables = amt_var,
    max_variables = amt_var,
    which_min_variables = char_var,
    count_variable = "count_actions",
    to_arrow_table = F
  )

data %>%
  tbl_arrow_summarise(
    widen_variable = "type_action",
    distinct_variables = char_var,
    amount_variables =  amt_var,
    top_variables = char_var,
    which_max_variables = char_var,
    calculation_variable = amt_var,
    median_variables = amt_var,
    mean_variables = amt_var,
    unique_variables = char_var[[1]],
    min_variables = amt_var,
    variance_variables = amt_var,
    sd_variables = amt_var,
    max_variables = amt_var,
    which_min_variables = char_var,
    count_variable = "count_actions",
    to_arrow_table = F
  )


## grouped

d <- data %>%
  tbl_arrow_summarise(
    group_variables = "code_product_service",
    distinct_variables = char_var,
    amount_variables =  amt_var,
    top_variables = char_var,
    which_max_variables = char_var,
    calculation_variable = amt_var,
    median_variables = amt_var,
    mean_variables = amt_var,
    unique_variables = char_var[[1]],
    min_variables = amt_var,
    variance_variables = amt_var,
    sd_variables = amt_var,
    max_variables = amt_var,
    which_min_variables = char_var,
    count_variable = "count_actions"
  )

d %>%
  asbviz::tbl_fct_lump(variable = "name_agency_award_which_max",
               weight = "amount_obligation_total",
               n_unique = 5) %>%
  asbviz::hc_xy(
    x = "amount_obligation_mean",
    y = "amount_obligation_total",
    name = "code_product_service",
    transformations = c("log_x", "log_y"),
    group = "name_agency_award_which_max_lumped"
  )


data %>%
  tbl_arrow_summarise(
    group_variables = "code_product_service",
    widen_variable = "type_action",
    distinct_variables = char_var,
    amount_variables =  amt_var,
    top_variables = char_var,
    which_max_variables = char_var,
    calculation_variable = amt_var,
    median_variables = amt_var,
    mean_variables = amt_var,
    unique_variables = char_var[[1]],
    min_variables = amt_var,
    variance_variables = amt_var,
    sd_variables = amt_var,
    max_variables = amt_var,
    which_min_variables = char_var,
    count_variable = "count_actions"
  )
