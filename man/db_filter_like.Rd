% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/duckdb.R
\name{db_filter_like}
\alias{db_filter_like}
\title{Execute a SQL Like Statement}
\usage{
db_filter_like(
  con = NULL,
  table_name = NULL,
  select_sql = "*",
  features = NULL,
  variables = NULL,
  variable_case = "upper",
  return_query = T,
  to_arrow_table = F
)
}
\arguments{
\item{to_arrow_table}{}
}
\value{

}
\description{
Create and execute a SQL like statementxw
}
