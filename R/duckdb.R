





#' Create Database index statement
#'
#' @param table_name a table name
#' @param index_name if not `NULL` an index name
#' @param variable variable name
#'
#' @return
#' @export
#'
#' @examples
create_db_index <-
  function(table_name = NULL,
           data = NULL,
           index_name = NULL,
           is_unique = F,
           variable = NULL) {
    if (length(table_name) == 0) {
      "Requires table name" %>% message()
      return(invisible())
    }

    if (length(variable) == 0) {
      "Requires variable name" %>% message()
      return(invisible())
    }

    if (length(index_name) == 0) {
      index_name <- variable
    }

    if (length(data) > 0) {
      has_var <- data %>% hasName(variable)
      if (!has_var) {
        glue("Variable {variable} does not exist in the data") %>% message()
        return(invisible())
      }
    }
    uq_slug <- case_when(is_unique ~ " UNIQUE ",
                         TRUE ~ " ")
    index_name_clean <- index_name %>% str_replace_all("\\, ", "_")
    idx <-
      glue("CREATE{uq_slug}INDEX {index_name_clean} ON {table_name} ({variable});") %>% as.character()

    idx
  }

#' Create Variables Indices
#'
#' @param table_name a table name
#' @param variables vector of variables
#' @param data if not `NULL` a `tibble` to validate against
#'
#' @return
#' @export
#'
#' @examples
create_db_variable_indicies <-
  function(table_name = NULL,
           variables = NULL,
           data = NULL,
           is_unique = F) {
    variables %>%
      map_chr(function(x) {
        create_db_index(
          table_name = table_name,
          data = data,
          index_name = NULL,
          variable = x,
          is_unique = is_unique
        )
      })
  }

#' Create a SQL Like Statement
#'
#' @param table_name
#' @param features
#' @param variables
#' @param variable_case
#' @param select_sql
#'
#' @return
#' @export
#'
#' @examples
create_sql_like <-
  function(table_name = NULL,
           features =  NULL,
           variables = NULL,
           variable_case = "upper",
           select_sql = "*") {
    if (length(table_name) == 0) {
      stop("Enter table name")
    }

    if (length(features) == 0) {
      stop("Enter column features")
    }

    if (length(variables) == 0) {
      stop("Enter column features")
    }

    base_sql <-
      glue("SELECT {select_sql} FROM {table_name} where ") %>% as.character()

    features_sql <-
      variables %>%
      map(function(variable) {
        1:length(features) %>%
          map_chr(function(x) {
            if (length(variable_case) > 0) {
              variable <- case_when(
                str_to_lower(variable_case) %>% str_detect("lower") ~ str_to_lower(variable),
                str_to_lower(variable_case) %>% str_detect("upper") ~ str_to_upper(variable),
                TRUE ~ variable
              )
              feature_name <- features[[x]]
              glue("{feature_name} like '%{variable}'") %>% as.character()
            }

          })
      }) %>%
      flatten_chr() %>%
      str_c(collapse = " OR ")


    statement <-
      glue("{base_sql}{features_sql}") %>%
      as.character()

    statement
  }

#' Create a REGEX Match SQL Statement
#'
#' @param table_name
#' @param features
#' @param variables
#' @param variable_case
#' @param select_sql
#'
#' @return
#' @export
#'
#' @examples
create_sql_regexp_matches <-
  function(table_name = NULL,
           features =  NULL,
           variables = NULL,
           variable_case = "upper",
           select_sql = "*") {
    if (length(table_name) == 0) {
      stop("Enter table name")
    }

    if (length(features) == 0) {
      stop("Enter column features")
    }

    if (length(variables) == 0) {
      stop("Enter column features")
    }

    base_sql <-
      glue("SELECT {select_sql} FROM {table_name} where ") %>% as.character()

    if (length(variable_case) > 0) {
      variables <- case_when(
        str_to_lower(variable_case) %>% str_detect("lower") ~ str_to_lower(variables),
        str_to_lower(variable_case) %>% str_detect("upper") ~ str_to_upper(variables),
        TRUE ~ variables
      )
    }

    variables_slug <-
      str_c(variables, collapse = "|")

    features_sql <-
      features %>%
      map_chr(function(x) {
        glue("regexp_matches({as_name(x)}, '{variables_slug}')") %>% as.character()
      }) %>%
      str_c(collapse = " OR ")


    statement <-
      glue("{base_sql}{features_sql}") %>%
      as.character()

    statement
  }



#' Execute a SQL Like Statement
#'
#' Create and execute a SQL like statementxw
#'
#' @param con
#' @param table_name
#' @param features
#' @param variables
#' @param variable_case
#' @param return_query
#' @param to_arrow_table
#'
#' @return
#' @export
#'
#' @examples
db_filter_like <-
  function(con = NULL,
           table_name = NULL,
           select_sql = "*",
           features =  NULL,
           variables = NULL,
           variable_case = "upper",
           return_query = T,
           to_arrow_table = F) {
    if (length(con) == 0) {
      "Enter database connections" %>% message()
      return(invisible())
    }

    tbl_features <-
      con %>% db_table_features(table_name = table_name)

    tbl_features <- tbl_features %>% filter(feature %in% features)
    if (nrow(tbl_features) == 0) {
      "No Matched features" %>% message()
      return(invisible())
    }

    statement <-
      create_sql_like(
        table_name = table_name,
        features = tbl_features$feature,
        variables = variables,
        variable_case = variable_case,
        select_sql = select_sql
      )

    if (return_query) {
      statement %>% print()
    }

    data <-
      dbGetQuery(con, statement = statement) %>%
      as_tibble()

    if (to_arrow_table) {
      data <- tbl_arrow(data = data)
    }

    data

  }

#' Execute regexp filter search on a database
#'
#' @param con
#' @param table_name
#' @param select_sql
#' @param features
#' @param variables
#' @param variable_case
#' @param return_query
#' @param to_arrow_table
#'
#' @return
#' @export
#'
#' @examples
db_filter_regexp <-
  function(con = NULL,
           table_name = NULL,
           select_sql = "*",
           features =  NULL,
           variables = NULL,
           variable_case = "upper",
           return_query = T,
           to_arrow_table = F) {
    if (length(con) == 0) {
      "Enter database connections" %>% message()
      return(invisible())
    }

    tbl_features <-
      con %>% db_table_features(table_name = table_name)

    tbl_features <- tbl_features %>% filter(feature %in% features)
    if (nrow(tbl_features) == 0) {
      "No Matched features" %>% message()
      return(invisible())
    }

    statement <-
      create_sql_regexp_matches(
        table_name = table_name,
        features = tbl_features$feature,
        variables = variables,
        variable_case = variable_case,
        select_sql = select_sql
      )

    if (return_query) {
      statement %>% print()
    }

    data <-
      dbGetQuery(con, statement = statement) %>%
      as_tibble()

    if (to_arrow_table) {
      data <- tbl_arrow(data = data)
    }

    data

  }

#' Duck Database Features
#'
#' @param con
#' @param table_name
#'
#' @return
#' @export
#'
#' @examples
#' db_table_features(con = con, table_name = "contract_solicitations")
db_table_features <-
  function(con, table_name) {
    feature <- con %>% dbListFields(name = table_name)
    tibble(feature) %>%
      mutate(number_column = 1:n()) %>%
      select(number_column, everything())
  }

# other -------------------------------------------------------------------

#' Add Duck DB Indicies
#'
#' @param con
#' @param db_indicies
#'
#' @return
#' @export
#'
#' @examples
db_duck_add_indicies <-
  function(con = NULL, db_indicies) {
    if (length(con) == 0) {
      return(invisible())
    }

    db_indicies %>%
      walk(function(x) {
        x %>% message()
        dbSendQuery(conn = con, statement = x)
      })
  }

# duck --------------------------------------------------------------------


#' Create a DuckDB connection
#'
#' @param db_path if not `NULL` path to database
#' @param read_only Set to TRUE for read-only operation
#' @param config Named list with DuckDB configuration flag
#'
#' @return
#' @export
#'
#' @examples
db_duck_connect <-
  function(db_path = NULL,
           file_name = NULL,
           read_only = FALSE,
           config = list()) {
    if (length(db_path) == 0) {
      con <- dbConnect(duckdb(read_only = read_only, config = config))
    }

    if (length(db_path) > 0) {
      if (length(file_name) == 0) {
        stop("Enter duck db name")
      }
      full_path <- glue("{db_path}/{file_name}")
      duckdb_file <-
        glue("{full_path}/db.duckdb")
      .build_folder(path = full_path)
      oldwd <- getwd()
      setwd("~")
      con <-
        dbConnect(duckdb(read_only = read_only, config = config), duckdb_file)
      if (getwd() != oldwd) {
        setwd(oldwd)
      }
    }

    con
  }

#' Disconnect from DuckDB
#'
#' @param con a connection object
#' @param shutdown if `TRUE` shutdown server
#'
#' @return
#' @export
#'
#' @examples
db_duck_disconnect <-
  function(con = NULL, shutdown = T) {
    if (length(con) == 0) {
      "Enter connection" %>% message()
      return(invisible())
    }
    dbDisconnect(con, shutdown = shutdown)
    gc()
    invisible()

  }

#' Write table to duck db connection
#'
#' @param con a duckdb connection
#' @param data a table name
#' @param table_name table table
#' @param db_indicies if not `NULL` vector of index statements
#' @param drop_existing_table if `TRUE` drop a table that exists
#' @param append_table if `TRUE` appends table
#' @param overwrite_table if `TRUE` overrites existing table
#' @param row.names if `TRUE` rownames
#'
#' @return
#' @export
#'
#' @examples
#'
#' library(asbtools)
#'
db_duck_write_table <-
  function(con = NULL,
           data = NULL,
           table_name = NULL,
           db_indicies = NULL,
           drop_existing_table = F,
           append_table = TRUE,
           overwrite_table = FALSE,
           row.names = FALSE) {
    if (length(con) == 0) {
      "Enter duck connection" %>% message()
      return(invisible())
    }

    if (length(data) == 0) {
      "Enter data" %>% message()
      return(invisible())
    }

    if (length(table_name) == 0) {
      "Enter table name" %>% message()
      return(invisible())
    }

    has_table <- con %>% dbExistsTable(name = table_name)

    if (has_table) {
      glue("{table_name} already exists") %>% message()
    }

    if (has_table & drop_existing_table) {
      glue("Dropping {table_name}") %>% message()
      con %>% dbRemoveTable(name = table_name)
      glue("Recreating {table_name}") %>% message()
      dbCreateTable(con, name = table_name, fields = data)
    }

    if (!has_table) {
      glue("Creating {table_name}") %>% message()
      dbCreateTable(con, name = table_name, fields = data)
    }

    if (length(db_indicies) != 0) {
      db_indicies %>%
        walk(function(x) {
          x %>% message()
          dbSendQuery(conn = con, statement = x)
        })
    }

    count_rows <- nrow(data)

    glue("Writing {count_rows} rows to {table_name} duckdb table") %>% message()

    dbWriteTable(
      conn = con,
      name = table_name,
      value = data,
      append = append_table,
      overwrite = overwrite_table,
      row.names = row.names
    )

    return(invisible())

  }


# to_duck_db --------------------------------------------------------------


#' Create a DuckDB from an Arrow Data Source
#'
#' Turns an arrow data source into a duckdb.
#'
#' Check out \href{https://duckdb.org/docs/}{duckdb documentation}
#'
#' @param duck_path
#' @param table_name
#' @param disconnect
#' @param index_variables
#' @param is_unique_index
#' @param read_only
#' @param config
#' @param arrow_source
#' @param arrow_schema_file
#' @param partitioning
#' @param unify_schemas
#' @param ...
#'
#' @return
#' @export
#'
#' @examples
arrow_dataset_to_duck_db <-
  function(duck_path = NULL,
           table_name = NULL,
           disconnect = T,
           index_variables =  NULL,
           is_unique_index = F,
           read_only = FALSE,
           config = list(),
           arrow_source =  NULL,
           arrow_schema_file = NULL,
           partitioning = NULL,
           unify_schemas = NULL,
           ...) {
    if (length(duck_path) == 0) {
      stop("Enter duck path name")
    }

    if (length(table_name) == 0) {
      stop("Enter table name")
    }

    if (length(arrow_source) == 0) {
      stop("Enter arrow source features")
    }
    oldwd <- getwd()
    setwd("~")

    duck_exists <- dir.exists(paths = glue("{duck_path}{table_name}"))

    if (duck_exists) {
      delete_item(path = glue("{duck_path}{table_name}"))
    }

    if (getwd() != oldwd) {
      setwd(oldwd)
    }

    con <- db_duck_connect(
      db_path = duck_path,
      file_name = table_name,
      read_only = read_only,
      config = config

    )


    if (length(index_variables) > 0) {
      idx_vars <-
        create_db_variable_indicies(table_name = table_name, variables = index_variables)
    }

    if (length(index_variables) == 0) {
      idx_vars <- NULL
    }

    if (length(arrow_schema_file) == 0) {
      "Please define an arrow schema file" %>% message()
    }

    type <- arrow_schema_file %>% str_split("\\.") %>% flatten_chr()
    file_type <- type[length(type)]
    data <-
      arrow_open_data(sources = arrow_schema_file) %>% collect()

    glue("Creating {table_name}") %>% message()
    dbCreateTable(con, name = table_name, fields = data)

    if (length(idx_vars) > 0) {
      db_duck_add_indicies(con = con, db_indicies = idx_vars)
    }


    rm(data)
    gc(verbose = T,
       reset = T,
       full = T)

    oldwd
    setwd("~")
    files <- list.files(path = arrow_source, pattern = file_type)
    if (getwd() != oldwd) {
      setwd(oldwd)
    }
    as <- arrow_source %>% str_remove_all("/$")
    files <- glue("{as}/{files}") %>% as.character()

    files[31:length(files)] %>%
      walk(function(x) {
        glue("\n\nWriting {x} to {table_name}\n\n") %>% message()
        data <-
          arrow_open_data(
            sources = x,
            schema_file = arrow_schema_file,
            to_duck = F,
            assign_schema = F,
            partitioning = partitioning,
            format = file_type
          )
        select_cols <- con %>% dbListFields(name = table_name)
        data <- data %>% select(one_of(select_cols)) %>% collect()
        count_rows <- nrow(data)
        glue("\n\nWriting {count_rows} rows to {table_name} duckdb table\n\n") %>% message()

        dbWriteTable(
          conn = con,
          name = table_name,
          value = data,
          append = T,
          overwrite = F
        )

        rm(data)
        gc(verbose = T,
           reset = T,
           full = T)
        return(invisible())
      })

    glue("Wrote {table_name} to duckb") %>% message()
    if (disconnect) {
      con %>% dbDisconnect()
    }
    return(invisible())
  }
