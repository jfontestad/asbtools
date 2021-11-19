
# utils -------------------------------------------------------------------
.top_group <-
  function(data,
           group_variables = "cut",
           top_variable = "color",
           calculation_variable = "price",
           filters = c("UNNAMED", "UNKNOWN"),
           top = 1,
           remove_top_amount = T) {
    new_var_name <- glue("{top_variable}_top") %>% as.character()
    amount_var <-
      glue("{calculation_variable}_{top_variable}_top") %>% as.character()

    if (length(filters) > 0) {
      filter_slugs <- str_c(filters, collapse = "|")
      data <- data %>%
        filter(!(!!sym(top_variable) %>% str_detect(filter_slugs)))
    }

    if (length(group_variables) > 0) {
      data <-
        data %>%
        filter(!is.na((!!sym(top_variable)))) %>%
        group_by(!!!syms(c(group_variables, top_variable))) %>%
        summarise(UQ(amount_var) := sum(!!sym(calculation_variable), na.rm = T)) %>%
        collect() %>%
        ungroup() %>%
        group_by(!!!syms(group_variables)) %>%
        slice(1:top) %>%
        ungroup() %>%
        rename(UQ(new_var_name) := top_variable)
    }

    if (length(group_variables) == 0) {
      data <-
        data %>%
        filter(!is.na((!!sym(top_variable)))) %>%
        group_by(!!!sym(top_variable)) %>%
        summarise(UQ(amount_var) := sum(!!sym(calculation_variable), na.rm = T)) %>%
        collect() %>%
        ungroup() %>%
        slice(1:top) %>%
        ungroup() %>%
        rename(UQ(new_var_name) := top_variable)
    }



    if (remove_top_amount) {
      data <- data %>%
        select(-one_of(amount_var))
    }
    data
  }


#' Top Arrow Groups
#'
#' @param data
#' @param group_variables
#' @param top_variables
#' @param calculation_variable
#' @param filters
#' @param top
#' @param remove_top_amount
#'
#' @return
#' @export
#'
#' @examples
tbl_arrow_top_n_groups <-
  function(data,
           group_variables = NULL,
           top_variables = NULL,
           calculation_variable = NULL,
           filters = NULL,
           top = 1,
           remove_top_amount = T) {

    if (length(calculation_variable) == 0) {
      "Enter calculation variable" %>% message()
      return(data)
    }

    if (length(top_variables) == 0) {
      "Enter top variables" %>% message()
      return(data)
    }
    all_data <-
      top_variables %>%
      map(function(x) {
        .top_group(
          data = data,
          group_variables = group_variables,
          top_variable = x,
          calculation_variable = calculation_variable,
          filters = filters,
          top = top,
          remove_top_amount = remove_top_amount
        )
      })

    if (length(group_variables) == 0) {
      all_data <-
        all_data %>% reduce(bind_cols)
    }

    if (length(group_variables) > 0) {
      all_data <-
        all_data %>% reduce(left_join, by  = group_variables)
    }


    all_data
  }

#' Summarise Arrow Table
#'
#' @param data
#' @param group_variables
#' @param widen_variable
#' @param count_variable
#' @param distinct_variables
#' @param amount_variables
#' @param mean_variables
#' @param top_variables
#' @param calculation_variable
#' @param median_variables
#' @param min_variables
#' @param max_variables
#' @param which_max_variables
#' @param which_min_variables
#' @param unique_variables
#' @param unique_concatenator
#' @param first_variables
#' @param last_variables
#' @param variance_variables
#' @param sd_variables
#' @param coalesce_numeric
#' @param remove_top_amount
#' @param filters
#' @param ...
#' @param to_arrow_table
#'
#' @return
#' @export
#'
#' @example inst/arrow_examples/arrow_summarise.R

tbl_arrow_summarise <-
  function(data,
           group_variables = NULL,
           widen_variable = NULL,
           count_variable = "count",
           distinct_variables = NULL,
           amount_variables = NULL,
           mean_variables = NULL,
           top_variables = NULL,
           calculation_variable = NULL,
           median_variables = NULL,
           min_variables = NULL,
           max_variables = NULL,
           which_max_variables = NULL,
           which_min_variables = NULL,
           unique_variables = NULL,
           unique_concatenator = " | ",
           first_variables = NULL,
           last_variables = NULL,
           variance_variables = NULL,
           sd_variables = NULL,
           coalesce_numeric = T,
           remove_top_amount = T,
           filters =  NULL,
           to_arrow_table = F,
           ...) {

    is_arrow <- class(data) %in% c("Table", "ArrowTabular", "ArrowObject") %>% sum(na.rm = T) >= 1

    if (!is_arrow) {
      "Not arrow type" %>% message()
      return(data)
    }

    across_length <-
      length(distinct_variables) + length(amount_variables) + length(mean_variables) + length(median_variables) +
      length(min_variables) + length(max_variables) + length(first_variables) + length(last_variables) + length(variance_variables) + length(sd_variables) +
      length(unique_variables)

    has_across <-  across_length > 0

    if (!has_across) {
      return(data)
    }

    if (across_length + length(count_variable) == 0) {
      "No summary variables" %>% message()
      return(data)
    }

    if (length(group_variables) > 0) {
      group_slugs <- c(group_variables, widen_variable) %>% unique()
      data <- data %>%
        group_by(!!!syms(group_slugs))
    }

    if (length(group_variables) == 0 &
        length(widen_variable) > 0) {
      group_slugs <- c(widen_variable) %>% unique()
      data <-
        data %>%
        group_by(!!!syms(group_slugs))
    }

    if (length(group_variables) == 0 && length(widen_variable) == 0) {
      group_slugs <- NULL
    }

    if (length(count_variable) > 0) {
      all_data <-
        data %>%
        summarise(UQ(count_variable) := n(),
                  .groups = "drop") %>%
        collect()
    }

    if (length(amount_variables) > 0) {
      amount_variables %>%
        walk(function(var) {
          new_var <-
            glue("{var}_total") %>% as.character()
          df_var <-
            data %>%
            summarise(UQ(new_var) := sum(!!sym(var), na.rm = T)) %>%
            collect() %>%
            ungroup()


          if (length(group_slugs) == 0) {
            all_data <<-
              all_data %>%
              bind_cols(df_var)
          }

          if (length(group_slugs) > 0) {
            all_data <<-
              all_data %>%
              left_join(df_var, by = group_slugs)
          }
        })
    }

    if (length(distinct_variables) > 0) {
      distinct_variables %>%
        walk(function(var) {
          new_var <-
            glue("{var}_distinct") %>% as.character()


          if (length(group_slugs) == 0) {
            distinct_count <-
              data %>%
              select(!!sym(var)) %>%
              filter(!is.na(!!sym(var))) %>%
              distinct() %>%
              collect() %>%
              nrow()

            df_var <-
              tibble(UQ(new_var) := distinct_count)

            all_data <<-
              all_data %>%
              bind_cols(df_var)
          }

          if (length(group_slugs) > 0) {
            df_var <-
              data %>%
              select(!!!syms(c(var, group_slugs))) %>%
              filter(!is.na(!!sym(var))) %>%
              distinct() %>%
              group_by(!!!syms(c(group_slugs))) %>%
              summarise(UQ(new_var) := n_distinct(!!sym(var))) %>%
              collect()

            all_data <<-
              all_data %>%
              left_join(df_var, by = group_slugs)
            }



        })
    }

    if (length(mean_variables) > 0) {
      mean_variables %>%
        walk(function(var) {
          new_var <-
            glue("{var}_mean") %>% as.character()
          df_var <-
            data %>%
            summarise(UQ(new_var) := mean(!!sym(var), na.rm = T)) %>%
            collect() %>%
            ungroup()

          if (length(group_slugs) == 0) {
            all_data <<-
              all_data %>%
              bind_cols(df_var)
          }

          if (length(group_slugs) > 0) {
            all_data <<-
              all_data %>%
              left_join(df_var, by = group_slugs)
          }
        })
    }

    if (length(median_variables) > 0) {
      median_variables %>%
        walk(function(var) {
          new_var <-
            glue("{var}_median") %>% as.character()
          df_var <-
            data %>%
            summarise(UQ(new_var) := median(!!sym(var), na.rm = T)) %>%
            collect() %>%
            ungroup()

          if (length(group_slugs) == 0) {
            all_data <<-
              all_data %>%
              bind_cols(df_var)
          }

          if (length(group_slugs) > 0) {
            all_data <<-
              all_data %>%
              left_join(df_var, by = group_slugs)
          }
        })
    }

    if (length(min_variables) > 0) {
      min_variables %>%
        walk(function(var) {
          new_var <-
            glue("{var}_min") %>% as.character()
          df_var <-
            data %>%
            summarise(UQ(new_var) := min(!!sym(var), na.rm = T)) %>%
            collect() %>%
            ungroup()

          if (length(group_slugs) == 0) {
            all_data <<-
              all_data %>%
              bind_cols(df_var)
          }

          if (length(group_slugs) > 0) {
            all_data <<-
              all_data %>%
              left_join(df_var, by = group_slugs)
          }
        })
    }

    if (length(max_variables) > 0) {
      max_variables %>%
        walk(function(var) {
          new_var <-
            glue("{var}_max") %>% as.character()
          df_var <-
            data %>%
            summarise(UQ(new_var) := max(!!sym(var), na.rm = T)) %>%
            collect() %>%
            ungroup()

          if (length(group_slugs) == 0) {
            all_data <<-
              all_data %>%
              bind_cols(df_var)
          }

          if (length(group_slugs) > 0) {
            all_data <<-
              all_data %>%
              left_join(df_var, by = group_slugs)
          }
        })
    }

    if (length(sd_variables) > 0) {
      sd_variables %>%
        walk(function(var) {
          new_var <-
            glue("{var}_sd") %>% as.character()
          df_var <-
            data %>%
            summarise(UQ(new_var) := sd(!!sym(var), na.rm = T)) %>%
            collect() %>%
            ungroup()

          if (length(group_slugs) == 0) {
            all_data <<-
              all_data %>%
              bind_cols(df_var)
          }

          if (length(group_slugs) > 0) {
            all_data <<-
              all_data %>%
              left_join(df_var, by = group_slugs)
          }
        })
    }

    if (length(variance_variables) > 0) {
      variance_variables %>%
        walk(function(var) {
          new_var <-
            glue("{var}_variance") %>% as.character()
          df_var <-
            data %>%
            summarise(UQ(new_var) := var(!!sym(var), na.rm = T)) %>%
            collect() %>%
            ungroup()

          if (length(group_slugs) == 0) {
            all_data <<-
              all_data %>%
              bind_cols(df_var)
          }

          if (length(group_slugs) > 0) {
            all_data <<-
              all_data %>%
              left_join(df_var, by = group_slugs)
          }
        })
    }

    if (length(unique_variables) > 0) {
      unique_variables %>%
        walk(function(var) {
          new_var <-
            glue("{var}_unique") %>% as.character()


          if (length(group_slugs) == 0) {
            unique_vars <-
              data %>%
              select(!!sym(var)) %>%
              filter(!is.na(!!sym(var))) %>%
              distinct() %>%
              collect() %>%
              ungroup() %>%
              arrange(!!sym(var)) %>%
              pull() %>%
              sort() %>%
              str_c(collapse = unique_concatenator)

            df_var <-
              tibble(UQ(new_var) := unique_vars)


          }

          if (length(group_slugs) > 0) {
            df_var <-
              data %>%
              select(!!!syms(c(var, group_slugs))) %>%
              filter(!is.na(!!sym(var))) %>%
              distinct() %>%
              collect() %>%
              arrange(!!sym(var)) %>%
              summarise(across(
                .cols = all_of(var),
                .fns = ~ {
                  .x %>%
                    str_split("\\|") %>% flatten_chr() %>% str_squish() %>%
                    unique() %>% str_c(collapse = unique_concatenator)
                },
                .names = "{.col}_unique"
              )) %>%
              ungroup()

          }

          if (length(group_slugs) == 0) {
            all_data <<-
              all_data %>%
              bind_cols(df_var)
          }

          if (length(group_slugs) > 0) {
            all_data <<-
              all_data %>%
              left_join(df_var, by = group_slugs)
          }
        })
    }

    if (length(which_max_variables) > 0) {
      which_max_variables %>%
        walk(function(var) {
          new_var <-
            glue("{var}_which_max") %>% as.character()
          df_var <-
            data %>%
            summarise(UQ(new_var) := (!!sym(var))[which.max(!!sym(calculation_variable))]) %>%
            ungroup()

          if (length(group_slugs) == 0) {
            all_data <<-
              all_data %>%
              bind_cols(df_var)
          }

          if (length(group_slugs) > 0) {
            all_data <<-
              all_data %>%
              left_join(df_var, by = group_slugs)
          }
        })
    }

    if (length(which_min_variables) > 0) {
      which_min_variables %>%
        walk(function(var) {
          new_var <-
            glue("{var}_which_min") %>% as.character()
          df_var <-
            data %>%
            summarise(UQ(new_var) := (!!sym(var))[which.min(!!sym(calculation_variable))]) %>%
            ungroup()

          if (length(group_slugs) == 0) {
            all_data <<-
              all_data %>%
              bind_cols(df_var)
          }

          if (length(group_slugs) > 0) {
            all_data <<-
              all_data %>%
              left_join(df_var, by = group_slugs)
          }
        })
    }

    if (length(first_variables) > 0) {
      first_variables %>%
        walk(function(var) {
          new_var <-
            glue("{var}_first") %>% as.character()
          df_var <-
            data %>%
            summarise(UQ(new_var) := first(!!sym(var))) %>%
            collect() %>%
            ungroup()

          if (length(group_slugs) == 0) {
            all_data <<-
              all_data %>%
              bind_cols(df_var)
          }

          if (length(group_slugs) > 0) {
            all_data <<-
              all_data %>%
              left_join(df_var, by = group_slugs)
          }
        })
    }

    if (length(last_variables) > 0) {
      last_variables %>%
        walk(function(var) {
          new_var <-
            glue("{var}_last") %>% as.character()
          df_var <-
            data %>%
            summarise(UQ(new_var) := last(!!sym(var))) %>%
            collect() %>%
            ungroup()

          if (length(group_slugs) == 0) {
            all_data <<-
              all_data %>%
              bind_cols(df_var)
          }

          if (length(group_slugs) > 0) {
            all_data <<-
              all_data %>%
              left_join(df_var, by = group_slugs)
          }
        })
    }




    if (length(top_variables) > 0) {
      df_top <-
        tbl_arrow_top_n_groups(
          data = data,
          group_variables = group_slugs,
          top_variables = top_variables,
          calculation_variable = calculation_variable,
          filters = filters,
          top = 1,
          remove_top_amount = remove_top_amount
        )

      if (length(group_slugs) > 0) {
        all_data <-
          all_data %>%
          left_join(df_top, by = group_slugs)
      }

      if (length(group_slugs) == 0) {
        all_data <-        all_data %>%
          bind_cols(df_top)
      }


    }

    if (length(widen_variable) > 0) {
      wide_vars <-
        all_data %>%
        select(!!!syms(widen_variable)) %>%
        distinct() %>%
        pull()

      join_vars <-
        all_data %>%
        select(one_of(group_slugs)) %>%
        select(-!!sym(widen_variable)) %>%
        names()

      all_data <-
        wide_vars %>%
        map(function(x) {
          glue("Widening {x}") %>% message()
          new_name <- make_clean_names(x)
          df <-
            all_data %>%
            filter(!!sym(widen_variable) == x) %>%
            select(-!!sym(widen_variable))

          append_vars <- names(df)[!names(df) %in% group_slugs]

          names(df)[names(df) %in% append_vars] <-
            names(df)[names(df) %in% append_vars] %>% str_c(new_name, sep = "_")

          df

        }) %>%
        reduce(left_join, by = join_vars)

      if (coalesce_numeric) {
        all_data <- all_data %>%
          mutate_if(is.numeric,  ~ {
            coalesce(.x, 0L)
          })
      }

    }


    if (coalesce_numeric) {
      all_data <- all_data %>%
        mutate_if(is.numeric,  ~ {
          coalesce(.x, 0L)
        })
    }

    all_data <- all_data %>%
      mutate_if(is.character, list(function(x) {
        x %>% coalesce("UNKNOWN")
      }))

    bad_totals <-
      names(all_data) %>% str_detect("_total_total") %>% sum(na.rm = T) > 0

    if (bad_totals) {
      new_var <-
        names(all_data)[names(all_data) %>% str_detect("_total_total$")] %>%
        str_remove_all("_total")
      names(all_data)[names(all_data) %>% str_detect("_total_total$")] <-
        names(all_data)[names(all_data) %>% str_detect("_total_total$")] %>%
        str_remove_all("_total")

      names(all_data)[names(all_data) %>% str_detect(new_var)] <-
        names(all_data)[names(all_data) %>% str_detect(new_var)] %>% str_c("_total")
    }

    if (to_arrow_table) {
      all_data <- arrow::arrow_table(all_data)
    }




    all_data

  }

#' Remove Setof Arrow Columns
#'
#' @param data
#' @param remove_columns
#'
#' @return
#' @export
#'
#' @examples
arrow_remove_columns <-
  function(data, remove_columns = NULL) {

    if (length(remove_columns) == 0) {
      return(data)
    }

    tbl_features <-
      arrow_table_features(data = data) %>%
      filter(feature %in% remove_columns)

    if (nrow(tbl_features) == 0) {
      return(data)
    }

    remove_columns <- tbl_features %>% pull(number_column_python)


    remove_columns %>%
      walk(function(col_no) {
        data <<-
          data$RemoveColumn(i = as.integer(col_no))
      })

    data
  }


#' Snake Case Arrow Names
#'
#' @param data an arrow table
#' @param case case default to `snake`
#'
#' @return
#' @export
#'
#' @examples
snake_arrow_names <-
  function(data, case = "snake") {
    new_cols <- names(data) %>%
      janitor::make_clean_names(case = case)

    data <- data$RenameColumns(value = new_cols)

    data


  }

#' To Arrow
#'
#' @param data
#' @param schema
#' @param snake_names
#' @param to_duck
#' @param return_message
#' @param ...
#' @param assign_schema
#'
#' @return
#' @export
#'
#' @examples
#' library(asbtools)
#' mtcars %>% tbl_arrow()
#'
tbl_arrow <-
  function(data,
           schema = NULL,
           snake_names = F,
           to_duck = F,
           return_message = T,
           assign_schema = T,
           ...) {
    if (return_message) {
      "Coercing to Arrow table" %>% message()
    }

    data  <-
      data %>%
      mutate_if(is.factor, as.character)

    data <-
      data %>%
      arrow::arrow_table(..., schema = schema)

    if (snake_names) {
      data <- snake_arrow_names(data = data)
    }

    if (assign_schema) {
      tbl_features <- arrow_table_features(data = data)
      assign(x = "tbl_arrow_schema", value = tbl_features, envir = .GlobalEnv)
    }

    if (to_duck) {
      "Coercing to duckdb" %>% message()
      data <-
        data %>%
        arrow::to_duckdb()
    }

    data


  }

#' Table of Arrow Features
#'
#' @param data
#'
#' @return
#' @export
#'
#' @examples
arrow_table_features <-
  function(data) {

    if (class(data) %in% c("FileSystemDataset") %>% sum(na.rm = T) > 0) {
      tbl_features <-
        tibble(feature = data$schema$ToString() %>% str_split("\\n") %>% flatten_chr()) %>%
        tidyr::separate(
          col = feature,
          into = c("feature", "column_type"),
          sep = "\\:"
        ) %>%
        mutate_if(is.character, str_squish) %>%
        mutate(number_column = 1:n(),
               number_column_python = 1:n() - 1) %>%
        select(number_column, number_column_python, everything())

      return(tbl_features)
    }

    fields <- data$schema$ToString()
    tbl_features <-
      tibble(feature = fields %>% str_split("\\n") %>% flatten_chr()) %>%
      tidyr::separate(
        col = feature,
        into = c("feature", "column_type"),
        sep = "\\:"
      ) %>%
      mutate_if(is.character, str_squish) %>%
      mutate(number_column = 1:n(),
             number_column_python = 1:n()-1) %>%
      select(number_column,number_column_python, everything())

    tbl_features

  }

#' Arrow Compute Functions
#'
#' @return
#' @export
#'
#' @examples
tbl_arrow_compute_functions <-
  function() {
    tibble(name_function = arrow::list_compute_functions()) %>%
      mutate(name_function_r = glue("arrow_{name_function}") %>% as.character()) %>%
      mutate(number_function = 1:n()) %>%
      select(number_function, everything())
  }


# arrow -------------------------------------------------------------------


#' Write Arrow Parquet Files
#'
#' @param data
#' @param file_path
#' @param folder
#' @param file_name
#' @param chunk_size
#' @param version
#' @param use_compression
#' @param compression
#' @param compression_level
#' @param use_dictionary
#' @param write_statistics
#' @param data_page_size
#' @param use_deprecated_int96_timestamps
#' @param coerce_timestamps
#' @param allow_truncated_timestamps
#' @param properties
#' @param arrow_properties
#' @param return_message
#'
#' @return
#' @export
#'
#' @examples
pq_write <-
  function(data = NULL,
           file_path = NULL,
           folder =  NULL,
           file_name = NULL,
           chunk_size = NULL,
           version = NULL,
           use_compression = T,
           compression = default_parquet_compression(),
           compression_level = 9,
           use_dictionary = NULL,
           write_statistics = NULL,
           data_page_size = NULL,
           use_deprecated_int96_timestamps = FALSE,
           coerce_timestamps = NULL,
           allow_truncated_timestamps = FALSE,
           properties = NULL,
           arrow_properties = NULL,
           return_message = T) {
    if (length(data) == 0) {
      "Enter data" %>% message()
      return(invisible())
    }


    if (length(file_path) == 0) {
      "Enter file path" %>% message()
      return(invisible())
    }

    if (length(file_name) == 0) {
      "Enter file name" %>% message()
      return(invisible())
    }

    if (length(folder) > 0) {
      folder_path <-
        glue("{file_path}/{folder}") %>% as.character() %>% str_replace_all("//", "/")
    } else {
      folder_path <-
        glue("{file_path}") %>% as.character() %>% str_replace_all("//", "/")
    }

    .build_folder(path = folder_path)
    oldwd <- getwd()
    setwd(folder_path)

    if (return_message) {
      glue("Saving {file_name} via parquet") %>% message()
    }

    if (use_compression) {
      compression <- "gzip"
      file_slug <-
        glue("{file_name}.gz.parquet") %>% as.character()
    } else {
      file_slug <-
        glue("{file_name}.parquet") %>% as.character()
    }


    write_parquet(
      x = data,
      sink =  file_slug,
      chunk_size = chunk_size,
      version = version,
      compression = compression,
      compression_level = compression_level,
      use_dictionary = use_dictionary,
      write_statistics = write_statistics,
      data_page_size = data_page_size,
      use_deprecated_int96_timestamps = use_deprecated_int96_timestamps,
      coerce_timestamps = coerce_timestamps,
      allow_truncated_timestamps = allow_truncated_timestamps,
      properties = properties,
      arrow_properties = arrow_properties
    )

    if (oldwd != folder_path) {
      setwd(oldwd)
    }

    return(invisible())

  }

#' Write a dataset to file using arrow
#'
#' @param data
#' @param file_path
#' @param format
#' @param partitioning
#' @param base_file_name
#' @param hive_style
#' @param existing_data_behavior
#' @param ...
#'
#' @return
#' @export
#'
#' @examples
#' library(asbtools)
#' library(tidyverse)
#'
#' data <- ggplot2::diamonds %>% tbl_arrow()
#'
#' arrow_write_data_set(data = data, file_path  = "Desktop/arrow_test_write", format = "csv")
#' arrow_write_data_set(data = data, file_path  = "Desktop/arrow_test_write", format = "feather")
#' arrow_write_data_set(data = data, file_path  = "Desktop/arrow_test_write", format = "parquet")
#' arrow_write_data_set(data = as_tibble(data), file_path  = "Desktop/arrow_test_write", format = "parquet")
#'
#'
#'
arrow_write_data_set <-
  function(data,
           file_path = NULL,
           format = c("parquet", "feather", "arrow", "ipc", "csv"),
           partitioning = dplyr::group_vars(data),
           base_file_name = paste0("part-{i}.", as.character(format)),
           hive_style = TRUE,
           existing_data_behavior = c("overwrite", "error", "delete_matching"),
           ...) {

    if (length(file_path) == 0) {
      "No path" %>% message()
      return(data)
    }
    write_dataset(
      dataset = data,
      path = file_path,
      format = format[[1]],
      partitioning = partitioning,
      basename_template = base_file_name[[1]],
      hive_style = hive_style,
      existing_data_behavior = existing_data_behavior
    )
  }


.pq_read_df <-
  function(x,
           snake_names = T,
           remove_columns = NULL,
           to_duck = F,
           properties =  ParquetArrowReaderProperties$create()) {
    oldwd <- getwd()

    setwd("~")

    data <-
      arrow::read_parquet(
        file = x,
        as_data_frame = T,
        props = properties
      )

    if (length(remove_columns) > 0) {
      has_actual_names <- names(data) %in% remove_columns %>% sum(na.rm = T) > 0

      if (has_actual_names) {
        remove_columns <- names(data)[names(data) %in% remove_columns]
        data <- data %>%
          select(-one_of(remove_columns))
      }
    }

    if (snake_names) {
      data <- data %>%
        janitor::clean_names()
    }

    if (to_duck) {
      "Coercing to duckdb" %>% message()
      pos_cols <- data %>% select_if(lubridate::is.POSIXct) %>% names()

      if (length(pos_cols) > 0) {
        data <- data %>%
          mutate_at(pos_cols, as.Date)
      }

      data <-
        data %>%
        arrow::arrow_table() %>%
        arrow::to_duckdb()
    }


    if (oldwd != getwd())  {
      setwd(oldwd)
    }


    data
  }

.pq_read_arrow <-
  function(x,
           to_duck = F,
           snake_names = F,
           remove_columns = NULL,
           assign_schema = T,
           schema_name = NULL,
           properties =  ParquetArrowReaderProperties$create()) {

    oldwd <- getwd()

    setwd("~")

    data <-
      arrow::read_parquet(file = x,
                          as_data_frame = F,
                          props = properties)

    if (length(remove_columns) > 0) {
      data <-
        arrow_remove_columns(data = data, remove_columns = remove_columns)
    }


    if (snake_names) {
      data <-
        snake_arrow_names(data = data)
    }

    if (to_duck) {
      "Coercing to duckdb" %>% message()

      datetime_cols <-
        names(data)[names(data) %>% str_detect("datetime")]

      if (length(datetime_cols) > 0) {
        data <-
          data %>%
          as.data.frame() %>%
          as_tibble() %>%
          mutate_at(datetime_cols, as.Date) %>%
          arrow::arrow_table()
      }

      data <-
        data %>%
        arrow::to_duckdb()
    }

    if (getwd() != oldwd) {
      setwd(oldwd)
    }

    if (assign_schema) {

      if (length(schema_name) == 0) {
        schema_name <- "tbl_arrow_schema"
      }

      if (length(schema_name) > 0) {
        schema_name <-
          glue("tbl_arrow_schema_{schema_name}") %>% as.character()
      }

      tbl_features <- arrow_table_features(data = data)
      assign(x = schema_name, value = tbl_features, envir = .GlobalEnv)
    }

    data
  }

#' Read a parquet files
#'
#' @param x
#' @param file_path
#' @param as_data_frame
#' @param to_duck
#' @param snake_names
#' @param remove_columns
#' @param properties
#' @param assign_schema
#' @param schema_name
#'
#' @return
#' @export
#'
#' @examples
#' library(asbtools)
#' x = "Desktop/data/usa_spending/fpds/1978.gz.parquet"
#' pq_read(x = x, to_duck = F)
#' pq_read(x = x, to_duck = T)
#' pq_read(x = x, to_duck = F, as_data_frame = F)
#' pq_read(x = x, to_duck = T, as_data_frame = F)

pq_read <-
  function(x = NULL,
           file_path = NULL,
           as_data_frame = T,
           assign_schema = T,
           schema_name = NULL,
           to_duck = F,
           snake_names = F,
           remove_columns = NULL,
           properties =  ParquetArrowReaderProperties$create()) {

    if (length(x) == 0) {
      "Please enter a parquet file" %>% message()
      return(invisible())
    }

    oldwd <- getwd()
    setwd("~")

    if (length(file_path) == 0) {
      full_path <- x
    }

    if (length(file_path) > 0) {
      file_path <- file_path %>% str_remove_all("\\/$")
      full_path <-
        glue::glue("{file_path}/{x}") %>% as.character()

    }

    if (as_data_frame) {
      data <- .pq_read_df(
        x = full_path,
        snake_names = snake_names,
        remove_columns = remove_columns,
        to_duck = to_duck,
        properties = properties
      )

      return(data)
    }

    if (!as_data_frame) {
      data <-
        .pq_read_arrow(
          x = full_path,
          to_duck = to_duck,
          snake_names = snake_names,
          remove_columns = remove_columns,
          properties = properties,
          assign_schema = assign_schema,
          schema_name = schema_name
        )
    }

    if (oldwd != getwd()) {
      setwd(oldwd)
    }



    data

  }

#' Read set of parquet fules
#'
#' @param path
#' @param as_data_frame
#' @param to_duck
#' @param exclude_files
#' @param schema_file
#' @param partitioning
#' @param unify_schemas
#' @param snake_names
#' @param add_file_name
#' @param to_arrow_table
#' @param return_message
#' @param schema_name
#' @param assign_schema
#'
#' @return
#' @export
#'
#' @examples
#' library(asbtools)
#' library(tidyverse)
#'
#'tbl <- pq_read_files(path = "Desktop/data/usa_spending/contract_archives/solicitations/", as_data_frame = F)
#'
#'tbl %>% count(year_data, sort= T) %>% collect()
#'
#'pq_read_files(path = "Desktop/data/usa_spending/contract_archives/solicitations/", as_data_frame = F)
#'
#'
pq_read_files <-
  function(path = NULL,
           as_data_frame = F,
           to_duck = F,
           exclude_files = NULL,
           schema_file = NULL,
           schema_name = NULL,
           assign_schema = T,
           partitioning = NULL,
           unify_schemas = NULL,
           snake_names = T,
           add_file_name = F,
           to_arrow_table = F,
           return_message = T) {
    if (length(path) == 0) {
      stop("Enter path")
    }
    oldwd <- getwd()
    setwd("~")

    if (!as_data_frame) {
      con <- arrow_open_data(
        sources = path,
        schema_file = schema_file,
        to_duck = to_duck,
        partitioning = partitioning,
        format = "parquet",
        unify_schemas = unify_schemas,
        schema_name = schema_name,
        assign_schema = assign_schema
      )
      return(con)
    }

    setwd(path)

    files <- list.files()[list.files() %>% str_detect(".parquet")]

    if (length(files) == 0) {
      message("No parquet files")
      return(invisible())
    }

    if (length(exclude_files) > 0) {
      exclude_slugs <- exclude_files %>% str_c(collapse = "|")

      if (return_message) {
        glue('Excluding {exclude_files %>% str_c(collapse = ", ")} parquet files') %>% message()

      }
      files  <- files[!files %>% str_detect(exclude_slugs)]
      if (length(files) == 0) {
        message("No parquet files")
        return(invisible())
      }
    }

    all_data <-
      files %>%
      map_dfr(function(x) {
        file <- x %>% str_remove_all("\\.gz.parquet")
        if (return_message) {
          glue("\n\nReading {file}\n\n") %>% message()
        }
        data <- read_parquet(x, as_data_frame = as_data_frame)

        zip_cols <-
          data %>% select(matches("^zip|^fax|^phone")) %>% names()

        if (length(zip_cols) > 0) {
          data <- data %>%
            mutate_at(zip_cols, as.character)
        }
        if (add_file_name) {
          data <-
            data %>%
            mutate(name_file = as.character(file)) %>%
            select(name_file, everything())
        }

        data
      })


    if (oldwd != getwd()) {
      setwd(oldwd)
    }

    if (to_duck) {
      "Coercing to duckdb" %>% message()
      all_data <-
        all_data %>%
        arrow::to_duckdb()
    }

    if (to_arrow_table) {
      "Coercing to arrow table" %>% message()
      all_data <- all_data %>%
        arrow::arrow_table()
      if (snake_names) {
        all_data <- snake_arrow_names(data = all_data)
      }
    }

    if (!to_arrow_table) {
      if (snake_names) {
        all_data <- all_data %>%
          janitor::clean_names()
      }
    }

    all_data
  }


#' RDA to Parquet
#'
#' @param rda_file
#' @param unique_data
#' @param sort_column
#' @param pq_path
#' @param pq_folder
#' @param pq_file
#' @param use_compression
#' @param return_message
#'
#' @return
#' @export
#'
#' @examples
rda_to_pq <-
  function(rda_file = "Desktop/data/usa_spending/fpds_atom/2018_final.rda",
           unique_data = F,
           sort_column = NULL,
           pq_path = "Desktop/data/",
           pq_folder = "fpds",
           pq_file = NULL,
           use_compression = T,
           return_message = T) {
    oldwd <- getwd()
    setwd("~")

    if (return_message) {
      glue("Reading {rda_file}") %>% message()
    }

    data <- read_rda(file = rda_file)

    if (unique_data) {
      data <- unique(data)
    }


    if (length(sort_column) > 0) {
      data <- data %>%
        arrange(!!!sym(sort_column))
    }

    pq_write(
      data = data,
      file_path = pq_path,
      folder = pq_folder,
      file_name = pq_file,
      use_compression = use_compression
    )

    rm(data)
    gc()

    if (getwd() != oldwd) {
      setwd(oldwd)
    }
    return(invisible())
  }


#' Open an Arrow Data Set
#'
#' @param sources vector of locations
#' @param schema
#' @param partitioning
#' @param unify_schemas
#' @param format options are `parquet` `ipc` `feather` `csv` `tsv`
#' @param ...
#' @param schema_file Location of the schema file
#' @param to_duck
#' @param assign_schema
#' @param schema_name
#'
#' @return
#' @export
#' @examples
#' library(asbtools)
#' arrow_open_data(sources = "Desktop/data/usa_spending/assistance/", schema_file = "Desktop/data/usa_spending/assistance/2021.gz.parquet", schema_name = "assistance")
#' asbtools::arrow_open_data(sources  = "Desktop/abresler.github.io/r_packages/govtrackR/data/thousand_talents.tsv.gz", format = "csv") %>% count(nameSponsor, sort =  T) %>% collect()
#' arrow_open_data(sources  = "Desktop/abresler.github.io/r_packages/govtrackR/data/thousand_talents.tsv.gz", format = "csv", to_duck = T)
#'
arrow_open_data <-
  function(sources = NULL,
           schema_file = NULL,
           schema = NULL,
           schema_name = NULL,
           assign_schema = T,
           to_duck = F,
           partitioning = NULL,
           unify_schemas = NULL,
           format = c("parquet", "arrow", "ipc",
                      "feather", "csv", "tsv", "text"),
           ...) {
    if (length(sources) == 0) {
      "No Source" %>% message()
      return(invisible())
    }

    oldwd <- getwd()
    setwd("~")

    if (length(schema_file) > 0) {
      glue::glue("Setting schema file to {schema_file}") %>% message()
      data <-
        arrow::open_dataset(sources  = schema_file)
      schema <- data$schema
      rm(data)
      gc()
    }

    con <- arrow::open_dataset(
      sources = sources,
      schema = schema,
      unify_schemas = unify_schemas,
      format = format,
      partitioning = partitioning,
      ...
    )


    if (assign_schema) {
      tbl_features <-
        tibble(feature = con$schema$ToString() %>% str_split("\\n") %>% flatten_chr()) %>%
        tidyr::separate(
          col = feature,
          into = c("feature", "column_type"),
          sep = "\\:"
        ) %>%
        mutate_if(is.character, str_squish) %>%
        mutate(number_column = 1:n(),
               number_column_python = 1:n() - 1) %>%
        select(number_column, number_column_python, everything())

      if (length(schema_name) == 0) {
        schema_name <- "tbl_arrow_schema"
      }

      if (length(schema_name) > 0) {
        schema_name <-
          glue("tbl_arrow_schema_{schema_name}") %>% as.character()
      }

      assign(x = schema_name, value = tbl_features, envir = .GlobalEnv)
    }


    if (to_duck) {
      "Coercing to duckdb" %>% message()
      con <-
        con %>%
        arrow::to_duckdb()
    }

    if (getwd() != oldwd) {
      setwd(oldwd)
    }

    con

  }

