#' Filter across features
#'
#' @param data
#' @param filter_across_columns
#' @param exclude_filters
#' @param keep_filters
#'
#' @return
#' @export
#'
#' @examples
filter_across <-
  function(data,
           filter_across_columns = NULL,
           exclude_filters = NULL,
           keep_filters = NULL) {
    if (length(filter_across_columns) == 0) {
      return(data)
    }

    if (length(exclude_filters) > 0) {
      filter_slug <-
        exclude_filters %>% str_c(collapse = "|")

      filter_across_columns %>%
        walk(function(x) {
          glue("Filtering out {x} excluding {filter_slug}") %>% message()
          data <<- data %>%
            filter(!(!!sym(x) %>% str_detect(filter_slug)))
        })
    }

    if (length(keep_filters) > 0) {
      filter_slug <-
        keep_filters %>% str_c(collapse = "|")

      filter_across_columns %>%
        walk(function(x) {
          glue("Filtering {x} keeping {filter_slug}") %>% message()
          data <<-
            data %>%
            filter((!!sym(x) %>% str_detect(filter_slug)))
        })
    }

    data
  }

.tbl_allocate <-
  function(data,
           allocation_variable = NULL,
           split_variable = NULL,
           split_separator = "\\|",
           split_data = TRUE,
           is_already_allocated = F,
           remove_orginal_allocation = TRUE) {
    if (length(allocation_variable) == 0) {
      message("Enter allocation variable")
      return(data)
    }

    if (length(split_variable) == 0) {
      message("Enter spit variable")
      return(data)
    }

    if (is_already_allocated) {
      amt_var <- allocation_variable
    } else {
      amt_var <-
        glue("{allocation_variable}_allocated") %>% as.character()

    }


    data <- data %>%
      mutate(
        count_split = !!sym(split_variable) %>% str_count(split_separator) + 1,
        UQ(amt_var) := (1 / count_split) * (!!sym(allocation_variable))
      ) %>%
      select(-count_split)

    if (remove_orginal_allocation) {
      data <- data %>%
        select(-one_of(allocation_variable))
    }

    if (split_data) {
      data <-
        data %>%
        separate_rows(!!sym(split_variable), sep = split_separator) %>%
        mutate_if(is.character, str_squish)
    }

    data

  }

#' Allocate summarized data
#'
#' @param data
#' @param split_separator
#' @param keep_columns
#' @param remove_columns
#' @param split_data
#' @param remove_orginal_allocation
#' @param allocation_variable
#' @param split_variables
#' @param summarise_groups
#' @param widen_variable
#' @param count_variable
#' @param distinct_variables
#' @param amount_variables
#' @param mean_variables
#' @param median_variables
#' @param min_variables
#' @param max_variables
#' @param first_variables
#' @param last_variables
#' @param variance_variables
#' @param sd_variables
#' @param which_max_variables
#' @param which_min_variables
#' @param remove_top_amount
#' @param coalesce_numeric
#' @param return_message
#'
#' @return
#' @export
#'
#' @examples
tbl_allocate <-
  function(data,
           allocation_variable = NULL,
           split_variables = NULL,
           split_separator = "\\|",
           keep_columns = NULL,
           remove_columns = NULL,
           split_data = TRUE,
           summarise_groups = NULL,
           widen_variable = NULL,
           count_variable = "count",
           distinct_variables = NULL,
           amount_variables = NULL,
           mean_variables = NULL,
           median_variables = NULL,
           min_variables = NULL,
           max_variables = NULL,
           first_variables = NULL,
           last_variables = NULL,
           variance_variables = NULL,
           sd_variables = NULL,
           which_max_variables = NULL,
           which_min_variables = NULL,
           remove_top_amount = TRUE,
           coalesce_numeric = F,
           remove_orginal_allocation = TRUE,
           return_message = TRUE) {
    if (length(keep_columns) > 0) {
      data <-
        data %>%
        select(one_of(c(
          keep_columns, split_variables, allocation_variable
        )))
    }

    if (length(remove_columns) > 0) {
      data <- data %>%
        select(-one_of(remove_columns))
    }

    seq_along(split_variables) %>%
      walk((function(x) {
        split_var <-
          split_variables[[x]]

        if (return_message) {
          glue("\n\nAllocating {allocation_variable} over {split_var}\n\n") %>% message()
        }

        if (x != 1) {
          alloc_var <-
            glue("{allocation_variable}_allocated") %>% as.character()
          is_already_allocated <- T
          remove_alloc <- F
        } else {
          alloc_var <- allocation_variable
          remove_alloc <- T
          is_already_allocated <- F
        }

        data <<-
          .tbl_allocate(
            data = data,
            allocation_variable = alloc_var,
            split_variable = split_var,
            split_data = split_data,
            remove_orginal_allocation = remove_alloc,
            split_separator = split_separator,
            is_already_allocated = is_already_allocated
          )

      }))

    if (length(summarise_groups) > 0) {
      if (return_message) {
        glue("Summarizing by {str_c(summarise_groups, collapse = ', ')}") %>% message()
      }
      amt_col <-
        glue("{allocation_variable}_allocated") %>% as.character()
      data <- tbl_summarise(
        data = data,
        group_variables = summarise_groups,
        widen_variable = widen_variable,
        count_variable = count_variable,
        distinct_variables = distinct_variables,
        amount_variables = amt_col,
        mean_variables = mean_variables,
        median_variables = median_variables,
        min_variables = min_variables,
        max_variables = max_variables,
        first_variables = first_variables,
        last_variables = last_variables,
        variance_variables = variance_variables,
        sd_variables = sd_variables,
        coalesce_numeric = coalesce_numeric,
        top_variables = 1,
        calculation_variable = amt_col,
        which_max_variables = which_max_variables,
        which_min_variables = which_min_variables,
        remove_top_amount = remove_top_amount,
      )
    }

    data
  }

# summarise ---------------------------------------------------------------

#' Summarise across tibble
#'
#' @param data
#' @param group_variables
#' @param widen_variable
#' @param count_variable
#' @param distinct_variables
#' @param amount_variables
#' @param mean_variables
#' @param median_variables
#' @param min_variables
#' @param max_variables
#' @param first_variables
#' @param last_variables
#' @param variance_variables
#' @param sd_variables
#' @param top_variables
#' @param calculation_variable
#' @param which_max_variables
#' @param which_min_variables
#' @param unique_variables
#' @param coalesce_numeric
#' @param remove_top_amount
#' @param filters
#' @param ...
#' @param unique_concatenator
#'
#' @return
#' @export
#'
#' @examples
#' library(tidyverse)
#' library(asbviz)
#' ggplot2::diamonds %>%
#' mutate_if(is.factor, as.character) %>%
#' tbl_summarise(
#' unique_variables = "clarity",
#' calculation_variable = "price",
#' amount_variable = "price"
#' )
#'
#' ggplot2::diamonds %>%
#' tbl_summarise(
#' group = "cut",
#' unique_variables = "clarity",
#' calculation_variable = "price",
#' amount_variable = "price"
#' )
#'
#' ggplot2::diamonds %>%
#' tbl_summarise(
#' widen_variable = "cut",
#' unique_variables = "clarity",
#' calculation_variable = "price",
#' amount_variable = "price"
#' )
#'
#'
#'
tbl_summarise <-
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
           coalesce_numeric = F,
           remove_top_amount = T,
           filters = c("UNNAMED", "UNKNOWN"),
           ...) {

    if (length(group_variables) == 0 & length(widen_variable) == 0) {
      group_slugs <- NULL
    } else {
      group_slugs <- c(group_variables, widen_variable) %>% unique()
    }

    all_data <- tibble()

    across_length <-
      length(distinct_variables) + length(amount_variables) + length(mean_variables) + length(median_variables) +
      length(min_variables) + length(max_variables) + length(first_variables) + length(last_variables) + length(variance_variables) + length(sd_variables) +
      length(unique_variables)

    has_across <-  across_length > 0

    if (across_length + length(count_variable) == 0) {
      "No summary variables" %>% message()
      return(tibble())
    }

    if (length(count_variable) > 0) {
      all_data <-
        data %>%
        group_by(!!!syms(group_slugs)) %>%
        summarise(UQ(count_variable) := n(),
                  .groups = "drop")
    }

    if (has_across) {
      if (length(calculation_variable) == 0) {
        calculation_variable <- amount_variables[[1]]
      }
      df <-
        data %>%
        group_by(!!!syms(group_slugs)) %>%
        summarise(
          across(
            .cols = all_of(amount_variables),
            .fns = ~ {
              sum(.x, na.rm = T)
            },
            .names = "{.col}_total"
          ),
          across(
            .cols = all_of(distinct_variables),
            .fns = ~ {
              n_distinct(.x, na.rm = T)
            },
            .names = "{.col}_distinct"
          ),
          across(
            .cols = all_of(mean_variables),
            .fns = ~ {
              mean(.x, na.rm = T)
            },
            .names = "{.col}_mean"
          ),
          across(
            .cols = all_of(median_variables),
            .fns = ~ {
              median(.x, na.rm = T)
            },
            .names = "{.col}_median"
          ),
          across(
            .cols = all_of(min_variables),
            .fns = ~ {
              min(.x, na.rm = T)
            },
            .names = "{.col}_min"
          ),
          across(
            .cols = all_of(max_variables),
            .fns = ~ {
              max(.x, na.rm = T)
            },
            .names = "{.col}_max"
          ),
          across(
            .cols = all_of(unique_variables),
            .fns = ~ {
              .x %>%
                str_split("\\|") %>% flatten_chr() %>% str_squish() %>%
                unique() %>% str_c(collapse = unique_concatenator)
            },
            .names = "{.col}_unique"
          ),
          across(
            .cols = all_of(sd_variables),
            .fns = ~ {
              sd(.x, na.rm = T)
            },
            .names = "{.col}_sd"
          ),
          across(
            .cols = all_of(variance_variables),
            .fns = ~ {
              sd(.x, na.rm = T)
            },
            .names = "{.col}_variance"
          ),
          across(
            .cols = all_of(first_variables),
            .fns = ~ {
              first(.x, na.rm = T)
            },
            .names = "{.col}_first"
          ),
          across(
            .cols = all_of(which_max_variables),
            .fns = ~ {
              .x[which.max(!!sym(calculation_variable))]
            },
            .names = "{.col}_which_max"
          ),
          across(
            .cols = all_of(which_min_variables),
            .fns = ~ {
              .x[which.min(!!sym(calculation_variable))]
            },
            .names = "{.col}_which_min"
          ),
          across(
            .cols = all_of(last_variables),
            .fns = ~ {
              last(.x, na.rm = T)
            },
            .names = "{.col}_first"
          ),
          ...,
          .groups = "drop"
        )



      if (nrow(all_data) > 0) {
        if (length(group_slugs) > 0) {
          all_data <- all_data %>%
            left_join(df, by = group_slugs)
        }
        if (length(group_slugs) == 0) {
          all_data <- all_data %>%
            bind_cols(df)
        }
      }

      if (nrow(all_data) == 0) {
        all_data <- df
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

      all_data <- wide_vars %>%
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

    if (length(top_variables) > 0) {
      df_top <-
        tbl_top_n_groups(
          data = data,
          group_variables = group_variables,
          top_variables = top_variables,
          calculation_variable = calculation_variable,
          filters = filters,
          top = 1,
          remove_top_amount = remove_top_amount
        )

      all_data <-
        all_data %>%
        left_join(df_top, by = group_variables)
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
      new_var <- names(all_data)[names(all_data) %>% str_detect("_total_total$")] %>%
        str_remove_all("_total")
      names(all_data)[names(all_data) %>% str_detect("_total_total$")] <-
        names(all_data)[names(all_data) %>% str_detect("_total_total$")] %>%
        str_remove_all("_total")

      names(all_data)[names(all_data) %>% str_detect(new_var)] <-
        names(all_data)[names(all_data) %>% str_detect(new_var)] %>% str_c("_total")
    }


    all_data

  }
# count -------------------------------------------------------------------
.count_variable <-
  function(data,
           matching_variables = c("^name", "^type"),
           exclude_columns = c("nameContract"),
           numeric_columns = NULL,
           include_logical = T,
           include_factor = T,
           weight = NA,
           include_proportion = T) {
    data <-
      data %>%
      dplyr::select(which(colMeans(is.na(.)) < 1))
    match_slugs <- matching_variables %>% str_c(collapse = "|")
    select_names <- c()
    char_names <-
      data %>% select(matches(match_slugs)) %>% select_if(list(function(x) {
        is.character(x)
      })) %>% names()
    select_names <- select_names %>% append(char_names)

    if (include_logical) {
      select_names <-
        select_names %>% append(data %>% select_if(is.logical) %>% names())
    }

    if (include_factor) {
      select_names <-
        select_names %>% append(data %>% select_if(is.factor) %>% names())
    }

    if (length(numeric_columns) > 0) {
      select_names <-
        select_names %>%
        append(data %>%
                 select_if(is.numeric) %>% names() %>% keep(function(x) {
                   x %in% numeric_columns
                 }))
    }


    if (length(exclude_columns) > 0) {
      select_columns <-
        select_names %>% discard(function(x) {
          x %in% exclude_columns
        })
    } else {
      select_columns <-
        select_names
    }


    if (is.na(weight)) {
      df_counts <-
        select_columns %>%
        map_dfr(function(col) {
          d <- data %>% count(!!sym(col), name = "count", sort = T) %>%
            rename(variable := !!sym(col)) %>%
            mutate(variable = as.character(variable),
                   column = col) %>%
            select(column, everything()) %>%
            mutate_if(is.numeric, as.numeric)

          if (include_proportion) {
            new_variable <- "pct_count"
            d <- d %>%
              mutate(!!sym(new_variable) := count / sum(count))
          }
          return(d)
        })
    } else {
      df_counts <-
        select_columns %>%
        map_dfr(function(col) {
          d <-
            data %>% count(
              !!sym(col),
              name = weight,
              wt = !!sym(weight),
              sort = T
            ) %>%
            rename(variable := !!sym(col)) %>%
            mutate(variable = as.character(variable),
                   column = col) %>%
            select(column, everything()) %>%
            mutate_if(is.numeric, as.numeric)

          if (include_proportion) {
            new_var <- str_c("pct_", weight %>% make_clean_names())
            d <-
              d %>%
              mutate(!!sym(new_var) := !!sym(weight) / sum(!!sym(weight)))

            d
          }
        })
    }

    df_counts <-
      df_counts %>%
      mutate(variable = variable %>% coalesce("UNKNOWN"))

    df_counts

  }

#' Build a tibble of counts
#'
#' @param data
#' @param include_logical
#' @param include_factor
#' @param unite_features
#' @param include_proportion
#' @param matching_variables
#' @param weight_columns
#' @param exclude_columns
#' @param numeric_columns
#' @param scale_results
#'
#' @return
#' @export
#'
#' @examples
tbl_count <-
  function(data,
           matching_variables = c("^name", "^type"),
           weight_columns = c(NA),
           exclude_columns = NULL,
           include_logical = T,
           include_factor = T,
           unite_features = T,
           numeric_columns = NULL,
           scale_results = F,
           include_proportion = T) {
    if (length(weight_columns) == 0) {
      stop("Enter weights")
    }

    all_data <-
      weight_columns %>%
      map(function(weight) {
        .count_variable(
          data = data,
          matching_variables = matching_variables,
          exclude_columns = exclude_columns,
          include_logical = include_logical,
          weight = weight,
          include_factor = include_factor,
          numeric_columns = numeric_columns,
          include_proportion = include_proportion
        )
      })

    all_data <-
      all_data %>% reduce(left_join, by = c("column", "variable"))

    if (unite_features) {
      all_data <-
        all_data %>%
        unite(item, column, variable, remove = F) %>%
        mutate(item = item %>% map_chr(janitor::make_clean_names))
    }

    if (scale_results) {
      all_data <- all_data %>%
        pre_process_data(scale_data = T, center = T)
    }

    all_data
  }


# groups ------------------------------------------------------------------

.top_group <-
  function(data,
           group_variables = "parent_keywords",
           top_variable = "name_awardee_clean",
           calculation_variable = "amount_award_allocated",
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

#' Top Groups
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
tbl_top_n_groups <-
  function(data,
           group_variables = NULL,
           top_variables = NULL,
           calculation_variable = "amount_award_allocated",
           filters = c("UNNAMED", "UNKNOWN"),
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



# feature_correlations ----------------------------------------------------

#' Correlation tibble
#'
#' @param data
#' @param correlation_method	a character string indicating which correlation coefficient (or covariance) is to be computed. One of "pearson" (default), "kendall", or "spearman": can be abbreviated.
#' @param correlation_used an optional character string giving a method for computing covariances in the presence of missing values. This must be (an abbreviation of) one of the strings "everything", "all.obs", "complete.obs", "na.or.complete", or "pairwise.complete.obs".
#' @param diagonal
#' @param remove_columns
#' @param include_logical if `TRUE` includes logical features
#' @param include_factors if `TRUE` converts factors to variables
#' @param character_to_factor if `TRUE` converts character vectors to factor
#' @param full_rank if `TRUE` returns full rank dummy variables
#'
#' @return
#' @export
#'
#' @examples
tbl_correlations <-
  function(data,
           correlation_method = "pearson",
           correlation_used = "pairwise.complete.obs",
           diagonal = NA,
           remove_columns = NULL,
           include_logical = T,
           include_factors = F,
           character_to_factor = F,
           exclude_feature_columns = NULL,
           exclude_feature_correlated_columns = NULL,
           full_rank = T) {
    options(warn = -1)
    if (length(remove_columns)) {
      data <-
        data %>%
        select(-one_of(remove_columns))
    }

    if (character_to_factor)  {
      data <- data %>%
        mutate(across(is.factor, as.factor))
    }

    if (include_factors) {
      data <- data %>%
        dummify_data(snake_names = T,
                     is_full_rank = full_rank)
    }

    if (include_logical) {
      data <- data %>%
        mutate(across(is.logical, as.numeric))
    }

    data <-
      data %>%
      select_if(is.numeric) %>%
      correlate(use = correlation_used,
                diagonal = diagonal,
                method = correlation_method) %>%
      rename(feature = term)

    data <-
      data %>%
      pivot_longer(cols = data %>% select(-feature) %>% names(),
                   names_to = "feature_correlated") %>%
      mutate(correlation_method,
             correlation_used,
             .before = "feature") %>%
      filter(feature != feature_correlated) %>%
      mutate(
        value_absolute = abs(value),
        type_correlation = case_when(value > 0 ~ "positive",
                                     TRUE ~ "negative")
      ) %>%
      arrange(feature, -value_absolute)

    if (length(exclude_feature_columns) > 0) {
      exclude_feature_columns_slugs <-
        str_c(exclude_feature_columns, collapse = " | ")

      data <- data %>%
        filter(!feature %>% str_detect(exclude_feature_columns_slugs))
    }

    if (length(exclude_feature_correlated_columns) > 0) {
      exclude_feature_correlated_columns_slugs <-
        str_c(exclude_feature_correlated_columns, collapse = " | ")

      data <- data %>%
        filter(!feature_correlated %>% str_detect(exclude_feature_correlated_columns_slugs))
    }


    data


  }

#' Dimension Reduced Correlations
#'
#' @param data
#' @param exclude_dimensional_correlations
#' @param methods a character string indicating which correlation coefficient (or covariance) is to be computed. One of "pearson" (default), "kendall", or "spearman": can be abbreviated.
#' @param correlation_method an optional character string giving a method for computing covariances in the presence of missing values. This must be (an abbreviation of) one of the strings "everything", "all.obs", "complete.obs", "na.or.complete", or "pairwise.complete.obs".
#' @param correlation_used
#' @param diagonal
#' @param remove_columns
#' @param include_logical
#' @param include_factors
#' @param character_to_factor
#' @param full_rank
#'
#' @return
#' @export
#'
#' @examples
tbl_dimension_correlations <-
  function(data,
           exclude_dimensional_correlations = T,
           methods = c(
             "gng",
             "cur",
             "h2oae",
             "h2oglrm",
             "ica",
             "isomap",
             "kpca",
             "lle",
             "mds",
             "nmf",
             "pca",
             "spca",
             "svd",
             "tsne",
             "umap"
           ),
           correlation_method = "pearson",
           correlation_used = "pairwise.complete.obs",
           diagonal = NA,
           remove_columns = NULL,
           include_logical = T,
           include_factors = F,
           exclude_clusters = F,
           character_to_factor = F,
           full_rank = T) {
    slugs <-
      glue("{methods}_") %>% as.character() %>% str_c(collapse = "|")

    data <- tbl_correlations(
      data = data,
      correlation_method = correlation_method,
      correlation_used = correlation_used,
      diagonal = diagonal,
      remove_columns = remove_columns,
      include_logical = include_logical,
      include_factors = include_factors,
      character_to_factor = character_to_factor,
      full_rank = full_rank
    )

    data <-
      data %>%
      filter(feature %>% str_detect(slugs)) %>%
      rename(dimension = feature)

    if (exclude_dimensional_correlations) {
      data <-
        data %>%
        filter(!feature_correlated %>% str_detect(slugs))
    }

    if (exclude_clusters) {
      data <-
        data %>%
        filter(!feature_correlated %>% str_detect("is_cluster"))
    }

    data <-
      data %>%
      separate(
        dimension,
        into = c("dimension_method", "dimension_number"),
        remove = F
      ) %>%
      mutate(dimension_number = as.numeric(dimension_number)) %>%
      arrange(dimension_number, dimension_method) %>%
      mutate(
        dimension_number = case_when(
          nchar(dimension_number) == 1 ~ str_c("00", dimension_number),
          nchar(dimension_number) == 2 ~ str_c("0", dimension_number),
          TRUE ~ as.character(dimension_number)
        ) %>% factor(ordered = T)
      )

    data


  }

#' Correlation Clusters
#'
#' @param data
#' @param exclude_cluster_correlations
#' @param methods a character string indicating which correlation coefficient (or covariance) is to be computed. One of "pearson" (default), "kendall", or "spearman": can be abbreviated.
#' @param correlation_method an optional character string giving a method for computing covariances in the presence of missing values. This must be (an abbreviation of) one of the strings "everything", "all.obs", "complete.obs", "na.or.complete", or "pairwise.complete.obs".
#' @param correlation_used
#' @param diagonal
#' @param remove_columns
#' @param include_logical
#' @param include_factors
#' @param character_to_factor
#' @param full_rank
#'
#' @return
#' @export
#'
#' @examples
tbl_cluster_correlations <-
  function(data,
           exclude_cluster_correlations = T,
           methods = c(
             "som",
             "cmeans",
             "emc",
             "hardcl",
             "hopach",
             "h2okmeans",
             "kmeans",
             "ngas",
             "pam",
             "pamk",
             "spec"
           ),
           correlation_method = "pearson",
           correlation_used = "pairwise.complete.obs",
           diagonal = NA,
           remove_columns = NULL,
           include_logical = T,
           include_factors = F,
           character_to_factor = F,
           full_rank = T) {
    slugs <-
      glue("{methods}_") %>% as.character() %>% str_c(collapse = "|")

    data <- tbl_correlations(
      data = data,
      correlation_method = correlation_method,
      correlation_used = correlation_used,
      diagonal = diagonal,
      remove_columns = remove_columns,
      include_logical = include_logical,
      include_factors = include_factors,
      character_to_factor = character_to_factor,
      full_rank = full_rank
    )

    data <-
      data %>%
      filter(feature %>% str_detect(slugs)) %>%
      rename(cluster = feature) %>%
      mutate(
        cluster = cluster %>% str_remove_all("is_cluster_"),
        feature_correlated = feature_correlated %>% str_remove_all("is_cluster_")
      )

    if (exclude_cluster_correlations) {
      data <-
        data %>%
        filter(!feature_correlated %>% str_detect(slugs))
    }

    data <-
      data %>%
      separate(cluster,
               into = c("cluster_method", "cluster_number"),
               remove = F) %>%
      mutate(cluster_number = as.numeric(cluster_number)) %>%
      arrange(cluster_number, cluster_method) %>%
      mutate(
        cluster_number = case_when(
          nchar(cluster_number) == 1 ~ str_c("00", cluster_number),
          nchar(cluster_number) == 2 ~ str_c("0", cluster_number),
          TRUE ~ as.character(cluster_number)
        ) %>% factor(ordered = T)
      )

    data


  }


# nest --------------------------------------------------------------------


#' Nest a tibble by grouping variables and specified other variables
#'
#' @param data a `tibble`
#' @param grouping_variables vector of variables to group by
#' @param nesting_variables if not `NULL` vector of other variables to select
#' @param data_column_name if not `NULL` new nested data column name
#'
#' @return `tibble`
#' @export
#'
#' @examples
#'
#' gapminder::gapminder %>%
#' tbl_nest(grouping_variables = "year",
#' data_column_name = "ttdata")
#'
#'
#' gapminder::gapminder %>%
#' tbl_nest(grouping_variables = c("continent", "country"),
#' nesting_variables = c("pop", "year"))
#'
#'
tbl_nest <- function(data,
                     grouping_variables = NULL,
                     nesting_variables = NULL,
                     data_column_name = NULL) {
  if (length(grouping_variables) == 0) {
    "Define grouping variable(s)" %>% message()
    return(data)
  }

  if (length(nesting_variables) == 0) {
    data <- data %>%
      group_by(!!!syms(grouping_variables)) %>%
      nest() %>%
      ungroup()
  }

  if (length(nesting_variables) > 0 ) {

    data <-
      data %>%
      select(one_of(c(grouping_variables, nesting_variables))) %>%
      group_by(!!!syms(grouping_variables)) %>%
      nest() %>%
      ungroup()

  }

  if (length(data_column_name) > 0) {
    data <-
      data %>%
      rename(UQ(data_column_name) := data)
  }

  data
}
