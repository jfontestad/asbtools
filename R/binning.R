



# cut ---------------------------------------------------------------------

.cut_variable <-
  function(data,
           variable = "price",
           breaks = 10,
           override_labels = NULL,
           is_ordered_factor = T,
           include.lowest = T,
           digit_label = 10,
           right = T) {
    if (length(variable) == 0) {
      "Enter a cut variable" %>%
        message()
      return(data)
    }
    is_numeric_variable <-
      data %>% select(one_of(variable)) %>%
      select_if(is.numeric) %>%
      ncol() > 0

    if (!is_numeric_variable) {
      glue("{variable} is not numeric") %>% message()
      return(data)
    }

    if (length(breaks) == 0) {
      "Enter number or vector of breaks" %>% message()
      return(data)
    }

    if (length(breaks) == 1) {
      zeros <-
        max(0, 4 - nchar(breaks))
      zeros <-
        rep(as.character(0), each = zeros) %>% str_c(collapse = "")       # not the same.

      type_slug <-
        glue("{zeros}{breaks}_cut_bins")
    }

    if (length(breaks) > 1) {
      zeros <-
        max(0, 4 - nchar(length(breaks)))
      zeros <-
        rep(as.character(0), each = zeros) %>% str_c(collapse = "")       # not the same.
      type_slug <-
        glue("{zeros}{length(breaks)}_cut_bins_{min(breaks)}_to_{max(breaks)}")
    }

    base_var <- glue("{variable}_{type_slug}")


    data %>%
      mutate(
        UQ(base_var) := cut(
          !!sym(variable),
          breaks = breaks,
          labels = override_labels,
          ordered_result = is_ordered_factor,
          right = right,
          include.lowest = include.lowest,
          dig.lab = digit_label
        )
      )
  }

#' Create Cut Bins from a Numeric Variable
#'
#' @param data
#' @param variables vector of variable names
#' @param breaks if a a single number the number of breaks or a numeric vector with the breaks
#' @param override_labels
#' @param is_ordered_factor
#' @param digit_label
#' @param right
#'
#' @return
#' @export
#'
#' @examples
#'
#' library(ggplot2)
#' library(asbtools)
#'
#' tbl_cut_variables(data = diamonds, variables = c("price", "table"), breaks = 10)
#'
#' tbl_cut_variables(data = diamonds, breaks = c(0, 1000, 5000, 15000, 20000), variable = "price", right = F, include.lowest = F) %>% count(price_0005_cut_bins_0_to_20000)
#'
#'
tbl_cut_variables <-
  function(data,
           variables = NULL,
           breaks = NULL,
           override_labels = NULL,
           is_ordered_factor = T,
           digit_label = 10,
           include.lowest = T,
           right = T) {
    df_input <- expand.grid(variable = variables,
                            stringsAsFactors = F) %>%
      as_tibble()

    original_names <- names(data)

    data <-
      1:nrow(df_input) %>%
      map(function(x) {
        df_r <- df_input[x, ]

        .cut_variable(
          data = data,
          variable = df_r$variable,
          breaks = breaks,
          override_labels = override_labels,
          is_ordered_factor = is_ordered_factor,
          digit_label = digit_label,
          include.lowest = include.lowest,
          right = right
        )
      }) %>%
      reduce(left_join, by = original_names)

    if (remove_original_variables) {
      data <- data %>%
        select(-one_of(remove_original_variables))
    }

    data

  }

# ntil_bin ----------------------------------------------------------------

#' Create ntil bins of a numeric variable
#'
#' @param data
#' @param variable
#' @param ntiles
#'
#' @return
#' @export
#'
#' @examples
#' library(ggplot2)
#' library(asbtools)
#'
#' ntile_bin(data = diamonds, "price", 12)
ntile_bin <-
  function(data,
           variable = NULL,
           ntiles = NULL) {
    if (length(variable) == 0) {
      "Enter a bin variable" %>%
        message()
      return(data)
    }

    if (length(ntiles) == 0) {
      "Enter a ntiles variable" %>%
        message()
      return(data)
    }

    tbl_variable <-
      data %>%
      select(one_of(variable)) %>%
      arrange(!!sym(variable)) %>%
      distinct()

    is_numeric_feature <- tbl_variable %>%
      select_if(is.numeric) %>%
      ncol() > 0

    if (!is_numeric_feature) {
      glue("{variable} is not numeric") %>% message()
      return(data)
    }

    zeros <-
      max(0, 4 - nchar(ntiles))
    zeros <-
      rep(as.character(0), each = zeros) %>% str_c(collapse = "")       # not the same.

    base_var <- glue("{variable}_{zeros}{ntiles}_bins")
    bin_id <-
      glue("bin_ntile_{base_var}")
    bin_name <-
      glue("bin_ntile_name_{base_var}")

    min_name <-
      glue("min_{base_var}")

    max_name <-
      glue("max_{base_var}")

    count_name <-
      glue("count_{base_var}")


    tbl_variable <-
      tbl_variable %>%
      mutate(UQ(bin_id) := dplyr::ntile(x = !!sym(variable), n = ntiles)) %>%
      group_by(!!sym(bin_id)) %>%
      summarise(
        UQ(min_name) := min(!!sym(variable), na.rm = T),
        UQ(max_name) := max(!!sym(variable), na.rm = T),
        UQ(count_name) := n()
      ) %>%
      mutate(UQ(bin_name) := str_c("From", !!sym(min_name), !!sym(max_name), sep = " to "))

    levels <- tbl_variable %>% pull(bin_name)

    tbl_variable %>%
      mutate(UQ(bin_name) := factor(!!sym(bin_name), levels = levels, ordered = T))


  }

.tbl_ntile_bin <-
  function(data,
           variable = NULL,
           ntiles = NULL,
           remove_original_bin = F,
           remove_summary_features = T) {
    tbl_bins <-
      ntile_bin(data = data,
                variable = variable,
                ntiles = ntiles)

    tbl_bin_var <-
      data %>%
      select(one_of(variable)) %>%
      arrange(!!sym(variable)) %>%
      distinct()

    join_var <-
      tbl_bins %>% select(matches("^min_")) %>% names()

    tbl_bin_var <-
      tbl_bin_var %>%
      left_join(tbl_bins %>% mutate(UQ(variable) := !!sym(join_var)), by = variable)

    fill_vars <- names(tbl_bin_var %>% select(-variable))

    fill_vars %>%
      walk(function(x) {
        glue("Filling {x}") %>% message()
        tbl_bin_var <<- tbl_bin_var %>%
          tidyr::fill(!!sym(x))
      })



    if (remove_summary_features) {
      tbl_bin_var <- tbl_bin_var %>%
        select(one_of(variable), matches("^name_ntile_bin|^bin_"))
    }

    data <-
      data %>%
      left_join(tbl_bin_var, by = variable)

    if (remove_original_bin) {
      data <- data %>%
        select(-one_of(variable))
    }

    data

  }

#' Create ntile bins
#'
#' @param data
#' @param variables name of the bin variables
#' @param ntiles number of ntiles
#' @param remove_original_bin remove the orignial numeric feature
#' @param remove_summary_features remove summaray fatures
#'
#' @return
#' @export
#'
#' @examples
#' library(ggplot2)
#' library(asbtools)
#'
#' tbl_ntile_bin(diamonds, variables = c("price", "carat"), ntiles = c(5,10))
#' tbl_ntile_bin(diamonds, variables = c("price"), ntiles = c(5,10))
#' tbl_ntile_bin(diamonds, variables = c("carat"), ntiles = c(10))
#'
tbl_ntile_bin <-
  function(data,
           variables = NULL,
           ntiles = NULL,
           remove_original_bin = F,
           remove_summary_features = T) {
    df_input <- expand.grid(variable = variables,
                            ntile = ntiles,
                            stringsAsFactors = F) %>%
      as_tibble()

    original_names <- names(data)

    if (remove_original_bin) {
      original_names <- original_names %>%
        discard(function(x) {
          x %in% variables
        })
    }

    1:nrow(df_input) %>%
      map(function(x) {
        df_r <- df_input[x, ]

        .tbl_ntile_bin(
          data = data,
          variable = df_r$variable,
          ntiles = df_r$ntile,
          remove_original_bin = remove_original_bin,
          remove_summary_features = remove_summary_features
        )
      }) %>%
      reduce(left_join, by = original_names)
  }



# rbin --------------------------------------------------------------------





# binr --------------------------------------------------------------------

.bin_r_variable <-
  function(data,
           variable = "price",
           target_bins = 10,
           max_breaks = 100,
           exact_groups = F,
           verbose = T,
           errthresh = 0.1,
           minpts = NA) {
    if (length(variable) == 0) {
      "Enter a cut variable" %>%
        message()
      return(data)
    }
    is_numeric_variable <-
      data %>% select(one_of(variable)) %>%
      select_if(is.numeric) %>%
      ncol() > 0

    if (!is_numeric_variable) {
      glue("{variable} is not numeric") %>% message()
      return(data)
    }

    if (length(target_bins) == 0) {
      "Enter number or vector of target breaks" %>% message()
      return(data)
    }

    if (length(max_breaks) == 0) {
      max_breaks <- target_bins
    }

    if (max_breaks < target_bins) {
      max_breaks <- target_bins
    }

    zeros <-
      max(0, 4 - nchar(target_bins))
    zeros <-
      rep(as.character(0), each = zeros) %>% str_c(collapse = "")       # not the same.

    type_slug <-
      glue("{zeros}{target_bins}_rbin_bins")

    base_var <- glue("{variable}_{type_slug}")

    data <- data %>%
      arrange(!!sym(variable))

    x <- data %>%
      pull(variable)

    obj <- binr::bins(x = x, target.bins = target_bins, max.breaks = max_breaks, exact.groups = exact_groups, verbose = verbose, errthresh = errthresh, minpts = minpts)

    tbl_bins <-
      tibble(UQ(base_var) := names(obj$binct)) %>%
      separate(col = base_var,into = c(variable, "remove"), sep = "\\, ", convert = T, remove = F) %>%
      select(-remove) %>%
      select(one_of(variable), everything()) %>%
      mutate(UQ(variable) := readr::parse_number(!!sym(variable)))

    levels <- tbl_bins %>% pull(!!sym(base_var))

    tbl_bins <-
      tbl_bins %>%
      mutate(UQ(base_var) := factor(!!sym(base_var), levels = levels, ordered = T))

    data <- data %>%
      left_join(tbl_bins, by = variable)

    fill_vars <- names(tbl_bins %>% select(-variable))

    fill_vars %>%
      walk(function(x) {
        glue("Filling {x}") %>% message()
        data <<- data %>%
          tidyr::fill(!!sym(x))
      })

    data

  }

#' Cut Numeric Values Into Evenly Distributed Groups (bins).
#'
#' @param data
#' @param variables
#' @param target_bins
#' @param max_breaks
#' @param exact_groups
#' @param verbose
#' @param errthresh
#' @param minpts
#'
#' @return
#' @export
#'
#' @examples
#' library(ggplot2)
#' library(tidyverse)
#' library(asbtools)
#' tbl_binr(data = diamonds, variables = c("price", "table"), target_bins = 5, max_breaks = 1)
#'
#'
tbl_binr <-
  function(data,
           variables = NULL,
           remove_original_variables = F,
           target_bins = NULL,
           max_breaks = NULL,
           exact_groups = F,
           verbose = T,
           errthresh = 0.1,
           minpts = NA) {
    df_input <- expand.grid(variable = variables,
                            stringsAsFactors = F) %>%
      as_tibble()

    original_names <- names(data)

    data <-
      1:nrow(df_input) %>%
      map(function(x) {
        df_r <- df_input[x, ]

        .bin_r_variable(
          data = data,
          variable = df_r$variable,
          target_bins = target_bins,
          max_breaks = max_breaks,
          exact_groups = exact_groups,
          verbose = verbose,
          errthresh = errthresh,
          minpts = minpts
        )
      }) %>%
      reduce(left_join, by = original_names)

    if (remove_original_variables) {
      data <- data %>%
        select(-one_of(variables))
    }

    data
  }
