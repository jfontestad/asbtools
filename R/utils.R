#' Assign a set of names to a tibble
#'
#' Requires a `feature` and `actual` column or
#' a `tibble` with 2 columns and the `actual` as the second colun
#'
#' @param data
#' @param dictionary_names
#'
#' @return
#' @export
#'
#' @examples
#' library(tibble)
#' dict_iris <- tibble(feature = names(iris), actual = c("SL", "SW", "PL", "PW", "Name of Species"))
#'
#' tbl_assign_dictionary_names(iris, dict_iris)
#' tbl_assign_dictionary_names(iris, dict_iris, snake_names = T)
#'
tbl_assign_dictionary_names <-
  function(data, dictionary_names = NULL,
           snake_names  = F) {

    if (length(dictionary_names) == 0) {
      "No name dictionary" %>% message()
      return(data)
    }

    data_names <- names(data)
    feature_col <- names(dictionary_names)[[1]]
    actual_col <- names(dictionary_names)[[2]]
    actual_names <-
      data_names %>%
      map_chr(function(x){

        df_row <- dictionary_names %>%
          filter(!!sym(feature_col) == x)

        if (nrow(df_row) == 0) {
          glue("Missing {x}") %>% message()
          return(x)
        }

        df_row[,2] %>% pull()


      })

    data <-
      data %>%
      setNames(actual_names) %>%
      as_tibble()

    if (snake_names) {
      data <- data %>%
        janitor::clean_names()
    }

    data
  }
#' Print a message using cat
#'
#' @param text vector text
#'
#' @return invisible
#' @export
#' @import glue purrr
#'
#' @examples
#' cat_message(text = "Hello World")
cat_message <-
  function(text = NULL) {
    if (length(text) == 0) {
      return(invisible())
    }

    text <- glue("\n\n{text}\n\n") %>% as.character()

    cat(text, fill = T)
  }


#' Installed Packages
#'
#' @return
#' @export
#'
#' @examples
tbl_installed_packages <-
  function() {
    data <- installed.packages() %>% as_tibble() %>% janitor::clean_names()
    all_data <- tibble()
    data$package %>%
      walk(function(x){
        x %>% message()
        d <- packageDescription(x) %>% flatten_df() %>% janitor::clean_names()
        all_data <<- all_data %>% bind_rows(d)
      })

    all_data
  }

#' Remove an Item or Folder
#'
#' @param path
#' @param recursive
#' @param force
#' @param return_message
#'
#' @return
#' @export
#'
#' @examples
delete_item <-
  function(path =  NULL,
           recursive = T,
           force = T,
           return_message = T) {
    oldwd <- getwd()
    setwd("~")
    if (length(path) == 0) {
      return(invisible())
    }
    if (return_message) {
      glue("Removing {path}") %>% message()
    }
    unlink(x = path,
           recursive = recursive,
           force = T)

    if (getwd() != oldwd) {
      setwd(oldwd)
    }
    return(invisible())
  }



# r_packages --------------------------------------------------------------

#' Tibble of CRAN Packages
#'
#' @param normalize_text if `TRUE` normalizes title descriptoon to upper
#'
#' @return
#' @export
#'
#' @examples
#' library(asbtools)
#' library(tidyverse)
#'
#' cran <- tbl_cran_packages()
#'
#' cran %>% sheldon::regex_keyword_match(text_columns = "title", keywords = "markdown", id_columns = "package")
#' cran %>% sheldon::kwic_keyword_match(text_columns = "title", id_columns = "package")
#'
tbl_cran_packages <- function(normalize_text  = T) {
  page <- "https://cran.r-project.org/web/packages/available_packages_by_date.html" %>%
    rvest::read_html()

  data <-
    page %>% rvest::html_table(header = T) %>% .[[1]] %>% janitor::clean_names() %>%
    mutate(date = lubridate::ymd(date))

  if (normalize_text) {
    data <- data %>%
      mutate(title = str_to_upper(title))
  }

  data <-
    data %>%
    mutate(
      url_cran = glue::glue(
        "https://cran.r-project.org/web/packages/{package}/index.html"
      ) %>% as.character(),
      year_released = date %>% lubridate::year() %>% as.numeric(),
      month_released = date %>% lubridate::month(label = T)
    ) %>%
    select(year_released, month_released, everything())

  data
}



# edit --------------------------------------------------------------------



#' Edit Data
#'
#' @param data data frame
#' @param file_path if not `NULL` a filepath to save the file
#' @param folder folder to save the file
#' @param file_name file name
#'
#' @return
#' @export
#'
#' @examples
#' tbl_edit(data = iris, file_path = "Desktop/test",  file_name = "iris")
#' tbl_edit(data = iris, file_path = "Desktop",  file_name = "iris")



tbl_edit <-
  function(data,
           file_path = NULL, folder = NULL, file_name = NULL) {

    if (override_common_group) {
      data %>%
        mutate(group = case_when(
          group == "common" ~ "",
          TRUE ~ group
        ))
    }

    data <- edit(data)
    data <- as_tibble(data)

    if (length(file_path) > 0) {
      "Saving data" %>% message()
      oldwd <- getwd()
      setwd("~")
      if (length(file_name) == 0) {
        file_name <- "data"
      }

      pq_write(data = data, file_path = file_path, folder = folder, file_name = file_name)

      if (getwd() != oldwd) {
        setwd(oldwd)
      }
    }


    data

  }

