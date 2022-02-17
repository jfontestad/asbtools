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
