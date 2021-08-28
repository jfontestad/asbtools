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
