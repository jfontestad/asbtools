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
    if (purrr::is_null(text)) {
      return(invisible())
    }

    text <- glue::glue("\n\n{text}\n\n") %>% as.character()

    cat(text, fill = T)
  }
