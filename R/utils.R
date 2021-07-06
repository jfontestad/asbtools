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
    if (is_null(text)) {
      return(invisible())
    }

    text <- glue("\n\n{text}\n\n") %>% as.character()

    cat(text, fill = T)
  }
