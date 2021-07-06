#' Setup a future method
#'
#' @param method if not \code{NULL} sets a `future` method options \itemize{
#' \item sequential: Resolves futures sequentially in the current R process.
#'\item transparent: Resolves futures sequentially in the current R process and assignments will be done to the calling environment. Early stopping is enabled by default.
#'\item multisession: Resolves futures asynchronously (in parallel) in separate R sessions running in the background on the same machine.
#'\item multicore: Resolves futures asynchronously (in parallel) in separate forked R processes running in the background on the same machine. Not supported on Windows.
#'\item multiprocess: If multicore evaluation is supported, that will be used, otherwise multisession evaluation will be used.
#'\item cluster: Resolves futures asynchronously (in parallel) in separate R sessions running typically on one or more machines.
#'\item remote: Resolves futures asynchronously in a separate R session running on a separate machine, typically on a different network.
#'#' }
#' @param remotes if not \code{NULL} a vector of remote IP addresses
#'
#' @return invisible
#' @export
#' @import furrr glue future
#' @examples
#' future_method("multicore")
future_method <-
  function(method = NULL, remotes = NULL) {
    if (is_null(method)) {
      "No method" %>% cat_message()
      return(invisible())
    }
    method <- str_to_lower(method)
    glue("Using {method}") %>% cat_message()

    if (!is_null(remotes)) {
      plan(sym(method), remotes = remotes)
      return(invisible())
    }
    plan(sym(method))
    return(invisible())
  }
