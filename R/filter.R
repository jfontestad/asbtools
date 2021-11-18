#' Filter a tible
#'
#' @param data
#' @param ...
#'
#' @return
#' @export
#'
#' @examples
tbl_filter <-
   function(data, ...) {
     data %>%
       filter(...)
   }
