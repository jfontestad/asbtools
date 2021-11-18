#' Push Changes to Github
#'
#' @return
#' @export
#' @import stringi dplyr
#' @importFrom glue glue
#' @importFrom stringi stri_rand_strings
#' @examples
push_website_changes_to_github <-
  function() {
    word_length <- sample(x = 2:16, 1)
    string_length <- sample(1:4, 1)
    commit_message <- stri_rand_strings(n = string_length, length = word_length)

    system_text <-
      glue(' cd
                 cd Desktop/abresler.github.io/
                 ls
                 git add .
                 git commit -m "{commit_message}"
                 git push origin
                 cd
                 ')

    system(command = system_text)
  }

#' Return Tree Path
#'
#' @param path
#'
#' @return
#' @export
#'
#' @examples
#'
#' tree_path(path = "Desktop/data/usa_spending/fpds/")
#'
tree_path <-
  function(path = NULL) {
    if (length(path) == 0) {
      return(invisible())
    }
    oldwd <- getwd()
    setwd("~")
    system(glue::glue("tree {path}"))
    if (getwd() != oldwd) {
      setwd(oldwd)
    }
    invisible()
  }
