.build_folder <-
  function(path = "Desktop/abresler.github.io/trelliscopes/jinkie/otr/kaute") {
    oldwd <- getwd()
    setwd("~")

    folder_exists <-
      dir.exists(paths = path)

    if (folder_exists) {
      setwd(oldwd)
      return(invisible())
    }

    parts <- path %>% str_split("/") %>% flatten_chr()

    seq_along(parts) %>%
      map(function(x) {
        if (x == 1) {
          directory <- parts[x]
          if (!dir.exists(directory)) {
            dir.create(directory)
          }
          return(invisible())
        }
        directory <- parts[1:x] %>% str_c(collapse = '/')
        if (!dir.exists(directory)) {
          dir.create(directory)
        }
        return(invisible())
      })

    setwd(oldwd)
    return(invisible())
  }

#' Build Set of Folders
#'
#' @param paths
#'
#' @return
#' @export
#'
#' @examples
build_folders <-
  function(paths = NULL) {
    if (length(paths) == 0) {
      return(invisible())
    }
    paths %>%
      walk(function(x){
        .build_folder(path = x)
      })
  }


# rda ---------------------------------------------------------------------

.import_rda_file <-
  function(file = NULL,
           return_tibble = TRUE) {
    if (length(file) == 0) {
      stop("Please enter a file path")
    }

    env <- new.env()
    nm <- load(file, env)[1]
    if (return_tibble) {
      data <-
        env[[nm]] %>%
        dplyr::as_tibble()
    } else {
      data <-
        env[[nm]]
    }
    data
  }

.curl_url <-
  function(url = "https://github.com/abresler/FRED_Dictionaries/blob/master/data/fred_series_data.rda?raw=true",
           return_tibble = TRUE) {
    con <-
      url %>%
      curl()

    data <-
      con %>%
      .import_rda_file(return_tibble = return_tibble)
    close(con)
    return(data)
  }

#' Read RDA File
#'
#' @param file
#' @param return_tibble
#'
#' @return
#' @export
#'
#' @examples
#' library(asbtools)
#' library(tidyverse)
#' setwd("~)
#' read_rda(file = "Desktop/abresler.github.io/r_packages/govtrackR/data/all_agencies.rda", snake_names = T)
#' read_rda(file = "Desktop/abresler.github.io/r_packages/govtrackR/data/all_agencies.rda", to_arrow_table = T, snake_names = T)
#'
#'
read_rda <-
  function(file = NULL,
           snake_names = F,
           to_arrow_table = F,
           return_tibble = TRUE) {
    if (length(file) == 0) {
      stop("Please enter a file")
    }
    is_html <-
      file %>% str_detect("http")

    if (is_html) {
      data <- .curl_url(url = file, return_tibble = return_tibble)
    } else {
      data <-
        .import_rda_file(file = file, return_tibble = return_tibble)
    }

    if (snake_names) {
      data <- data %>%
        janitor::clean_names()
    }

    if (to_arrow_table) {
      data <- tbl_arrow(data = data)
    }

    data
  }



#' Partition tbl
#'
#' @param data
#' @param partitions
#' @param return_message
#'
#' @return
#' @export
#'
#' @examples
tbl_partition <-
  function(data,
           partitions = 10,
           return_message = T) {
    data <-
      data %>%
      mutate(idRow = 1:n(),
             idPartition = idRow %>% ntile(n = partitions)) %>%
      select(idPartition, everything()) %>%
      select(-idRow)
    if (return_message) {
      mean_row <- nrow(data) %/% partitions
      size_p <-
        data %>% filter(idPartition == 1) %>% select(-idPartition) %>% object.size()
      glue(
        "Creating {partitions} partitions of {mean_row} rows & {ncol(data)} columns with an average size of {size_p/1000} KB"
      ) %>% message()
    }



    data
  }

#' Title
#'
#' @param data
#' @param group
#' @param include_name
#' @param file_name
#' @param folder_path
#' @param return_message
#'
#' @return
#' @export
#'
#' @examples
write_rda_group_files <-
  function(data = NULL,
           group = NULL,
           include_name = T,
           file_name = NULL,
           folder_path = NULL,
           return_message = T) {
    if (length(data) == 0) {
      "Enter data" %>% message()
      return(invisible())
    }

    if (length(group) == 0) {
      "Enter group" %>% message()
      return(invisible())
    }

    if (length(folder_path) == 0) {
      "Enter folder" %>% message()
      return(invisible())
    }

    if (length(file_name) == 0) {
      "Enter file" %>% message()
      return(invisible())
    }
    groups <- data %>% pull(group) %>% unique()
    is_num <- class(groups) %>% str_detect("integer|numeric")
    if (include_name) {
      folder_path <-
        glue("{folder_path}/{file_name}")
    }

    .build_folder(path = folder_path)
    oldwd <- getwd()
    setwd("~")
    setwd(folder_path)
    groups %>%
      walk(function(val) {
        d <- filter(data, !!sym(group) == val)
        file <- glue("{file_name}_{val}.rda") %>% as.character()
        if (return_message) {
          glue("Saving {folder_path}/{file}") %>% message()
        }
        d %>%
          select(-one_of(group)) %>%
          save(file = file)
      })

    if (oldwd != getwd()) {
      setwd(oldwd)
    }
    glue("Finished writing {file_name}") %>% message()
  }

#' Write partioned RDA files
#'
#' @param data
#' @param partitions number of partitions - defaults to 10
#' @param include_name
#' @param file_name
#' @param folder_path
#' @param return_message
#'
#' @return
#' @export
#'
#' @examples
write_partitioned_rda <-
  function(data = NULL,
           partitions = 10,
           include_name = T,
           file_name = NULL,
           folder_path =  NULL,
           return_message = T) {
    data <-
      tbl_partition(data = data,
                    partitions = partitions,
                    return_message = return_message)
    write_rda_group_files(
      data = data,
      group = "idPartition",
      include_name = include_name,
      file_name = file_name,
      folder_path = folder_path,
      return_message = return_message
    )
  }


#' Scramble df
#'
#' @param data
#'
#' @return
#' @export
#' @import dplyr
#'
#' @examples
tbl_scramble <-
  function(data) {
    as_tibble(data[sample(nrow(data)), sample(ncol(data))])
  }


#' Write single RDA file
#'
#' @param data
#' @param file_path
#' @param folder
#' @param file_name
#' @param return_message
#'
#' @return
#' @import glue dplyr
#' @export
#'
#' @examples
write_rda <-
  function(data = NULL,
           file_path = NULL,
           folder =  NULL,
           file_name = NULL,
           return_message = T) {
    if (length(data) == 0) {
      "Enter data" %>% message()
      return(invisible())
    }


    if (length(file_path) == 0) {
      "Enter file path" %>% message()
      return(invisible())
    }

    if (length(file_name) == 0) {
      "Enter file name" %>% message()
      return(invisible())
    }

    if (length(folder) > 0) {
      folder_path <-
        glue("{file_path}/{folder}") %>% as.character() %>% str_replace_all("//", "/")
    } else {
      folder_path <-
        glue("{file_path}") %>% as.character() %>% str_replace_all("//", "/")
    }

    .build_folder(path = folder_path)
    oldwd <- getwd()
    setwd(folder_path)
    file_slug <- glue("{file_name}.rda")

    if (return_message) {
      glue("Saving {file_name}") %>% message()
    }

    save(data, file = file_slug)

    if (oldwd != folder_path) {
      setwd(oldwd)
    }

    return(invisible())

  }


#' Title
#'
#' @param data
#' @param column
#' @param return_tibble
#'
#' @return
#' @export
#' @import rlang data.table dplyr tibble
#'
#' @examples
unnest_dt_list <- function(data, column, return_tibble = T) {
  data <- as.data.table(data)

  column <- ensyms(column)

  clnms <- syms(setdiff(colnames(data), as.character(column)))

  data <- as.data.table(data)

  data <-
    eval(expr(data[, as.character(unlist(!!!column)), by = list(!!!clnms)]))

  colnames(data) <- c(as.character(clnms), as.character(column))

  if (return_tibble) {
    data <- as_tibble(data)
  }

  data
}

#' Read RDA files
#'
#' @param path a file pah
#' @param snake_names
#' @param to_arrow_table
#'
#' @return a \code{tibble}
#' @export
#'
#' @examples
read_rda_files <-
  function(path = "Desktop/abresler.github.io/r_packages/govtrackR/data/nsf/nsf_grants/",
           snake_names = F,
           to_arrow_table = T) {

    if (length(path) == 0) {
      return(invisible())
    }


    old_wd <- getwd()
    setwd(path)
    rda_files <- list.files()[list.files() %>% str_detect(".rda")]
    all_data <-
      rda_files %>%
      map_dfr(function(file) {
        glue("Reading {file}") %>% message()
        read_rda(
          file = file,
          return_tibble = T,
          snake_names = F,
          to_arrow_table = F
        )
      })

    if (getwd() != old_wd) {
      setwd(old_wd)
    }

    if (snake_names) {
      all_data <- all_data %>% janitor::clean_names()
    }

    if (to_arrow_table) {
      all_data <- tbl_arrow(data = all_data)
    }
    all_data
  }



# tsv_gz ------------------------------------------------------------------


#' Write TSV.gz file
#'
#' @param data
#' @param file_path
#' @param folder
#' @param file_name
#' @param return_message
#'
#' @return
#' @export
#'
#' @examples
write_tsv_gz <-
  function(data = NULL,
           file_path = NULL,
           folder =  NULL,
           file_name = NULL,
           return_message = T) {
    if (length(data) == 0) {
      "Enter data" %>% message()
      return(invisible())
    }


    if (length(file_path) == 0) {
      "Enter file path" %>% message()
      return(invisible())
    }

    if (length(file_name) == 0) {
      "Enter file name" %>% message()
      return(invisible())
    }

    if (length(folder) > 0) {
      folder_path <-
        glue("{file_path}/{folder}") %>% as.character() %>% str_replace_all("//", "/")
    } else {
      folder_path <-
        glue("{file_path}") %>% as.character() %>% str_replace_all("//", "/")
    }

    .build_folder(path = folder_path)
    oldwd <- getwd()
    setwd("~")
    setwd(folder_path)

    if (return_message) {
      glue("Saving {file_name} via tsv.gz") %>% message()
    }
    tsv_file <- glue("{file_name}.tsv.gz")
    readr::write_csv(data, tsv_file)

    if (getwd() != oldwd) {
      setwd(oldwd)
    }
    return(invisible())

  }

#' Read TSV.gz file
#'
#' @param file file path
#' @param snake_names
#' @param to_arrow_table
#'
#' @return
#' @export
#'
#' @examples
#' library(tidyverse)
#' library(asbtools)
#' read_tsv_gz(file = "Desktop/abresler.github.io/r_packages/govtrackR/data/thousand_talents.tsv.gz", to_arrow_table = T) %>% count(name_sponsor, sort = T) %>% collect()
#'
#'
read_tsv_gz <-
  function(file = NULL, snake_names = T,
           to_arrow_table = F) {
  if (length(file) == 0) {
    message("Enter path")
    return(invisible())
  }

  oldwd <- getwd()
  setwd("~")
  data <- vroom::vroom(file)

  if (getwd() != oldwd) {
    setwd(oldwd)
  }

  if (snake_names) {
    data <- data %>%
      janitor::clean_names()
  }

  if (to_arrow_table) {
    data <- tbl_arrow(data = data)
  }


  data
}


#' Read TSV
#'
#' @param path
#' @param snake_names
#' @param to_arrow_table
#'
#' @return
#' @export
#'
#' @examples
read_tsv_gz_files <-
  function(path = NULL,
           snake_names = T,
           to_arrow_table = F) {
    if (length(path) == 0) {
      message("Enter file path")
    }
    oldwd <- getwd()
    setwd("~")
    setwd(path)

    files <-
      list.files()[list.files() %>% str_detect("tsv.gz|csv")]

    if (length(files) == 0) {
      message("No TSV or csv files")
    }
    all_data <-
      files %>% map_dfr( ~ {
      vroom(file = ., show_col_types = FALSE)
    })

    if (getwd() != oldwd) {
      setwd(oldwd)
    }

    if (snake_names) {
      all_data <- all_data %>%
        janitor::clean_names()
    }

    if (to_arrow_table) {
      all_data <- all_data %>% tbl_arrow()
    }

    all_data
  }
