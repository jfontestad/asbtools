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
read_rda <-
  function(file = NULL,
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
           folder_path=  NULL,
           return_message = T) {
    data <- tbl_partition(data = data, partitions = partitions, return_message = return_message)
    write_rda_group_files(data = data, group = "idPartition", include_name = include_name, file_name = file_name, folder_path = folder_path, return_message = return_message)
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
  function(data){
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
           return_message = T){
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
        glue("{file_path}/{folder}") %>% as.character() %>% str_replace_all("//","/")
    } else {
      folder_path <- glue("{file_path}") %>% as.character() %>% str_replace_all("//","/")
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

  data <- eval(
    expr(data[, as.character(unlist(!!!column)), by = list(!!!clnms)])
  )

  colnames(data) <- c(as.character(clnms), as.character(column))

  if (return_tibble) {
    data <- as_tibble(data)
  }

  data
}



# arrow -------------------------------------------------------------------


#' Write Arrow Parquet Files
#'
#' @param data
#' @param file_path
#' @param folder
#' @param file_name
#' @param chunk_size
#' @param version
#' @param use_compression
#' @param compression
#' @param compression_level
#' @param use_dictionary
#' @param write_statistics
#' @param data_page_size
#' @param use_deprecated_int96_timestamps
#' @param coerce_timestamps
#' @param allow_truncated_timestamps
#' @param properties
#' @param arrow_properties
#' @param return_message
#'
#' @return
#' @export
#'
#' @examples
pq_write <-
  function(data = NULL,
           file_path = NULL,
           folder =  NULL,
           file_name = NULL,
           chunk_size = NULL,
           version = NULL,
           use_compression = T,
           compression = default_parquet_compression(),
           compression_level = 9,
           use_dictionary = NULL,
           write_statistics = NULL,
           data_page_size = NULL,
           use_deprecated_int96_timestamps = FALSE,
           coerce_timestamps = NULL,
           allow_truncated_timestamps = FALSE,
           properties = NULL,
           arrow_properties = NULL,
           return_message = T){
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
        glue("{file_path}/{folder}") %>% as.character() %>% str_replace_all("//","/")
    } else {
      folder_path <- glue("{file_path}") %>% as.character() %>% str_replace_all("//","/")
    }

    .build_folder(path = folder_path)
    oldwd <- getwd()
    setwd(folder_path)

    if (return_message) {
      glue("Saving {file_name} via parquet") %>% message()
    }

    if (use_compression) {
      compression <- "gzip"
      file_slug <-
        glue("{file_name}.gz.parquet") %>% as.character()
    } else {
      file_slug <-
        glue("{file_name}.parquet") %>% as.character()
    }


    write_parquet(
      x = data,
      sink =  file_slug,
      chunk_size = chunk_size,
      version = version,
      compression = compression,
      compression_level = compression_level,
      use_dictionary = use_dictionary,
      write_statistics = write_statistics,
      data_page_size = data_page_size,
      use_deprecated_int96_timestamps = use_deprecated_int96_timestamps,
      coerce_timestamps = coerce_timestamps,
      allow_truncated_timestamps = allow_truncated_timestamps,
      properties = properties,
      arrow_properties = arrow_properties
    )

    if (oldwd != folder_path) {
      setwd(oldwd)
    }

    return(invisible())

  }

#' Read Parquet
#'
#' @param x
#' @param ...
#'
#' @return
#' @export
#'
#' @examples
pq_read <-
  function(x, ...) {
    oldwd <- getwd()

    data <- read_parquet(x)

    if (oldwd != getwd()) {
      setwd(oldwd)
    }

    data
  }

#' Read Parquet Files
#'
#' @param path folder path
#' @param exclude_files vector of file names to exclude
#' @param add_file_name if `TRUE` appends file name
#' @param return_message if `TRUE` returns a message
#'
#' @return
#' @export
#'
#' @examples
pq_read_files <-
  function(path = NULL, exclude_files = NULL,
           add_file_name = F,
           return_message = T) {

    if (length(path) == 0) {
      stop("Enter path")
    }
    oldwd <- getwd()
    setwd("~")

    setwd(path)

    files <- list.files()[list.files() %>% str_detect(".parquet")]

    if (length(files) == 0) {
      message("No parquet files")
      return(invisible())
    }

    if (length(exclude_files) > 0) {
      exclude_slugs <- exclude_files %>% str_c(collapse = "|")

      if (return_message) {
        glue('Excluding {exclude_files %>% str_c(collapse = ", ")} parquet files') %>% message()

      }
      files  <- files[!files %>% str_detect(exclude_slugs)]
      if (length(files) == 0) {
        message("No parquet files")
        return(invisible())
      }
    }

    all_data <-
      files %>%
      map_dfr(function(x){
        file <- x %>% str_remove_all("\\.gz.parquet")
        if (return_message) {
          glue("\n\nReading {file}\n\n") %>% message()
        }
        data <- read_parquet(x)

        zip_cols <- data %>% select(matches("^zip|^fax|^phone")) %>% names()

        if (length(zip_cols) > 0) {
          data <- data %>%
            mutate_at(zip_cols, as.character)
        }
        if (add_file_name) {
          data <-
            data %>%
            mutate(name_file = as.character(file)) %>%
            select(name_file, everything())
        }

        data
      })


    if (oldwd != getwd()) {
      setwd(oldwd)
    }

    all_data
  }


#' RDA to Parquet
#'
#' @param rda_file
#' @param unique_data
#' @param sort_column
#' @param pq_path
#' @param pq_folder
#' @param pq_file
#' @param use_compression
#' @param return_message
#'
#' @return
#' @export
#'
#' @examples
rda_to_pq <-
  function(rda_file = "Desktop/data/usa_spending/fpds_atom/2018_final.rda",
           unique_data = F,
           sort_column = NULL,
           pq_path = "Desktop/data/",
           pq_folder = "fpds",
           pq_file = NULL,
           use_compression = T,
           return_message = T
           ) {
    oldwd <- getwd()
    setwd("~")

    if (return_message) {
      glue("Reading {rda_file}") %>% message()
    }

    data <- read_rda(file = rda_file)

    if (unique_data) {
      data <- unique(data)
    }


    if (length(sort_column) > 0) {
      data <- data %>%
        arrange(!!!sym(sort_column))
    }

    pq_write(
      data = data,
      file_path = pq_path,
      folder = pq_folder,
      file_name = pq_file,
      use_compression = use_compression
    )

    rm(data)
    gc()

    if (getwd() != oldwd) {
      setwd(oldwd)
    }
    return(invisible())
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
           return_message = T
           ) {

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
        glue("{file_path}/{folder}") %>% as.character() %>% str_replace_all("//","/")
    } else {
      folder_path <- glue("{file_path}") %>% as.character() %>% str_replace_all("//","/")
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
#'
#' @return
#' @export
#'
#' @examples
read_tsv_gz <- function(file = NULL) {

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

  data
}


