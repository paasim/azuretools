#' Download a blob
#'
#' Downloads the blob speficied in `resource_path`.
#'
#' @param storage_account The name of the storage account
#' @param container_name The name of the container
#' @param azure_access_key Access key to the storage account
#' @param resource_path Path to the blob
#' @param dest_file The name of the destination file. If empty, extracts the
#' file name from `resource_path`.
#'
#' @return The result of the request is returned invisibly.
#'
#' @export
blob_download <- function(storage_account, container_name, azure_access_key, resource_path,
                          dest_file = character(0)) {

  # headers
  x_ms_date <- get_x_ms_date()
  x_ms_version <- get_x_ms_version()
  x_ms_blob_type <- get_x_ms_blob_type()

  # headers and resource URI
  canonicalized_headers <- get_canonicalized_headers(x_ms_blob_type, x_ms_date, x_ms_version)
  canonicalized_resource <- get_canonicalized_resource(storage_account, container_name, resource_path)

  # request signature encrypted with azure access key
  signature <- get_string_to_sign(canonicalized_headers, canonicalized_resource) %>%
    calc_signature(azure_access_key)
  authorization <- get_authorization(storage_account, signature)

  # request url and headers
  url <- construct_url(storage_account, container_name, resource_path)
  headers <- add_headers(`x-ms-date` = x_ms_date,
                         `x-ms-version` = x_ms_version,
                         `x-ms-blob-type` = x_ms_blob_type,
                         Authorization = authorization)
  result <- GET(url, headers)
  stop_for_status(result)

  # write the result in the destination file,
  # if dest_file
  dest_file <- if (length(dest_file) == 0L) path_file(resource_path) else dest_file
  write_file(result$content, dest_file)

  invisible(result)
}

#' Parse blob metadata
#'
#' Extracts blob metadata from a list obtained from parsing the xml result from
#' the blob service API. Used as a helper function for [blobs_list()].
#'
#' @param blob A list containing the blob metadata. (If there are no blobs,
#' the result is a tibble with 0 rows and 0 columns).
#'
blob_parse <- function(blob) {
  # / added for consistency with other functions that expect absolute file path
  tibble(path = paste0("/", blob$Name[[1]]),
         size = as_fs_bytes(blob$Properties$`Content-Length`[[1]]),
         type = blob$Properties$`Content-Type`[[1]],
         creation_time = as.POSIXct(blob$Properties$`Creation-Time`[[1]],
                    tz = "GMT", format = "%a, %d %b %Y %H:%M:%S"),
         last_modified = as.POSIXct(blob$Properties$`Last-Modified`[[1]],
                    tz = "GMT", format = "%a, %d %b %Y %H:%M:%S"))
}

empty_blob_tbl <- function() {
  # a tibble with 0 rows but the correct columns and types
  tibble(path = character(0),
         size = as_fs_bytes(character(0)),
         type = character(),
         creation_time = as.POSIXct(character(0)),
         last_modified = as.POSIXct(character(0)))
}

#' List blobs
#'
#' Lists all the blobs in the given container.
#'
#' @param storage_account The name of the storage account
#' @param container_name The name of the container
#' @param azure_access_key Access key to the storage account
#'
#' @return A tibble with information of the blobs.
#'
#' @export
blobs_list <- function(storage_account, container_name, azure_access_key) {

  # headers
  x_ms_date <- get_x_ms_date()
  x_ms_version <- get_x_ms_version()
  x_ms_blob_type <- get_x_ms_blob_type()

  # headers and query parameters
  canonicalized_headers <- get_canonicalized_headers(x_ms_blob_type, x_ms_date, x_ms_version)
  canonicalized_resource <- get_canonicalized_resource(storage_account, container_name,
                                                       "\ncomp:list\nrestype:container")

  # request signature encrypted with azure access key
  signature <- get_string_to_sign(canonicalized_headers, canonicalized_resource) %>%
    calc_signature(azure_access_key)
  authorization <- get_authorization(storage_account, signature)

  # request url and headers
  url <- construct_url(storage_account, container_name, "?comp=list&restype=container")
  headers <- add_headers(`x-ms-date` = x_ms_date,
                         `x-ms-version` = x_ms_version,
                         `x-ms-blob-type` = x_ms_blob_type,
                         Authorization = authorization)
  result <- GET(url, headers)
  stop_for_status(result)

  tbl <- read_xml(result$content) %>%
    as_list() %>%
    pluck("EnumerationResults", "Blobs") %>%
    map_dfr(blob_parse)
  if (ncol(tbl) == 0L) empty_blob_tbl() else tbl
}

#' Delete a blob
#'
#' Downloads the blob speficied in `resource_path`.
#'
#' @param storage_account The name of the storage account
#' @param container_name The name of the container
#' @param azure_access_key Access key to the storage account
#' @param path Path to the blob
#'
#' @return The result of the request is returned invisibly.
#'
#' @export
blob_delete <- function(storage_account, container_name, azure_access_key, path) {

  # headers
  x_ms_date <- get_x_ms_date()
  x_ms_version <- get_x_ms_version()
  x_ms_blob_type <- get_x_ms_blob_type()

  # headers and query parameters
  canonicalized_headers <- get_canonicalized_headers(x_ms_blob_type, x_ms_date, x_ms_version)
  canonicalized_resource <- get_canonicalized_resource(storage_account, container_name, path)

  # request signature encrypted with azure access key
  signature <- get_string_to_sign(canonicalized_headers, canonicalized_resource, http_method = "DELETE") %>%
    calc_signature(azure_access_key)
  authorization <- get_authorization(storage_account, signature)

  # request url and headers
  url <- construct_url(storage_account, container_name, path)
  headers <- add_headers(`x-ms-date` = x_ms_date,
                         `x-ms-version` = x_ms_version,
                         `x-ms-blob-type` = x_ms_blob_type,
                         Authorization = authorization)

  result <- DELETE(url, headers)
  stop_for_status(result)
}

#' Upload a blob
#'
#' Uploads the blob speficied in `source_file_path` to the `destination_path`.
#' The file name is not changed.
#'
#' @param storage_account The name of the storage account
#' @param container_name The name of the container
#' @param azure_access_key Access key to the storage account
#' @param source_file_path Path to the file to be uploaded
#' @param destination_path Destination path for the blob
#'
#' @return The result of the request is returned invisibly.
#'
#' @export
blob_upload <- function(storage_account, container_name, azure_access_key,
                        source_file_path, destination_path = "/") {

  # headers
  x_ms_date <- get_x_ms_date()
  x_ms_version <- get_x_ms_version()
  x_ms_blob_type <- get_x_ms_blob_type()

  # file metadata required for the request
  file_length <- file_size(source_file_path) %>% as.integer()
  file_type <- guess_type(source_file_path)
  file_md5 <- md5(file(source_file_path)) %>% base64_encode()

  # destination file path
  dest_file <- paste0(destination_path, path_file(source_file_path))

  # headers and query parameters
  canonicalized_headers <- get_canonicalized_headers(x_ms_blob_type, x_ms_date, x_ms_version)
  canonicalized_resource <- get_canonicalized_resource(storage_account, container_name, dest_file)

  # request signature encrypted with azure access key
  signature <- get_string_to_sign(canonicalized_headers, canonicalized_resource,
                                  http_method = "PUT", content_length = file_length,
                                  content_md5 = file_md5, content_type = file_type) %>%
    calc_signature(azure_access_key)
  authorization <- get_authorization(storage_account, signature)

  # request url and headers
  url <- construct_url(storage_account, container_name, dest_file)
  headers <- add_headers(`x-ms-date` = x_ms_date,
                         `x-ms-version` = x_ms_version,
                         `x-ms-blob-type` = x_ms_blob_type,
                         `Content-Length` = file_length,
                         `Content-MD5` = file_md5,
                         `Content-Type` = file_type,
                         Authorization = authorization)

  result <- PUT(url, headers, body = upload_file(source_file_path))
  stop_for_status(result)
}
