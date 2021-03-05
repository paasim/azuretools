get_x_ms_blob_type <- function() {
  # see the documentation of get_canonicalized_headers
  "BlockBlob"
}

get_x_ms_date <- function() {
  # see the documentation of get_canonicalized_headers
  format(Sys.time(), format = "%a, %d %b %Y %H:%M:%S %Z", tz = "GMT")
}

get_x_ms_version <- function() {
  # see the documentation of get_canonicalized_headers
  "2020-04-08"
}

#' Canonicalized headers
#'
#' [As speficied in the documentation.](https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key).
#'
#' @param blob_type Always `"BlockBob"`, which corresponds to a new block
#' (as apposed to page blob or append blob)
#' @param date Current timestamp formatted with `"%a, %d %b %Y %H:%M:%S %Z"`
#' @param version Always `"2020-04-08"`, most recent version when the package
#' was initially implemented
#'
get_canonicalized_headers <- function(blob_type, date, version) {
  # combine the headers in the specified format
  glue("x-ms-blob-type:{blob_type}",
       "x-ms-date:{date}",
       "x-ms-version:{version}",
       .sep = "\n")
}

#' Canonicalized resource
#'
#' [As speficied in the documentation.](https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key).
#'
#' @param storage_account The name of the storage account
#' @param container_name The name of the container
#' @param rest Rest of the resource. If referring to a file, the destination file path. If listing files, the required parameters.
#'
get_canonicalized_resource <- function(storage_account, container_name, rest = "") {
  glue("/{storage_account}/{container_name}{rest}")
}

#' Get the string to be signed
#'
#' See [Constructing the signature string](https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key#constructing-the-signature-string)
#' in the documentation.
#'
#' @param canonicalized_headers Obtained with [get_canonicalized_headers()]
#' @param canonicalized_resource Obtained with [get_canonicalized_resource()]
#' @param http_method `"GET"`, `"PUT"`, `"DELETE"` etc.
#' @param content_length Length of the file to be uploaded in bytes. Otherwise (and by default) empty
#' @param content_md5 md5sum of the file to be uploaded. Otherwise (and by default) empty
#' @param content_type Content type of the file to be uploaded. Otherwise (and by default) empty
#'
get_string_to_sign <- function(canonicalized_headers, canonicalized_resource,
                               http_method = "GET", content_length = "",
                               content_md5 = "", content_type = "") {
  # for now these are assumed to be empty
  content_encoding <- ""
  content_language <- ""
  date <-  ""
  if_modified_since <- ""
  if_match <- ""
  if_n <- ""
  if_unmodified_since <- ""
  range <- ""
  glue("{http_method}",
       "{content_encoding}", "{content_language}", "{content_length}", "{content_md5}", "{content_type}",
       "{date}",
       "{if_modified_since}", "{if_match}", "{if_n}", "{if_unmodified_since}",
       "{range}",
       "{canonicalized_headers}", "{canonicalized_resource}",
       .sep = "\n")
}

#' Calculate the signature in the request
#'
#' See [Specifying the authorization header.](https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key#specifying-the-authorization-header)
#' in the documentation.
#'
#' @param string_to_sign Obtained with [get_string_to_sign()]
#' @param azure_access_key Access key to the storage account
#'
calc_signature <- function(string_to_sign, azure_access_key) {
  base64_decode(azure_access_key) %>%
    hmac(string_to_sign, algo = "sha256", raw = TRUE) %>%
    base64_encode()
}


get_authorization <- function(storage_account, signature) {
  # authorization header in the correct format
  glue("SharedKey {storage_account}:{signature}")
}

#' Construct the url in the correct format
#'
#' Constructs the url. For listing blobs adds the desired parameters and others the file path.
#'
#' @param storage_account The name of the storage account
#' @param container_name The name of the container
#' @param rest Rest of the resource. If referring to a file, the destination file path. If listing files, the required parameters.
#'
construct_url <- function(storage_account, container_name, rest = "") {
  glue("https://{storage_account}.blob.core.windows.net/{container_name}{rest}")
}


