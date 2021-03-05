#' AZURETOOLS
#'
#' An R package for interacting with azure services.
#'
#' @docType package
#' @name azuretools
#' @importFrom digest hmac
#' @importFrom dplyr bind_rows
#' @importFrom fs as_fs_bytes file_size path_file
#' @importFrom glue glue glue_collapse
#' @importFrom httr add_headers DELETE GET PUT stop_for_status upload_file
#' @importFrom magrittr %>%
#' @importFrom mime guess_type
#' @importFrom openssl base64_decode base64_encode md5 sha256
#' @importFrom purrr map_dfr pluck
#' @importFrom readr write_file
#' @importFrom tibble tibble
#' @importFrom utils URLencode
#' @importFrom xml2 read_xml as_list
NULL
