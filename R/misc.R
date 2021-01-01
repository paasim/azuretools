.onAttach <- function(...) {
  ver <- utils::packageVersion("azuretools")
  packageStartupMessage("This is azuretools version ", ver)
}
