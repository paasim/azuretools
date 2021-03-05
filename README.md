# azuretools

[![R build status](https://github.com/paasim/azuretools/workflows/R-CMD-check/badge.svg)](https://github.com/paasim/azuretools/actions)

The package contains some R tools for working with the API of some azure services. Currently it only contains functions for uploading, downloading, deleting and listing blobs and in storage account.

## Installation

``` r
devtools::install_github("paasim/azuretools")
```

## Example

Below is an example for uploading a blob and then listing all the blobs in the container. The package also contain functions `blob_download` and `blob_delete` that work similarly.

``` r
library(azuretools)

# upload a file 'test.txt' to a container 'blobs' in the storage account
blob_upload("storage_account", "blobs", "azure_access_key", "/test.txt")

# list blobs in the container "blobs"
blobs_list("storage_account", "blobs", "azure_access_key")
```

