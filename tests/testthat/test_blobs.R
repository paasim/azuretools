context("blob_helpers")
test_that("calculating the signature returns expected result", {
  signature <- calc_signature("123", "abc")
  expect_true(signature == "wMwN/frvI3Dk1WcRF1/jSd727Uy6JdPHoB/G72VoIg0=")
})

get_local_creds <- function(path = "") {
  creds_path <- paste0(path, "creds.txt")
  if (fs::file_exists(creds_path)) {
    # the file should contain the two lines withe the first line
    # containing the name of the storage account and the second one
    # containing the azure access key for the storage account
    readr::read_lines(creds_path) %>%
      purrr::set_names(c("storage_account", "azure_access_key"))
  } else {
    skip("No credentials provided so skipping tests using blob storage")
  }
}

context("blobs")
test_that("Listing blobs returns a tibble", {
  creds <- get_local_creds()
  tbl <- blobs_list(creds['storage_account'], "test", creds['azure_access_key'])
  expect_s3_class(tbl, "tbl")
})

file_name <- "testfile.txt"
blob_path <- "/testfolder/"
file_content <- c("this is a test file", "that contains two lines")

test_that("Uploading a blob adds the file to the blob storage", {
  creds <- get_local_creds()
  blobs_before_upload <- blobs_list(creds['storage_account'], "test", creds['azure_access_key'])

  # write a line to be added

  readr::write_lines(file_content, file_name)
  blob_upload(creds['storage_account'], "test", creds['azure_access_key'],
              file_name, blob_path)
  fs::file_delete(file_name) # remove the file locally

  blobs_after_upload <- blobs_list(creds['storage_account'], "test", creds['azure_access_key'])
  # a new file is added with the correct filename
  expect_true(nrow(blobs_before_upload) == nrow(blobs_after_upload) - 1L)
  expect_true(paste0(blob_path, file_name) %in% blobs_after_upload$path)
})

test_that("Downloading a saves the file in the correct location and in contains the expected content", {
  creds <- get_local_creds()
  blobs <- blobs_list(creds['storage_account'], "test", creds['azure_access_key'])
  skip_if_not("/testfolder/testfile.txt" %in% blobs$path,
              "Uploading a blob did not succeed")

  dest_file <- "dest.txt"
  blob_download(creds['storage_account'], "test", creds['azure_access_key'],
                paste0(blob_path, file_name), dest_file)
  expect_true(fs::file_exists(dest_file))
  expect_equal(readr::read_lines(dest_file), file_content)

  fs::file_delete(dest_file) # remove the file locally
})

test_that("Deleting a blob works as expected", {
  creds <- get_local_creds()
  blobs_before_delete <- blobs_list(creds['storage_account'], "test", creds['azure_access_key'])

  skip_if_not("/testfolder/testfile.txt" %in% blobs_before_delete$path,
              "Uploading a blob did not succeed")

  blob_delete(creds['storage_account'], "test", creds['azure_access_key'],
              paste0(blob_path, file_name))

  blobs_after_delete <- blobs_list(creds['storage_account'], "test", creds['azure_access_key'])
  expect_true(nrow(blobs_before_delete) == nrow(blobs_after_delete) + 1L)
  expect_false(paste0(blob_path, file_name) %in% blobs_after_delete$path)
})

test_that("Trying to download a non-existent file returns an error", {
  creds <- get_local_creds()
  expect_error(blob_download(creds['storage_account'], "test", creds['azure_access_key'],
                             paste0(blob_path, file_name)))
})

test_that("Trying to delete a non-existent file returns an error", {
  creds <- get_local_creds()
  expect_error(blob_delete(creds['storage_account'], "test", creds['azure_access_key'],
                           paste0(blob_path, file_name)))
})
