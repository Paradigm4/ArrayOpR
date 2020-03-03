context("Repo tests with scidb connection")


# Populate the empty namespace ----
test_that("No arrays in the namespace", {
  arrays = repo$load_arrays_from_scidb_namespace(NS)
  expect_identical(length(arrays), 0L)
})

test_that("Loaded arrays are created in the namespace", {
  loadedArrays = repo$load_arrays_from_config(config)
  expect_identical(length(loadedArrays), length(config$arrays))
  
  # Create arrays manually
  for(arr in loadedArrays){
    log_job(NULL, sprintf("create array %s", str(arr)))
    repo$execute(sprintf("create array %s", str(arr)), .raw = T)
  }
  
  # Ensure the newly created arrays can be loaded
  scidbLoadedArrays = repo$load_arrays_from_scidb_namespace(NS)
  expect_identical(length(scidbLoadedArrays), length(loadedArrays))
  for(i in 1:length(scidbLoadedArrays)){
    fromConfig = loadedArrays[[i]]
    fromScidb = scidbLoadedArrays[[i]]
    expect_identical(fromScidb$to_afl(), fromConfig$to_afl())
    # Schema strings from scidb may be upper-cased
    expect_identical(tolower(fromScidb$to_schema_str()), fromConfig$to_schema_str())
  }
})
