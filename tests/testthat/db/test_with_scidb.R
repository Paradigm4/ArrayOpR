context("Repo tests with scidb connection")

# arrays loaded from the config file
CfgArrays = repo$load_arrays_from_config(config)

# Populate the empty namespace ----
test_that("No arrays in the namespace", {
  arrays = repo$load_arrays_from_scidb_namespace(NS)
  expect_identical(length(arrays), 0L)
})

test_that("Loaded arrays are created in the namespace", {
  expect_identical(length(CfgArrays), length(config$arrays))
  
  # Create arrays manually
  for(arr in CfgArrays){
    log_job(NULL, sprintf("create array %s", str(arr)))
    repo$execute(sprintf("create array %s", str(arr)), .raw = T)
  }
  
  # Ensure the newly created arrays can be loaded
  scidbCfgArrays = repo$load_arrays_from_scidb_namespace(NS)
  expect_identical(length(scidbCfgArrays), length(CfgArrays))
  for(i in 1:length(scidbCfgArrays)){
    fromConfig = CfgArrays[[i]]
    fromScidb = scidbCfgArrays[[i]]
    expect_identical(fromScidb$to_afl(), fromConfig$to_afl())
    # Schema strings from scidb may be upper-cased
    expect_identical(fromScidb$to_schema_str(), fromConfig$to_schema_str())
  }
})

test_that("Get array by raw name", {
  for(arr in CfgArrays){
    fromScidb = repo$load_array_from_scidb(arr$to_afl())
    expect_identical(str(fromScidb), str(arr))
  }
})




