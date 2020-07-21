context("Load arrays from namespace")


test_that("No arrays in the namespace", {
  arrays = repo$load_arrayops_from_scidb_namespace(NS)
  expect_identical(length(arrays), 0L)
})

test_that("Loaded arrays are created in the namespace", {
  expect_identical(length(CfgArrays), length(config$arrays))
  
  # Create arrays manually
  for(arr in CfgArrays){
    repo$.create_array(arr)
  }
  
  # Ensure the newly created arrays can be loaded
  scidbCfgArrays = repo$load_arrayops_from_scidb_namespace(NS)
  scidbCfgArrays = scidbCfgArrays[CfgArraysRawNames]
  expect_identical(length(scidbCfgArrays), length(CfgArrays))
  for(i in 1:length(scidbCfgArrays)){
    fromConfig = CfgArrays[[i]]
    fromScidb = scidbCfgArrays[[i]]
    expect_identical(fromScidb$to_afl(), fromConfig$to_afl())
    # Schema strings from scidb may be converted to upper-case
    expect_identical(fromScidb$to_schema_str(), fromConfig$to_schema_str())
  }
  
  # Test loading arrays one-by-one 
  for(arr in CfgArrays){
    fromScidb = repo$load_arrayop_from_scidb(arr$to_afl())
    expect_identical(str(fromScidb), str(arr))
  }
  
  # Clean up
  .remove_arrays_from_namespace()
})


