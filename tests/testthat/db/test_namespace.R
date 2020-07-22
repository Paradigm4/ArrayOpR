context("Load arrays from namespace")


test_that("No arrays in an empty namespace", {
  arrays = repo$load_arrayops_from_scidb_namespace(testNS$NS)
  expect_identical(length(arrays), 0L)
})

localArrays = (function(){
  aa = testNS$create_local_arrayop("A", "<f_str:string COMPRESSION 'zlib', f_datetime: datetime>  [da=0:*:0:*]")
  ab = testNS$create_local_arrayop("B", "<f_int32:int32, f_int64:int64, f_bool: bool, f_double: double>  [da=0:*:0:1;db=0:*:0:*]")
  list("A" = aa, "B" = ab)
})()


test_that("Load all arrayOps from a namespace", {
  
  # Create arrays manually
  for(arr in localArrays){
    repo$.create_array(arr)
  }
  
  # Load arrayOps from namespace NS
  dbArrays = repo$load_arrayops_from_scidb_namespace(testNS$NS)
  
  # Tests
  expect_identical(length(localArrays), length(dbArrays))
  for(name in names(localArrays)){
    fromLocal = localArrays[[name]]
    fromDB = dbArrays[[name]]
    expect_identical(
      toupper(str(fromLocal)), 
      toupper(str(fromDB))
    )
  }
  
  # Clean up
  testNS$cleanup_after_each_test()
})


test_that("Load arrayOps one-by-one from a namespace", {

  # Create arrays manually
  for(arr in localArrays){
    repo$.create_array(arr)
  }
  
  # Tests
  for(name in names(localArrays)){
    fromLocal = localArrays[[name]]
    # Load arrayOp from scidb
    fromDB = repo$load_arrayop_from_scidb(fromLocal$to_afl())
    expect_identical(
      toupper(str(fromLocal)), 
      toupper(str(fromDB))
    )
  }
  
  # Clean up
  testNS$cleanup_after_each_test()
})

