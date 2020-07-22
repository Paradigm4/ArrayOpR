context("Load arrays from namespace")


test_that("No arrays in an empty namespace", {
  arrays = repo$load_arrayops_from_scidb_namespace(NS)
  expect_identical(length(arrays), 0L)
})

test_that("Load all arrayOps from a namespace", {
  new_arrayop = function(name, schema) {
    repo$get_array(sprintf("%s.%s %s", NS, name, schema))
  }
  
  aa = new_arrayop("A", "<f_str:string COMPRESSION 'zlib', f_datetime: datetime>  [da=0:*:0:*]")
  ab = new_arrayop("B", "<f_int32:int32, f_int64:int64, f_bool: bool, f_double: double>  [da=0:*:0:1;db=0:*:0:*]")
  
  localArrays = list("A" = aa, "B" = ab)
  
  # Create arrays manually
  for(arr in localArrays){
    repo$.create_array(arr)
  }
  
  # Load arrayOps from namespace NS
  dbArrays = repo$load_arrayops_from_scidb_namespace(NS)
  
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
  .remove_arrays_from_namespace()
})


test_that("Load arrayOps one-by-one from a namespace", {
  new_arrayop = function(name, schema) {
    repo$get_array(sprintf("%s.%s %s", NS, name, schema))
  }
  
  aa = new_arrayop("A", "<f_str:string COMPRESSION 'zlib', f_datetime: datetime>  [da=0:*:0:*]")
  ab = new_arrayop("B", "<f_int32:int32, f_int64:int64, f_bool: bool, f_double: double>  [da=0:*:0:1;db=0:*:0:*]")
  
  localArrays = list("A" = aa, "B" = ab)
  
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
  .remove_arrays_from_namespace()
})

