context("Get array_op from scidb array names and schemas")

# Tests begin ---- 

# from array name ---- 

test_that("array_op from scidb array name", {
  name = random_array_name()
  arr = conn$create_new_scidb_array(name, "<a:string> [z]", .temp = T)
  expect_identical(arr$to_afl(), name)
  expect_identical(conn$array_op_from_name(name)$to_afl(), name)
  
  arr$remove_self()
})


# from schema string ----

test_that("get array_op from schema string", {
  verify = function(schema_str, name, attrs, dims){
    arr = conn$array_op_from_schema_str(schema_str)
    expect_identical(arr$to_afl(), name)
    expect_identical(arr$attrs, attrs)
    expect_identical(arr$dims, dims)
  }
  
  verify("temp<a:string, b:int32>[z]", "temp", c("a", "b"), c("z"))
  verify("temp<a:string, b:int32>[z; x=0:*:0:*]", "temp", c("a", "b"), c("z", "x"))
  # space around array name, attribute/dim does not affect schema parsing
  verify("temp <a:string, b:int32> [z]", "temp", c("a", "b"), c("z"))
  # schema without array name
  verify("<a:string, b:int32>[z]", "", c("a", "b"), c("z"))
})


