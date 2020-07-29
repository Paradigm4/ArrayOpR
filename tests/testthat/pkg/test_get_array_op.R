context("Get array_op from scidb")

CONN = get_default_connection()

.name = "testarray_xyz"

.setup = function(){
  CONN$execute(sprintf("create temp array %s <a:string> [z]", .name))
}

.teardown = function(){
  CONN$execute(afl(.name | remove))
}

.setup()

# Tests begin

test_that("get array_op from array name", {
  arr = CONN$array_op_from_name(.name)
  expect_identical(arr$to_afl(), .name)
})

test_that("get array_op from afl", {
  rawAfl = "apply(list('operators'), extra, 'abc')"
  arr = CONN$array_op_from_afl(rawAfl)
  expect_identical(arr$to_afl(), rawAfl)
  expect_equal(arr$attrs, c("name", "library", "extra"))
})

test_that("get array_op from afl and stored it as array", {
  rawAfl = "apply(list('operators'), extra, 'abc')"
  name = "testarray_stored_afl"
  arr = CONN$array_op_from_afl(rawAfl, store_as_array = name)
  expect_identical(arr$to_afl(), name)
  expect_equal(arr$attrs, c("name", "library", "extra"))
  expect_equal(
    CONN$query(afl(arr | op_count))$count, 
    CONN$query(afl(rawAfl | op_count))$count 
  )
  
  CONN$execute(afl(name | remove))
})


test_that("get array_op from schema string", {
  verify = function(schema_str, name, attrs, dims){
    arr = CONN$array_op_from_schema_str(schema_str)
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


# Tests end

.teardown()
