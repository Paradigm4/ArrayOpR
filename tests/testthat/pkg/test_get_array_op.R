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
  CONN$execute(afl(rawAfl | store(name)))
  
  transientArr = CONN$array_op_from_afl(rawAfl)
  storedArr = CONN$array_op_from_name(name)
  
  expect_identical(transientArr$to_afl(), rawAfl)
  expect_identical(storedArr$to_afl(), name)
  expect_identical(storedArr$attrs, transientArr$attrs)
  expect_equal(storedArr$row_count(), transientArr$row_count())
  
  storedArr$remove_self()
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

test_that("get array_op from uploaded data frame", {
  template = CONN$array_op_from_schema_str("new <a:string, b:int32, extra:bool> [z]")
  df = data.frame(a = letters[1:5], b = 1:5, z = 11:15)
  name = "testarray_uploaded"
  arr = CONN$array_op_from_uploaded_df(df, template, name)
  saved = CONN$array_op_from_name(name)
  
  # all matched fields are uploaded as attributes (dimensions vary with upload operators)
  expect_identical(arr$attrs, c('a', 'b', 'z'))
  expect_identical(arr$to_afl(), name)
  expect_identical(arr$to_schema_str(), saved$to_schema_str())
  expect_equal(arr$row_count(), 5)
  
  CONN$execute(afl(name | remove))
})

test_that("get array_op from build literal", {
  template = CONN$array_op_from_schema_str("new <a:string, b:int32, extra:bool> [z]")
  df = data.frame(
    b = 1:5, z = 11:15,
    a = c(
      "slashes: http://a-b/c\\d%20%",
      NA,
      "quotes: 'a' \"|\" ",
      "special: \t\n[abcd]",
      "''"
    )
  )
  
  arr = CONN$array_op_from_build_literal(df, template, build_dim_spec = "i=1:*:0:*")
  result = arr$to_df(only_attributes = T) %>% dplyr::arrange(z)
  
  expect_equal(arr$row_count(), 5)
  expect_equal(result, df)
  
  # should report error if build_dim is invalid, verified by scidb show
  expect_error(CONN$array_op_from_build_literal(df, template, build_dim_spec = "i=non-sense"))
})


# Tests end

.teardown()
