context("Get array_op from scidb")

CONN = get_default_connection()

# Tests begin ---- 

# from array name ---- 

test_that("get array_op from array name", {
  .name = utility$random_array_name()
  CONN$execute(sprintf("create temp array %s <a:string> [z]", .name))
  
  arr = CONN$array_op_from_name(.name)
  expect_identical(arr$to_afl(), .name)
  
  try(arr$remove_self(), silent = T)
})

# from afl ----

test_that("get array_op from afl", {
  rawAfl = "apply(list('operators'), extra, 'abc')"
  arr = CONN$array_op_from_afl(rawAfl)
  expect_identical(arr$to_afl(), rawAfl)
  expect_equal(arr$attrs, c("name", "library", "extra"))
})

test_that("get array_op from afl and stored it as array manually", {
  rawAfl = "apply(list('operators'), extra, 'abc')"
  name = utility$random_array_name(prefix = "Rarrayop_tests_stored_afl")
  
  storedArr = CONN$array_op_from_stored_afl(rawAfl, name)
  retrievedArr = CONN$array_op_from_name(name)
  
  expect_identical(storedArr$to_afl(), name)
  expect_equal(
    storedArr$to_df_attrs(), 
    retrievedArr$to_df_attrs()
  )
  
  storedArr$remove_self()
})

test_that("get array_op from afl and stored it as array manually", {
  rawAfl = "apply(list('operators'), extra, 'abc')"
  name = utility$random_array_name(prefix = "Rarrayop_tests_stored_afl")
  
  CONN$execute(afl(rawAfl | store(name)))
  
  transientArr = CONN$array_op_from_afl(rawAfl)
  storedArr = CONN$array_op_from_name(name)
  
  expect_identical(transientArr$to_afl(), rawAfl)
  expect_identical(storedArr$to_afl(), name)
  expect_identical(storedArr$attrs, transientArr$attrs)
  expect_equal(
    storedArr$to_df_attrs(), 
    transientArr$to_df_attrs()
  )
  
  storedArr$remove_self()
})

# from schema string ----

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

# from uploaded data frame ----

test_that("get array_op from uploaded data frame", {
  template = CONN$array_op_from_schema_str("<a:string, b:int32, extra:bool> [z]")
  df = data.frame(a = c(
    "slashes: http://a-b/c d%20%", # double backslackes \\ cause a bug
    # 'NA not working',
    NA, # NA cannot be uploaded
    "quotes: 'a' \"|\" ", 
    "special:  [abcd]", # if \t or \n were added to the field, tests would fail
    "''"
  ), b = 1:5, z = 11:15, stringsAsFactors = F)
  
  arr = CONN$array_op_from_uploaded_df(df, template)
  
  # all matched fields are uploaded as attributes (dimensions vary with upload operators)
  expect_identical(arr$attrs, c('a', 'b', 'z'))
  expect_equal(arr$to_df_attrs(), df)
  
  arr$remove_self()
})

test_that("get array_op from uploaded data frame by merging columns", {
  template = CONN$array_op_from_schema_str("<a:string, b:int32, extra:bool> [z]")
  df = data.frame(
    a = c(
    "slashes: http://a-b/c\\d%20%",
    'notNA', # NA cannot be uploaded in non-merge mode
    "quotes: 'a' \"|\" ",
    "special: \t\n[abcd]", #  \t or \n cannot be uploaded in non-merge mode
    "''"
    ),
    # a = letters[1:5],
  b = 1:5, z = 11:15, stringsAsFactors = F)
  
  arr = CONN$array_op_from_uploaded_df(df, template, upload_by_vector = T)
  
  # all matched fields are uploaded as attributes (dimensions vary with upload operators)
  expect_identical(arr$attrs, c('a', 'b', 'z'))
  
  expect_equal(arr$to_df_attrs(), df)
})

test_that("get array_op from uploaded data frame by storing joined df columns", {
  template = CONN$array_op_from_schema_str("<a:string, b:int32, extra:bool> [z]")
  df = data.frame(a = letters[1:5], b = 1:5, z = 11:15)
  
  joinOp = CONN$array_op_from_uploaded_df(df, template, upload_by_vector = T, .temp = T)
  stored = CONN$array_op_from_stored_afl(joinOp$to_afl())
  
  expect_equal(joinOp$to_df_attrs(), df)
  expect_equal(stored$to_df_attrs(), df)
})


test_that("get array_op from uploaded data frame with GC", {
  template = CONN$array_op_from_schema_str("<a:string, b:int32, extra:bool> [z]")
  df = data.frame(a = letters[1:5], b = 1:5, z = 11:15)
  
  gc_on = function() {
    name = "Rarrayop_test_upload_array_gc_on"
    arr = CONN$array_op_from_uploaded_df(df, template, name = name, .gc = TRUE)
    expect_true(!is.null(
      CONN$array_op_from_name(name) # array with name must exist
    ))
    rm(arr)
    gc()
    expect_error(CONN$array_op_from_name(name)) # array should be removed during GC
  }
  
  gc_off = function() {
    name = "Rarrayop_test_upload_array_gc_off"
    arr = CONN$array_op_from_uploaded_df(df, template, name = name, .gc = F)
    expect_true(!is.null(
      CONN$array_op_from_name(name) # array with name must exist
    ))
    rm(arr)
    gc()
    
    retried = CONN$array_op_from_name(name)
    expect_identical(retried$to_afl(), name) # array should still exists
    
    retried$remove_self()
    expect_error(CONN$array_op_from_name(name)) # now the array should be removed 
  }
  
  gc_on()
  gc_off()
  
})

# from build literal ----

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
  result = arr$to_df_attrs() %>% dplyr::arrange(z)
  
  expect_equal(arr$row_count(), 5)
  expect_equal(result, df)
  
  # should report error if build_dim is invalid, verified by scidb show
  expect_error(CONN$array_op_from_build_literal(df, template, build_dim_spec = "i=non-sense"))
})


# Tests end ----

