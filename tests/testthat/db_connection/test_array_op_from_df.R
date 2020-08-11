context("persistent array_op from uploaded data frame")


# from uploaded data frame ----


test_that("upload data frame: no template", {
  df = data.frame(a = letters[1:5], b = 1:5, z = 11:15 * 1.2)
  uploaded = CONN$array_op_from_uploaded_df(df, .gc = F)
  
  # uploaded data frame will have an artificial dimension
  expect_equal(uploaded$to_df_attrs(), df)
  uploaded$remove_self()
})

test_that("upload data frame: no template, by vectors", {
  df = data.frame(a = letters[1:5], b = 1:5, z = 11:15 * 1.2)
  uploaded = CONN$array_op_from_uploaded_df(df, upload_by_vector = T, .gc = F)
  
  # uploaded data frame will have an artificial dimension
  expect_equal(uploaded$to_df_attrs(), df)
  columnArrays = uploaded$.get_meta(".ref")
  for(arr in columnArrays){
    arr$remove_self()
  }
})

test_that("upload data frame with a template", {
  df = data.frame(a = letters[1:5], b = 1:5, z = 11:15)
  
  joinOp = CONN$array_op_from_uploaded_df(
    df, 
    template = "<a:string, b:int32, extra:bool> [z]", 
  )
  stored = CONN$array_op_from_stored_afl(joinOp$to_afl())
  
  expect_equal(joinOp$to_df_attrs(), df)
  expect_equal(stored$to_df_attrs(), df)
})

test_that("uploaded data frame by vectors and store the joined vectors", {
  df = data.frame(a = letters[1:5], b = 1:5, z = 11:15)
  
  joinOp = CONN$array_op_from_uploaded_df(df, "<a:string, b:int32, extra:bool> [z]", upload_by_vector = T, .temp = T)
  stored = CONN$array_op_from_stored_afl(joinOp$to_afl())
  
  expect_equal(joinOp$to_df_attrs(), df)
  expect_equal(stored$to_df_attrs(), df)
})


test_that("upload data frame with special chars", {
  df = data.frame(a = c(
    "slashes: http://a-b/c d%20%", # double backslackes \\ cause a bug
    NA, # NA is ok if we upload df as a whole instead of by vectors
    "quotes: 'a' \"|\" ", 
    "special:  [abcd]", # if \t or \n were added to the field, tests would fail
    "''"
  ), b = 1:5, z = 11:15, stringsAsFactors = F)
  
  arr = CONN$array_op_from_uploaded_df(df, template = "<a:string, b:int32, extra:bool> [z]")
  
  # all matched fields are uploaded as attributes (dimensions vary with upload operators)
  expect_identical(arr$attrs, c('a', 'b', 'z'))
  expect_equal(arr$to_df_attrs(), df)
  
  arr$remove_self()
})

test_that("upload data frame by vectors", {
  df = data.frame(
    a = c(
      "slashes: http://a-b/c\\d%20%",
      'notNA', # NA cannot be uploaded in non-merge mode
      "quotes: 'a' \"|\" ",
      "special: \t\n[abcd]", #  \t or \n cannot be uploaded in non-merge mode
      "''"
    ),
    b = 1:5, z = 11:15)
  
  arr = CONN$array_op_from_uploaded_df(df, "<a:string, b:int32, extra:bool> [z]", upload_by_vector = T)
  
  # all matched fields are uploaded as attributes (dimensions vary with upload operators)
  expect_identical(arr$attrs, c('a', 'b', 'z'))
  
  expect_equal(arr$to_df_attrs(), df)
})


test_that("upload data frame with GC setting", {
  template = "<a:string, b:int32, extra:bool> [z]"
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


test_that("upload data frame with other scidbR settings", {
  df = data.frame(
    f_str = letters[1:5], 
    f_double = c(3.14, 2.0, NA, 0, -99), 
    f_bool = c(T,NA,F,NA,F), 
    f_int64 = 1:5 * 10.0, 
    f_datetime = c('2020-03-14 01:23:45', '2000-01-01', '01/01/1999 12:34:56', as.character(Sys.time()), "2020-01-01 3:14:15")
  )
  
  schema = "<f_str:string COMPRESSION 'zlib', f_int32:int32, f_int64:int64, f_bool: bool, f_double: double, f_datetime: datetime> [da=0:*:0:*]"
  template = conn$array_op_from_schema_str(sprintf(
    "%s %s", utility$random_array_name(), schema
  ))
  
  uploaded = conn$array_op_from_uploaded_df(df, schema, .temp = F, .use_aio_input = F)
  # Template can also be a list of data types 
  uploaded2 = conn$array_op_from_uploaded_df(df, template$get_field_types(), .temp = T, .use_aio_input = T)
  
  templateMatchedDTypes = template$get_field_types(names(df), .raw = T)
  expect_identical(uploaded$get_field_types(uploaded$attrs), templateMatchedDTypes)
  expect_identical(uploaded2$get_field_types(uploaded2$attrs), templateMatchedDTypes)
  
  # R date time conversion is cubersome. We replace it with the scidb parsed values.
  dfCopy = df
  dbdf1 = uploaded$to_df_attrs()
  dbdf2 = uploaded2$to_df_attrs()
  
  dfCopy$f_datetime = dbdf1[['f_datetime']] 
  expect_equal(dbdf1, dfCopy)
  expect_equal(dbdf2, dfCopy)
  
  # Test limit function
  expect_equal(uploaded$limit(3)$to_df_attrs(), dfCopy[1:3,])
  expect_equal(uploaded$limit(3, skip=1)$to_df_attrs(), 
               data.frame(dfCopy[2:4,], row.names = NULL))
  
  # Validate number of rows
  expect_equal(uploaded$row_count(), nrow(df))
  expect_equal(uploaded2$row_count(), nrow(df))
  
  uploaded$remove_self()
  uploaded2$remove_self()
})


# from build literal ----

test_that("transient array_op from build literal", {
  # build literal is generated by ArrayOp class, all special characters are properly escaped
  # While the data frame below will work in build literal, it would NOT work in upload mode (because of bugs from SciDBR package)
  
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
  
  expect_equal( 
    arr$to_df_attrs() %>% dplyr::arrange(z), 
    df
  )
  
  # should report error if build_dim is invalid, verified by scidb show
  expect_error(CONN$array_op_from_build_literal(df, template, build_dim_spec = "i=non-sense"))
})
