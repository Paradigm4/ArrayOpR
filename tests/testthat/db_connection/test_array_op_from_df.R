context("persistent array_op from uploaded data frame")


# from uploaded data frame ----


test_that("upload data frame: no template", {
  df = data.frame(a = letters[1:5], b = 1:5, z = 11:15 * 1.2)
  uploaded = conn$upload_df(df, .gc = F)
  
  # uploaded data frame will have an artificial dimension
  expect_equal(uploaded$to_df(), df)
  uploaded$remove_self()
})

test_that("various types of template", {
  templateStr = "<a:int32, b:string, c:bool> [x;y]"
  templateArray = conn$create_array(utility$random_array_name(), templateStr)
  df = data.frame(x=1:2, y=3:4, a=1:2, b=letters[1:2], c=c(T,F))
  
  verify_template = function(template) {
    a1 = conn$upload_df(df, template, .gc = F)
    expect_identical(a1$attrs, templateArray$dims_n_attrs)
    a1$remove_self()
  }
  
  verify_template(templateStr)
  verify_template(templateArray)
  verify_template(templateArray$to_afl()) # array name as template
  
  # force the array to conform with the template's schema
  a2 = conn$array_from_df(df, templateArray, force_template_schema = T)$persist(.gc = F)
  
  expect_identical(a2$attrs, templateArray$attrs)
  expect_identical(a2$dims, templateArray$dims)
  
  a2$remove_self()
  templateArray$remove_self()
  
})

test_that("upload data frame: no template, temporary", {
  df = data.frame(a = letters[1:5], b = 1:5, z = 11:15 * 1.2)
  uploaded = conn$upload_df(df, .gc = F, .temp = T)
  
  expect_identical(uploaded$array_meta_data()$temporary, TRUE)
  # uploaded data frame will have an artificial dimension
  expect_equal(uploaded$to_df(), df)
  
  uploaded$remove_self()
})

test_that("upload data frame: by a single vector", {
  df = data.frame(a = letters[1:5])
  uploaded = conn$upload_df(df, upload_by_vector = T, .gc = F)
  
  # uploaded data frame will have an artificial dimension
  expect_equal(uploaded$to_df(), df)
  
  uploaded$remove_self()
})

test_that("upload data frame: no template, by vectors", {
  df = data.frame(a = letters[1:5], b = 1:5, z = 11:15 * 1.2)
  uploaded = conn$upload_df(df, upload_by_vector = T, .gc = F)
  
  # uploaded data frame will have an artificial dimension
  expect_equal(uploaded$to_df(), df)
  columnArrays = uploaded$.get_meta(".ref")
  for(arr in columnArrays){
    arr$remove_self()
  }
})

test_that("upload data frame with a template", {
  df = data.frame(a = letters[1:5], b = 1:5, z = 11:15)
  
  joinOp = conn$upload_df(
    df, 
    template = "<a:string, b:int32, extra:bool> [z]", 
  )
  stored = conn$array_from_stored_afl(joinOp$to_afl())
  
  expect_equal(joinOp$to_df(), df)
  expect_equal(stored$to_df(), df)
})

test_that("uploaded data frame by vectors and store the joined vectors", {
  df = data.frame(a = letters[1:5], b = 1:5, z = 11:15)
  
  joinOp = conn$upload_df(df, "<a:string, b:int32, extra:bool> [z]", upload_by_vector = T, .temp = T)
  stored = conn$array_from_stored_afl(joinOp$to_afl())
  
  expect_equal(joinOp$to_df(), df)
  expect_equal(stored$to_df(), df)
})


test_that("upload data frame with special chars", {
  df = data.frame(a = c(
    "slashes: http://a-b/c d%20%", # double backslackes \\ cause a bug
    NA, # NA is ok if we upload df as a whole instead of by vectors
    "quotes: 'a' \"|\" ", 
    "special:  [abcd]", # if \t or \n were added to the field, tests would fail
    "''"
  ), b = 1:5, z = 11:15, stringsAsFactors = F)
  
  arr = conn$upload_df(df, template = "<a:string, b:int32, extra:bool> [z]")
  
  # all matched fields are uploaded as attributes (dimensions vary with upload operators)
  expect_identical(arr$attrs, c('a', 'b', 'z'))
  expect_equal(arr$to_df(), df)
  
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
  
  arr = conn$upload_df(df, "<a:string, b:int32, extra:bool> [z]", upload_by_vector = T)
  
  # all matched fields are uploaded as attributes (dimensions vary with upload operators)
  expect_identical(arr$attrs, c('a', 'b', 'z'))
  
  expect_equal(arr$to_df(), df)
})


test_that("upload data frame with GC setting", {
  template = "<a:string, b:int32, extra:bool> [z]"
  df = data.frame(a = letters[1:5], b = 1:5, z = 11:15)
  
  gc_on = function() {
    name = "Rarrayop_test_upload_array_gc_on"
    arr = conn$upload_df(df, template, name = name, .gc = TRUE)
    expect_true(!is.null(
      conn$array(name) # array with name must exist
    ))
    rm(arr)
    gc()
    expect_error(conn$array(name)) # array should be removed during GC
  }
  
  gc_off = function() {
    name = "Rarrayop_test_upload_array_gc_off"
    arr = conn$upload_df(df, template, name = name, .gc = F)
    expect_true(!is.null(
      conn$array(name) # array with name must exist
    ))
    rm(arr)
    gc()
    
    retried = conn$array(name)
    expect_identical(retried$to_afl(), name) # array should still exists
    
    retried$remove_self()
    expect_error(conn$array(name)) # now the array should be removed 
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
    f_datetime = c('2020-03-14 01:23:45', '2000-01-01 0:0:0', '1999-12-3 12:34:56', as.character(Sys.time()), "2020-01-01 3:14:15")
  )
  
  schema = "<f_str:string COMPRESSION 'zlib', f_int32:int32, f_int64:int64, f_bool: bool, f_double: double, f_datetime: datetime> [da=0:*:0:*]"
  template = conn$array_from_schema(sprintf(
    "%s %s", random_array_name(), schema
  ))
  
  uploaded = conn$upload_df(df, schema, .temp = F, .use_aio_input = F)
  # Template can also be a list of data types 
  uploaded2 = conn$upload_df(df, template$.private$get_field_types(), .temp = T, .use_aio_input = T)
  
  templateMatchedDTypes = template$.private$get_field_types(names(df), .raw = T)
  expect_identical(uploaded$.private$get_field_types(uploaded$attrs), templateMatchedDTypes)
  expect_identical(uploaded2$.private$get_field_types(uploaded2$attrs), templateMatchedDTypes)
  
  # R date time conversion is cubersome. We replace it with the scidb parsed values.
  df = dplyr::mutate(df, f_datetime = as.POSIXct(f_datetime))
  dbdf1 = uploaded$to_df()
  dbdf2 = uploaded2$to_df()
  
  expect_equal(dbdf1, df)
  expect_equal(dbdf2, df)
  
  # Validate number of rows
  expect_equal(uploaded$row_count(), nrow(df))
  expect_equal(uploaded2$row_count(), nrow(df))
  
  uploaded$remove_self()
  uploaded2$remove_self()
})

test_that("Error cases: upload data frames", {
  expect_error(conn$upload_df(list()))
  expect_error(conn$upload_df(data.frame()))
  expect_error(conn$upload_df(data.frame(a=1:2), template = "<m:string> [z]"))
})

# from build literal ----

test_that("transient array_op from build literal", {
  # build literal is generated by ArrayOp class, all special characters are properly escaped
  # While the data frame below will work in build literal, it would NOT work in upload mode (because of bugs from SciDBR package)
  
  #template = conn$array_from_schema("new <a:string, b:int32, extra:bool> [z]")
  template = "new <a:string, b:int32, extra:bool> [z]"
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
  
  arr = conn$compile_df(df, template, build_dim_spec = "i=1:*:0:*")
  arr2 = conn$compile_df(df, build_dim_spec = "i=1:*:0:*")
  
  expect_equal(arr$to_df() %>% dplyr::arrange(z), df)
  expect_equal(arr2$to_df() %>% dplyr::arrange(z), df)
  
})

test_that("Error case: invalid dimension specs in build_literal", {
  # should report error if build_dim is invalid, verified by scidb show
  expect_error(conn$compile_df(data.frame(a=1:10), build_dim_spec = "i=non-sense"))
})

# From data frame auto mode ----

test_that("auto determine whether to upload or build", {
  dataFrame = data.frame(a=1:10, b=1:10*3.14, c=letters[1:10], d=c(T,F), e=as.POSIXct("2020/1/23 3:45"))
  
  # ScidbR has a bug for uploading data frames with a datetime column
  dataFrame2 = data.frame(a=1:10, b=1:10*3.14, c=letters[1:10], d=c(T,F))
  
  arrBuild = conn$array_from_df(dataFrame, build_or_upload_threshold = 5000)
  arrUpload = conn$array_from_df(dataFrame2, build_or_upload_threshold = 10)
  
  expect_true(grepl("build", arrBuild$to_afl()))
  expect_true(!grepl("build", arrUpload$to_afl()))
  
  expect_equal(arrBuild$to_df(), dataFrame)
  expect_equal(arrUpload$to_df(), dataFrame2)
})
