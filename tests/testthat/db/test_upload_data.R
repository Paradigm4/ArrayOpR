context("Upload data to scidb")



test_that("Upload data frame to scidb", {
  df = data.frame(
    f_str = letters[1:5], 
    f_double = c(3.14, 2.0, NA, 0, -99), 
    f_bool = c(T,NA,F,NA,F), 
    f_int64 = 1:5 * 10.0, 
    f_datetime = c('2020-03-14 01:23:45', '2000-01-01', '01/01/1999 12:34:56', as.character(Sys.time()), "2020-01-01 3:14:15")
  )
  
  template = create_local_arrayop(
    "template_a", 
    "<f_str:string COMPRESSION 'zlib', f_int32:int32, f_int64:int64, f_bool: bool, f_double: double, f_datetime: datetime> [da=0:*:0:*]"
  )
  
  uploaded = repo$upload_df(df, template, temp = F, use_aio_input = F)
  # Template can also be a list of data types 
  uploaded2 = repo$upload_df(df, template$get_field_types(), temp = T, use_aio_input = T)
  
  templateMatchedDTypes = template$get_field_types(names(df), .raw = T)
  expect_identical(uploaded$get_field_types(uploaded$attrs), templateMatchedDTypes)
  expect_identical(uploaded2$get_field_types(uploaded2$attrs), templateMatchedDTypes)
  
  # Check array content
  dbdf1 = repo$query(uploaded, only_attributes=TRUE)
  dbdf2 = repo$query(uploaded2, only_attributes=TRUE)
  # R date time conversion is cubersome. We replace it with the scidb parsed values.
  dfCopy = df
  dfCopy$f_datetime = dbdf1[['f_datetime']] 
  expect_equal(dbdf1, dfCopy)
  expect_equal(dbdf2, dfCopy)
  
  # Test repo$limit function
  expect_equal(repo$limit(uploaded, 3, only_attributes=TRUE), dfCopy[1:3,])
  expect_equal(repo$limit(uploaded, 3, offset=1, only_attributes=TRUE), 
               data.frame(dfCopy[2:4,], row.names = NULL))
  
  # Validate number of rows
  expect_equal(repo$nrow(uploaded), nrow(df))
  expect_equal(repo$nrow(uploaded2), nrow(df))
  
  cleanup_after_each_test()
})
