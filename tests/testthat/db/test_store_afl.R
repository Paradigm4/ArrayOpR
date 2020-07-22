context("Store AFL as scidb array and return arrayOp")

test_that("Store AFL as scidb array", {
  df = data.frame(
    f_str = letters[1:5], 
    f_double = c(3.14, 2.0, NA, 0, -99),
    f_bool = c(T,NA,F,NA,F),
    f_int64 = 1:5 * 10.0
  )
  template = testNS$create_local_arrayop(
    "template_a", 
    "<f_str:string COMPRESSION 'zlib', f_int32:int32, f_int64:int64, f_bool: bool, f_double: double, f_datetime: datetime> [da=0:*:0:*]"
  )
  
  uploaded = repo$upload_df(df, template, temp = F)
  filtered = uploaded$where(f_double > 0)
  
  randomName = testNS$full_array_name(sprintf('myStoredArray%s', .random_attr_name()))
  stored1 = repo$save_as_array(filtered, name = randomName, temp = F, gc = F)
  stored2 = repo$save_as_array(filtered, temp = T)
  filteredDf = dplyr::filter(df, f_double > 0)
  
  result1 = repo$query(stored1, only_attributes = T)
  result2 = repo$query(stored2, only_attributes = T)
  expect_equal(result1, filteredDf)
  expect_equal(result2, filteredDf)
  
  # Clean up
  repo$.remove_array(stored1)
  gc() # To auto remove stored2
  testNS$cleanup_after_each_test()
})

test_that("Store AFL as scidb array and return arrayOp", {
  df = data.frame(
    f_str = letters[1:5], 
    f_double = c(3.14, 2.0, NA, 0, -99),
    f_bool = c(T,NA,F,NA,F),
    f_int64 = 1:5 * 10.0
  )
  template = testNS$create_local_arrayop(
    "template_a", 
    "<f_str:string COMPRESSION 'zlib', f_int32:int32, f_int64:int64, f_bool: bool, f_double: double, f_datetime: datetime> [da=0:*:0:*]"
  )
  
  uploaded = repo$upload_df(df, template, temp = F)
  filtered = uploaded$where(f_double > 0)
  
  randomName = testNS$full_array_name(sprintf('myStoredArray%s', .random_attr_name()))
  stored = repo$save_as_array(
    uploaded$where(f_double > 0), 
    name = randomName, temp = T, gc = F
  )
  # 'overwrite' an existing array
  stored = repo$save_as_array(
    uploaded$where(f_double < 0), 
    name = randomName
  )
  
  expect_equal(
    repo$query(stored, only_attributes = T),
    dplyr::filter(df, f_double < 0)
  )
  
  # Cleanup
  repo$.remove_array(stored)
  testNS$cleanup_after_each_test()
})
