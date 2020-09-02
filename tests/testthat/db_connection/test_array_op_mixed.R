context("Get array_from from different methods")


test_that("persistent array_op from uploaded data frame", {
  df = data.frame(
    f_str = letters[1:5], 
    f_double = c(3.14, 2.0, NA, 0, -99),
    f_bool = c(T,NA,F,NA,F),
    f_int64 = 1:5 * 10.0
  )
  template = conn$array_from_schema(
    " <f_str:string COMPRESSION 'zlib', f_int32:int32, f_int64:int64, f_bool: bool, f_double: double, f_datetime: datetime> [da=0:*:0:*]"
  )
  
  uploaded = conn$array_op_from_uploaded_df(df, template, .temp = F)
  filtered = uploaded$filter(f_double > 0)
  
  stored1 = conn$array_from_stored_afl(
    filtered$to_afl(), 
    save_array_name = random_array_name(), .temp = F, .gc = F)
  stored2 = conn$array_from_stored_afl(filtered$to_afl(), .temp = T)
  filteredDf = dplyr::filter(df, f_double > 0)
  
  expect_equal(stored1$to_df(), filteredDf)
  expect_equal(stored2$to_df(), filteredDf)
  
  # Clean up
  stored1$remove_self()
  gc() 
  # stored2 should be auto deleted after GC
  try(stored2$remove_self(), silent = T)
})

test_that("Store AFL as scidb array and return arrayOp", {
  df = data.frame(
    f_str = letters[1:5], 
    f_double = c(3.14, 2.0, NA, 0, -99),
    f_bool = c(T,NA,F,NA,F),
    f_int64 = 1:5 * 10.0
  )
  template = conn$array_from_schema(
    "<f_str:string COMPRESSION 'zlib', f_int32:int32, f_int64:int64, f_bool: bool, f_double: double, f_datetime: datetime> [da=0:*:0:*]"
  )
  
  uploaded = conn$array_op_from_uploaded_df(df, template, .temp = F)
  filtered = uploaded$filter(f_double > 0)
  
  randomName = random_array_name()
  
  stored = conn$array_from_stored_afl(
    uploaded$filter(f_double > 0)$to_afl(), 
    save_array_name = randomName, .temp = T, .gc = F
  )
  # 'overwrite' an existing array
  stored = conn$array_from_stored_afl(
    uploaded$filter(f_double < 0)$to_afl(), 
    save_array_name = randomName
  )
  
  expect_equal(
    stored$to_df(),
    dplyr::filter(df, f_double < 0)
  )
  
  # Cleanup
  stored$remove_self()
  uploaded$remove_self()
})
