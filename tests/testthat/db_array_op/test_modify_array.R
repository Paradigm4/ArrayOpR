context("ArrayOp: write ops to scidb arrays")

expect_df_equal = function(actual_df, expected_df) {
   expect_equal(
    actual_df %>% dplyr::arrange(!!! sapply(names(actual_df), as.name)), 
    expected_df
  )
}


test_that("Overwrite array", {
  Target = conn$create_new_scidb_array(utility$random_array_name(), "<fa:string, fb:int32> [da;db]")
  
  df1 = data.frame(da=1:5, db=11:15, fa = letters[1:5], fb = 1:5)
  df2 = data.frame(da=3:8, db=13:18, fa = letters[3:8], fb = 3:8)
  df2_alt = data.frame(da=3:8, db2=13:18, fa2 = letters[3:8], fb = 3:8)
  df3 = data.frame(da=1:8, db=11:18, fa = letters[1:8], fb = 1:8)
  
  conn$array_op_from_df(df1, Target, force_template_schema = T)$overwrite(Target)$execute()
  expect_df_equal(Target$to_df_all(), df1)
  
  # Field names don't matter
  conn$array_op_from_df(df2_alt, "<fa2:string,fb:int32> [da;db2]", force_template_schema = T)$overwrite(Target)$execute()
  expect_df_equal(Target$to_df_all(), df2)
  
  conn$array_op_from_df(df3, Target, force_template_schema = T)$overwrite(Target)$execute()
  expect_df_equal(Target$to_df_all(), df3)
  
  # Overwrite with source data
  Target$filter(1 == 2)$overwrite(Target)$execute()
  expect_equal(Target$row_count(), 0)
  
  Target$remove_self()
})

test_that("Update array", {
  Target = conn$create_new_scidb_array(utility$random_array_name(), "<fa:string, fb:int32> [da;db]")
  
  df1 = data.frame(da=1:5, db=11:15, fa = letters[1:5], fb = 1:5)
  df2 = data.frame(da=3:8, db2=13:18, fa2 = letters[3:8], fb = 3:8)
  df3 = data.frame(da=1:8, db=11:18, fa = letters[1:8], fb = 1:8)
  
  conn$array_op_from_df(df1, Target, force_template_schema = T)$update(Target)$execute()
  expect_df_equal(Target$to_df_all(), df1)
  
  # Field names don't matter
  conn$array_op_from_df(df2, "<fa2:string,fb:int32> [da;db2]", force_template_schema = T)$update(Target)$execute()
  expect_df_equal(Target$to_df_all(), df3)
  
  # Update with 0-row
  Target$filter(1 == 2)$update(Target)$execute()
  expect_df_equal(Target$to_df_all(), df3)
  
  Target$remove_self()
})


test_that("error cases", {
  Target = conn$array_from_schema("array_name <fa:string, fb:int32> [da;db]")
  dfArray = conn$array_op_from_df(data.frame(fa = letters[1:2], da=1:2, db=11:12), Target)
  
  # before redimension, the number of dimensions do not match between source and target
  expect_error(dfArray$update(Target), "dimensions mismatch")
  expect_error(dfArray$overwrite(Target), "dimensions mismatch")
  # redimensioned array lacks the 'fb' attribute
  expect_error(dfArray$change_schema(Target, strict = F)$update(Target), "attributes mismatch")
  expect_error(dfArray$change_schema(Target, strict = F)$overwrite(Target), "attributes mismatch")
})

