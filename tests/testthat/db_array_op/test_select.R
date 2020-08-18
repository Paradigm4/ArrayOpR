context("ArrayOp: select")

RefArray = conn$create_new_scidb_array(utility$random_array_name(), "<fa:string, fb:int32, fc:double> [da; db]", .temp = T)
DataContent = data.frame(fa = letters[1:3], fb = 1:3, fc = 3.14 * 1:3, da = 1:3, db = 11:13)
conn$array_op_from_df(DataContent)$change_schema(RefArray)$overwrite(RefArray)$execute()

df_equal = function(actual_df, expected_df) {
  expect_equal(
    actual_df %>% dplyr::arrange(!!! as.name(names(actual_df))), 
    expected_df
  )
}

test_that("select and return data frame with dimensions", {
  df_equal(RefArray$select('fa', 'fb')$to_df(), DataContent %>% dplyr::select('da', 'db', 'fa', 'fb'))
  df_equal(RefArray$select('fa', 'fb', 'da')$to_df(), DataContent %>% dplyr::select('da', 'db', 'fa', 'fb'))
})

test_that("select and return data frame of attrs", {
  df_equal(RefArray$select('fa', 'fb')$to_df_attrs(), DataContent %>% dplyr::select('fa', 'fb'))
  df_equal(RefArray$select('fa', 'fb', 'da')$to_df_attrs(), DataContent %>% dplyr::select('fa', 'fb', 'da'))
  
  cols = c('fa', 'fb')
  df_equal(RefArray$select(cols, 'da')$to_df_attrs(), DataContent %>% dplyr::select('fa', 'fb', 'da'))
})

test_that("select nothing", {
  # dimensions precede attributs
  df_equal(RefArray$select()$to_df(), DataContent %>% dplyr::select('da', 'db', dplyr::everything()))
  df_equal(RefArray$select()$to_df_attrs(), DataContent %>% dplyr::select('fa', 'fb', 'fc'))
})

test_that("error cases: select", {
  expect_error(RefArray$select("non-existent"), 'non-existent')
  expect_error(RefArray$select("non-existent", 'fa'), 'non-existent')
})
