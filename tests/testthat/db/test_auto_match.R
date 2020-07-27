context("Subset an array with a data frame")


RefArray = testNS$create_local_arrayop(
  'reference_array',
  "<lower:string COMPRESSION 'zlib', upper:string, f_int32:int32, f_int64:int64, f_bool: bool, f_double: double> 
      [da=0:*:0:*; db=0:*:0:*]"
)

ArrayContent = data.frame(
  da=1:20, db=101:120,
  lower = letters[1:20], 
  upper = LETTERS[1:20],
  f_int32 = -20:-1, 
  f_int64 = 1:20 * 10.0, 
  f_bool = c(T,NA,F,NA,F), 
  f_double = c(3.14, 2.0, NA, 0, -99)
)

change_repo_settings = function() {
  repo = testNS$repo
  repo$setting_semi_join_filter_mode_threshold = 10
  repo$setting_build_or_upload_threshold = 20
}

change_repo_settings()


test_that("Match with filter mode on dimension", {
  testNS$reset_array_with_content(RefArray, ArrayContent)
  
  df = data.frame(da = c(3,5,8,11))
  resultOp = repo$semi_join(df, RefArray)
  resultDf = repo$query(resultOp)
  expectedDf = dplyr::filter(ArrayContent, da %in% c(3,5,8,11))
  
  expect_match(resultOp$to_afl(), "filter")
  expect_identical(resultOp$to_schema_str(), RefArray$to_schema_str())
  expect_equal(resultDf, expectedDf)
})

test_that("Match with filter mode on attribute", {
  testNS$reset_array_with_content(RefArray, ArrayContent)
  
  values = c(-10, -11, -12)
  df = data.frame(f_int32 = values)
  resultOp = repo$semi_join(df, RefArray)
  resultDf = repo$query(resultOp)
  expectedDf = dplyr::filter(ArrayContent, f_int32 %in% values)
  
  expect_match(resultOp$to_afl(), "filter")
  expect_identical(resultOp$to_schema_str(), RefArray$to_schema_str())
  expect_equal(resultDf, expectedDf)
})

test_that("Match with filter mode on two attributes", {
  testNS$reset_array_with_content(RefArray, ArrayContent)
  
  values1 = c(-20, -17, -16)
  values2 = c("no_match", "d", "e")
  df = data.frame(f_int32 = values1, lower = values2)
  resultOp = repo$semi_join(df, RefArray)
  resultDf = repo$query(resultOp)
  expectedDf = dplyr::semi_join(ArrayContent, df)
  
  expect_match(resultOp$to_afl(), "filter")
  expect_identical(resultOp$to_schema_str(), RefArray$to_schema_str())
  expect_equal(resultDf, expectedDf)
})





