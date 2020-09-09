context("ArrayOp filter")


schemaTemplate = conn$array_from_schema(
  "<text:string COMPRESSION 'zlib', upper:string, f_int32:int32, f_int64:int64, f_bool: bool, f_double: double> 
      [da=0:*:0:*; db=0:*:0:*]"
)

randomText = c(
  "Lorem ipsum dolor sit amet, consectetur adipiscing elit",
  "Integer [sit] amet 'ex' sagittis, pulvinar dui eget, ullamcorper dolor",
  "Aliquam ac 'ipsum' == + mollis urna",
  " eu enim | sed justo ## ",
  "Aenean ! 'libero' (eleifend), at tincidunt quam porttitor",
  "Aenean \tmolestie \\ urna non ipsum tristique pellentesque",
  "Donec ftp://aliquet o\\sed elementum erat",
  "Nunc http://eval.info/%20%",
  "Nunc http://eval.info/%20%fal_abc?+item1+item2",
  NA,
  "Aliquam-varius-dui-semper",
  "Sed \"tempus 'quote' \"felis a tristique dignissim",
  "Duis finibus quam \"vitae vestibulum suscipit",
  "Cras (condimentum) sapien vitae augue eleifend semper",
  "Vivamus ut gravida",
  "Aenean auctor ex tempus orci egestas",
  "Proin suscipit feit, id auctor ante ultrices",
  "partial match list",
  "exact match",
  "dictum vulputate"
)

dfArraySource = data.frame(
  da=1:20, db=101:120,
  text = randomText, 
  upper = LETTERS[1:20],
  f_int32 = -20:-1, 
  f_int64 = 1:20 * 10.0, 
  f_bool = c(T,NA,F,NA,F), 
  f_double = c(3.14, 2.0, NA, 0, -99)
)

RefArray = conn$
  array_from_df(dfArraySource, schemaTemplate, force_template_schema = TRUE)$
  persist(.gc = FALSE)

assert_df_match = function(result_df, expected_df) {
  expect_equal(
    dplyr::arrange(result_df, da), # scidb doesn't gurantee result data frame ordering
    expected_df # locally filtered data frames follow default ordering
  )
}

test_that("filter numeric fields", {
  assert_df_match(
    RefArray$filter(da >= 10, f_double > 0)$to_df_all(),
    dplyr::filter(dfArraySource, da >= 10, f_double > 0)
  )
})

test_that("filter string fields: exact, starts_with, ends_with", {
  assert_df_match(
    RefArray$filter(text == 'exact match')$to_df_all(),
    dplyr::filter(dfArraySource, text == 'exact match')
  )
  
  assert_df_match(
    RefArray$filter(text %starts_with% 'A')$to_df_all(),
    dplyr::filter(dfArraySource, grepl("^[aA]", text))
  )
  
  assert_df_match(
    RefArray$filter(text %ends_with% 'e')$to_df_all(),
    dplyr::filter(dfArraySource, grepl("[eE]$", text))
  )
})

test_that("filter string fields: contains", {
  assert_df_match(
    RefArray$filter(text %contains% 'sit')$to_df_all(),
    dplyr::filter(dfArraySource, grepl("sit", text))
  )
  assert_df_match(
    RefArray$filter(text %contains% '/')$to_df_all(),
    dplyr::filter(dfArraySource, grepl("/", text))
  )
  assert_df_match(
    RefArray$filter(text %contains% "'")$to_df_all(),
    dplyr::filter(dfArraySource, grepl("'", text))
  )
  assert_df_match(
    RefArray$filter(text %contains% "(")$to_df_all(),
    dplyr::filter(dfArraySource, grepl("\\(", text))
  )
  assert_df_match(
    RefArray$filter(text %contains% "\\")$to_df_all(),
    dplyr::filter(dfArraySource, grepl("\\\\", text))
  )
})

test_that("filter string fields: like", {
  assert_df_match(
    RefArray$filter(text %like% '.*sit.*')$to_df_all(),
    dplyr::filter(dfArraySource, grepl("sit", text))
  )
  assert_df_match(
    RefArray$filter(text %like% '.*[eo]r')$to_df_all(),
    dplyr::filter(dfArraySource, grepl("[eo]r$", text))
  )
  assert_df_match(
    RefArray$filter(text %like% '.*(ftp|http).*')$to_df_all(),
    dplyr::filter(dfArraySource, grepl("ftp|http", text))
  )
})

test_that("filter NA", {
  assert_df_match(
    RefArray$filter(is_null(text))$to_df_all(),
    dplyr::filter(dfArraySource, is.na(text))
  )
  assert_df_match(
    RefArray$filter(not_null(text))$to_df_all(),
    dplyr::filter(dfArraySource, !is.na(text))
  )
  assert_df_match(
    RefArray$filter(is_null(f_double))$to_df_all(),
    dplyr::filter(dfArraySource, is.na(f_double))
  )
  assert_df_match(
    RefArray$filter(not_null(f_double))$to_df_all(),
    dplyr::filter(dfArraySource, !is.na(f_double))
  )
})

test_that("filter %in% %not_in%", {
  letterOptions = c('A', 'C', 'E', 'G', 'a', 'z')
  numOptions = c(1:5, 11,13, 100:102)
  
  assert_df_match(
    RefArray$filter(upper %in% !!letterOptions)$to_df_all(),
    dplyr::filter(dfArraySource, upper %in% letterOptions)
  )
  assert_df_match(
    RefArray$filter(upper %not_in% !!letterOptions)$to_df_all(),
    dplyr::filter(dfArraySource, ! upper %in% letterOptions)
  )
  
  assert_df_match(
    RefArray$filter(da %in% !!numOptions)$to_df_all(),
    dplyr::filter(dfArraySource, da %in% numOptions)
  )
  
  assert_df_match(
    RefArray$filter(da %not_in% !!numOptions)$to_df_all(),
    dplyr::filter(dfArraySource, ! da %in% numOptions)
  )
  
})

test_that("filter bool fields", {
  assert_df_match(
    RefArray$filter(f_bool)$to_df_all(),
    dplyr::filter(dfArraySource, f_bool)
  )
  assert_df_match(
    RefArray$filter(!f_bool)$to_df_all(),
    dplyr::filter(dfArraySource, !f_bool)
  )
  assert_df_match(
    RefArray$filter(f_bool == TRUE)$to_df_all(),
    dplyr::filter(dfArraySource, f_bool)
  )
  assert_df_match(
    RefArray$filter(f_bool == FALSE)$to_df_all(),
    dplyr::filter(dfArraySource, !f_bool)
  )
})

test_that("filter by scidb functions", {
  assert_df_match(
    RefArray$filter(strlen(text) >= 40)$to_df_all(),
    dplyr::filter(dfArraySource, nchar(text) >= 40)
  )
})

test_that("filter by compound expressions", {
  assert_df_match(
    RefArray$filter(da < 5 || db >= 110)$to_df_all(),
    dplyr::filter(dfArraySource, da < 5 | db >= 110)
  )
  assert_df_match(
    RefArray$filter(da < 5 | db >= 110)$to_df_all(),
    dplyr::filter(dfArraySource, da < 5 | db >= 110)
  )
  assert_df_match(
    RefArray$filter(da < 5 | (db >= 110 & strlen(text) < 40))$to_df_all(),
    dplyr::filter(dfArraySource, da < 5 | (db >= 110 & nchar(text) < 40))
  )
  
  assert_df_match(
    RefArray$filter(db >= 110 & strlen(text) < 40)$to_df_all(),
    dplyr::filter(dfArraySource, db >= 110, nchar(text) < 40)
  )
})

RefArray$remove_array()
