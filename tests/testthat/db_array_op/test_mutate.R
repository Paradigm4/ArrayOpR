context("ArrayOp: mutate")

schemaTemplate = conn$array_op_from_schema_str(
  "<lower:string COMPRESSION 'zlib', upper:string, f_int32:int32, f_int64:int64, f_bool: bool, f_double: double> 
      [da=0:*:0:*; db=0:*:0:*]"
)

ArrayContent = data.frame(
  da=0:4, db=10:14,
  lower = letters[1:5], 
  upper = LETTERS[1:5],
  f_int32 = -5:-1, 
  f_int64 = 1:5 * 10.0, 
  f_bool = c(T,NA,F,NA,F), 
  f_double = c(3.14, 2.0, NA, 0, -99)
)

RefArray = conn$
  array_op_from_df(ArrayContent, schemaTemplate)$
  change_schema(schemaTemplate)$
  persist(.gc = FALSE)

assert_df_match = function(result_df, expected_df) {
  expect_equal(
    dplyr::arrange(result_df, da), # scidb doesn't gurantee result data frame ordering
    expected_df # locally filtered data frames follow default ordering
  )
}

# Mutate by expression ----

test_that("Mutate an array by expression", {
  assert_df_match(
    RefArray$
      filter(is_null(f_bool))$
      mutate(list('f_bool'='iif(f_double > 1, true, false)', 'upper'="'mutated'", 'f_int32' = "strlen(lower)"))$
      to_df(),
    ArrayContent %>% 
      dplyr::filter(is.na(f_bool)) %>%
      dplyr::mutate("f_bool" = f_double > 1, "upper" = "mutated", "f_int32" = nchar(lower))
  )
})

# Mutate by another arrayOp ----

test_that("mutate: key:1-attr, update_fields: 2 attrs", {
  mutateDataSource = ArrayContent %>% 
    dplyr::filter(da %% 2 == 0) %>% 
    dplyr::mutate(f_int32 = 1:3, upper = 'changed')
  
  assert_df_match(
    RefArray$mutate(
      conn$array_op_from_df(mutateDataSource %>% dplyr::select(lower, upper, f_int32)), 
      keys = c('lower'), updated_fields = c('f_int32', 'upper')
    )$to_df(),
    mutateDataSource
  )
})

test_that("mutate: key:2-attrs, update_fields: 1 attr", {
  mutateDataSource = ArrayContent %>% 
    dplyr::filter(da %% 2 == 0) %>% 
    dplyr::mutate(f_int32 = 1:3)
  
  dataArray = conn$array_op_from_df(mutateDataSource %>% dplyr::select(lower, upper, f_int32), schemaTemplate)
  
  assert_df_match(
    RefArray$mutate(
      dataArray, keys = c('lower', 'upper'), updated_fields = c('f_int32')
    )$to_df(),
    mutateDataSource
  )
})

test_that("mutate: key:1 dim, update_fields: 1 attr", {
  mutateDataSource = ArrayContent %>% 
    dplyr::filter(da %% 2 == 0) %>% 
    dplyr::mutate(f_int32 = 1:3)
  dataArray = conn$array_op_from_df(mutateDataSource %>% dplyr::select(da, f_int32), schemaTemplate)
  
  assert_df_match(
    RefArray$mutate(
      dataArray, keys = c('da'), updated_fields = c('f_int32')
    )$to_df(),
    mutateDataSource
  )
})

test_that("mutate: key:2 dims, update_fields: 1 attr", {
  mutateDataSource = ArrayContent %>% 
    dplyr::filter(da %% 2 == 0) %>% 
    dplyr::mutate(f_int32 = 1:3)
  dataArray = conn$array_op_from_df(mutateDataSource %>% dplyr::select(da, db, f_int32), schemaTemplate)
  
  assert_df_match(
    RefArray$mutate(
      dataArray, keys = c('da', 'db'), updated_fields = c('f_int32')
    )$to_df(),
    mutateDataSource
  )
})

test_that("mutate: key: 1 dim + 1 attr, update_fields: 2 attrs", {
  mutateDataSource = ArrayContent %>% 
    dplyr::filter(da %% 2 == 0) %>% 
    dplyr::mutate(f_int32 = 1:3, upper = "changed")
  dataArray = conn$array_op_from_df(mutateDataSource %>% dplyr::select(db, lower, f_int32, upper), schemaTemplate)
  
  assert_df_match(
    RefArray$mutate(
      dataArray, keys = c('lower', 'db'), updated_fields = c('f_int32', 'upper')
    )$to_df(),
    mutateDataSource
  )
})

RefArray$remove_self()

