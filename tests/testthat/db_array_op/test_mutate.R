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
  sortedDf = if('da' %in% names(result_df)) 
    dplyr::arrange(result_df, da) else if('upper' %in% names(result_df))
      dplyr::arrange(result_df, "upper") else 
        result_df
  expect_equal(
    sortedDf, # scidb doesn't gurantee result data frame ordering
    expected_df # locally filtered data frames follow default ordering
  )
  
  # tryCatch(expect_equal(
  #   sortedDf, # scidb doesn't gurantee result data frame ordering
  #   expected_df # locally filtered data frames follow default ordering
  # ), error = browser())
  
}

# Mutate new ----

test_that("Mutate an array by expression", {
  assert_df_match(
    RefArray$
      filter(is_null(f_bool))$
      mutate(f_bool='iif(f_double > 1, true, false)', upper="'mutated'", f_int32 = "strlen(lower)")$
      to_df(),
    ArrayContent %>% 
      dplyr::filter(is.na(f_bool)) %>%
      dplyr::mutate("f_bool" = f_double > 1, "upper" = "mutated", "f_int32" = nchar(lower))
  )

  # New fields generated
  assert_df_match(
    RefArray$
      filter(is_null(f_bool))$
      mutate(lower_len = "strlen(lower)")$
      to_df(),
    ArrayContent %>% 
      dplyr::filter(is.na(f_bool)) %>%
      dplyr::mutate("lower_len" = nchar(lower))
  )

  # New fields generated and existing fields removed by NULL
  assert_df_match(
    RefArray$
      filter(is_null(f_bool))$
      mutate(lower_len = "strlen(lower)", f_bool = NULL, f_int64 = NULL)$
      to_df(),
    ArrayContent %>% 
      dplyr::filter(is.na(f_bool)) %>%
      dplyr::mutate(f_bool = NULL, f_int64 = NULL, "lower_len" = nchar(lower))
  )
})

test_that("mutate error cases", {
  expect_error(
    RefArray$
      filter(is_null(f_bool))$
      mutate("da" = "1", upper="'mutated'", f_int32 = "strlen(lower)"),
    "da"
  )
  expect_error(
    RefArray$
      filter(is_null(f_bool))$
      mutate("f_bool", upper="'mutated'", f_int32 = "strlen(lower)"),
    "f_bool"
  )
})

# transmute ----

test_that("transmute does not preserve old fields", {
  # transmute an array will keep the dimensions
  assert_df_match(
    RefArray$
      filter(is_null(f_bool))$
      transmute("f_double", upper="'mutated'", f_int32 = "strlen(lower)")$
      to_df(),
    ArrayContent %>% 
      dplyr::filter(is.na(f_bool)) %>%
      dplyr::transmute(da, db, f_double, "upper" = "mutated", "f_int32" = nchar(lower))
  )
  assert_df_match(
    RefArray$
      filter(is_null(f_bool))$
      transmute("f_double"="f_double", upper="'mutated'", f_int32 = "strlen(lower)")$
      to_df(),
    ArrayContent %>% 
      dplyr::filter(is.na(f_bool)) %>%
      dplyr::transmute(da, db, f_double, "upper" = "mutated", "f_int32" = nchar(lower))
  )
  assert_df_match(
    RefArray$
      filter(is_null(f_bool))$
      transmute(f_bool='iif(f_double > 1, true, false)', upper="'mutated'", f_int32 = "strlen(lower)")$
      to_df(),
    ArrayContent %>% 
      dplyr::filter(is.na(f_bool)) %>%
      dplyr::transmute(da, db, "f_bool" = f_double > 1, "upper" = "mutated", "f_int32" = nchar(lower))
  )

  # New fields generated & no old fields preserved
  assert_df_match(
    RefArray$
      filter(is_null(f_bool))$
      transmute(lower_len = "strlen(lower)")$
      to_df_attrs(),
    ArrayContent %>% 
      dplyr::filter(is.na(f_bool)) %>%
      dplyr::transmute("lower_len" = nchar(lower))
  )

})


# i_mutate ----

test_that("i_mutate returns arrays with same schemas", {
  
  # `i_mutate` Behave as `mutate` if no extra fields provided
  assert_df_match(
    RefArray$
      filter(is_null(f_bool))$
      i_mutate('f_bool'='iif(f_double > 1, true, false)', 'upper'="'mutated'", 'f_int32' = "strlen(lower)")$
      to_df(),
    ArrayContent %>% 
      dplyr::filter(is.na(f_bool)) %>%
      dplyr::mutate("f_bool" = f_double > 1, "upper" = "mutated", "f_int32" = nchar(lower))
  )
  
  # Error cases
  
  expect_error(RefArray$
    filter(is_null(f_bool))$
    i_mutate('da', 'upper'="'mutated'"),
    "named list"
  )
  
  expect_error(RefArray$
    filter(is_null(f_bool))$
    i_mutate('da'='3', 'upper'="'mutated'"),
    "da"
  )
  
  expect_error(RefArray$
    filter(is_null(f_bool))$
    i_mutate('new_field'='iif(f_double > 1, true, false)', 'upper'="'mutated'"),
    "new_field"
  )
  expect_error(RefArray$
    filter(is_null(f_bool))$
    i_mutate("f_int32" = NULL, 'upper'="'mutated'"),
    "f_int32"
  )
    
})

# i_mutate_by another arrayOp ----

test_that("mutate: key:1-attr, update_fields: 2 attrs", {
  mutateDataSource = ArrayContent %>% 
    dplyr::filter(da %% 2 == 0) %>% 
    dplyr::mutate(f_int32 = 1:3, upper = 'changed')
  
  assert_df_match(
    RefArray$i_mutate_by(
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
    RefArray$i_mutate_by(
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
    RefArray$i_mutate_by(
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
    RefArray$i_mutate_by(
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
    RefArray$i_mutate_by(
      dataArray, keys = c('lower', 'db'), updated_fields = c('f_int32', 'upper')
    )$to_df(),
    mutateDataSource
  )
})

RefArray$remove_self()

