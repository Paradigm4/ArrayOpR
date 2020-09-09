context("ArrayOp: mutate")

schemaTemplate = conn$array_from_schema(
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
  array_from_df(ArrayContent, schemaTemplate, force_template_schema = T)$
  persist(.gc = FALSE)

assert_df_match = function(actual_df, expected_df) {
   expect_equal(
    actual_df %>% dplyr::arrange(!!! sapply(names(actual_df), as.name)), 
    expected_df
  )
}

# Mutate new ----

test_that("Mutate an array by expression", {
  # Mutate existing field
  assert_df_match(
    RefArray$
      mutate(f_bool=iif(f_double > 1, true, false), upper='mutated', f_int32 = strlen(lower))$
      to_df_all(),
    ArrayContent %>% 
      dplyr::mutate("f_bool" = !is.na(f_double) & f_double > 1, upper = "mutated", f_int32 = nchar(lower))
  )
  assert_df_match(
    RefArray$
      filter(is_null(f_bool))$
      mutate(f_bool=iif(f_double > 1, !!T, !!F), upper='mutated', f_int32 = strlen(lower), 
             .sync_schema = F)$ # do not check schema
      to_df_all(),
    ArrayContent %>% 
      dplyr::filter(is.na(f_bool)) %>%
      dplyr::mutate(f_bool = f_double > 1, upper = "mutated", f_int32 = nchar(lower))
  )

  # New fields generated
  assert_df_match(
    RefArray$
      filter(is_null(f_bool))$
      mutate(lower_len = strlen(lower))$
      to_df_all(),
    ArrayContent %>% 
      dplyr::filter(is.na(f_bool)) %>%
      dplyr::mutate(lower_len = nchar(lower))
  )

  # New fields generated and existing fields are ignored if they are not changed
  assert_df_match(
    RefArray$
      filter(is_null(f_bool))$
      mutate(lower_len = strlen(lower), f_int32 = f_int32)$ # f_int32 ignored
      to_df_all(),
    ArrayContent %>% 
      dplyr::filter(is.na(f_bool)) %>%
      dplyr::mutate(lower_len = nchar(lower))
  )

  # New fields generated and existing fields removed by NULL
  assert_df_match(
    RefArray$
      filter(is_null(f_bool))$
      mutate(lower_len = strlen(lower), f_bool = NULL, f_int64 = NULL)$
      to_df_all(),
    ArrayContent %>% 
      dplyr::filter(is.na(f_bool)) %>%
      dplyr::mutate(f_bool = NULL, f_int64 = NULL, lower_len = nchar(lower))
  )
  # Remove existing fields
  assert_df_match(
    RefArray$
      mutate(f_bool = NULL, f_int64 = NULL)$
      to_df_all(),
    ArrayContent %>% 
      dplyr::mutate(f_bool = NULL, f_int64 = NULL)
  )
  # Pass field expression by list
  assert_df_match(
    RefArray$
      filter(is_null(f_bool))$
      mutate(.dots = list(lower_len = aflutils$e(strlen(lower)), f_bool = NULL, f_int64 = NULL))$
      to_df_all(),
    ArrayContent %>% 
      dplyr::filter(is.na(f_bool)) %>%
      dplyr::mutate(f_bool = NULL, f_int64 = NULL, lower_len = nchar(lower))
  )
})

test_that("Add attributes from dimensions", {
  assert_df_match(
    RefArray$
      mutate(lower_len = strlen(lower), da = da)$
      to_df() %>% 
      dplyr::select(da, dplyr::everything()),
    ArrayContent %>% 
      dplyr::mutate(db = NULL, lower_len = nchar(lower))
  )
})

test_that("At least one field in result", {
  assert_df_match(
    RefArray$
      mutate(f_bool = NULL, f_double = NULL, f_int32 = NULL, f_int64 = NULL, lower = NULL, upper = NULL, extra = bool(null))$
      select("da", "db")$
      to_df(),
    ArrayContent %>% 
      dplyr::select(da, db)
  )
  # all array attrs are removed
  expect_error(
    RefArray$
      mutate(f_bool = NULL, f_double = NULL, f_int32 = NULL, f_int64 = NULL, lower = NULL, upper = NULL),
    "no valid field"
  )
  
})

test_that("mutate error cases", {
  expect_error(
    RefArray$
      filter(is_null(f_bool))$
      mutate(upper='mutated', f_int32 = strlen(lower), nonexistent = NULL),
    "nonexistent"
  )
  expect_error(
    RefArray$
      filter(is_null(f_bool))$
      mutate(f_bool, upper='mutated', f_int32 = strlen(lower)),
    "f_bool"
  )
})

# transmute ----

test_that("transmute does not preserve old fields", {
  # transmute an array will keep the dimensions
  assert_df_match(
    RefArray$
      filter(is_null(f_bool))$
      transmute(f_double, upper='mutated', f_int32 = strlen(lower))$
      to_df_all(),
    ArrayContent %>% 
      dplyr::filter(is.na(f_bool)) %>%
      dplyr::transmute(da, db, f_double, upper = "mutated", f_int32 = nchar(lower))
  )
  assert_df_match(
    RefArray$
      filter(is_null(f_bool))$
      transmute(f_double=f_double, upper="mutated", f_int32 = strlen(lower))$
      to_df_all(),
    ArrayContent %>% 
      dplyr::filter(is.na(f_bool)) %>%
      dplyr::transmute(da, db, f_double, upper = "mutated", f_int32 = nchar(lower))
  )
  assert_df_match(
    RefArray$
      filter(is_null(f_bool))$
      transmute(f_bool=iif(f_double > 1, true, false), upper="mutated", f_int32 = strlen(lower))$
      to_df_all(),
    ArrayContent %>% 
      dplyr::filter(is.na(f_bool)) %>%
      dplyr::transmute(da, db, f_bool = f_double > 1, upper = "mutated", f_int32 = nchar(lower))
  )

  # New fields generated & no old fields preserved
  assert_df_match(
    RefArray$
      filter(is_null(f_bool))$
      transmute(.dots = list(lower_len = aflutils$e(strlen(lower))))$
      to_df(),
    ArrayContent %>% 
      dplyr::filter(is.na(f_bool)) %>%
      dplyr::transmute(lower_len = nchar(lower))
  )
})

test_that("Transmute array dimensions to attributes", {
  assert_df_match(
    RefArray$
      transmute(da)$
      to_df(),
    ArrayContent %>% 
      dplyr::transmute(da)
  )
  assert_df_match(
    RefArray$
      transmute(da, lower)$
      to_df(),
    ArrayContent %>% 
      dplyr::transmute(da, lower)
  )
  assert_df_match(
    RefArray$
      transmute(da = da + 1, lower)$
      to_df(),
    ArrayContent %>% 
      dplyr::transmute(da = da + 1, lower)
  )
})

test_that("Transmute error cases", {
  expect_error(RefArray$transmute(f_int32 = NULL), "transmute fields")
})


# mutate_by another arrayOp ----

test_that("mutate: key:1-attr, update_fields: 2 attrs", {
  mutateDataSource = ArrayContent %>% 
    dplyr::filter(da %% 2 == 0) %>% 
    dplyr::mutate(f_int32 = 1:3, upper = 'changed')
  
  assert_df_match(
    RefArray$mutate_by(
      conn$array_from_df(mutateDataSource %>% dplyr::select(lower, upper, f_int32)), 
      keys = c('lower'), updated_fields = c('f_int32', 'upper')
    )$to_df_all(),
    mutateDataSource
  )
})

test_that("mutate: key:2-attrs, update_fields: 1 attr", {
  mutateDataSource = ArrayContent %>% 
    dplyr::filter(da %% 2 == 0) %>% 
    dplyr::mutate(f_int32 = 1:3)
  
  dataArray = conn$array_from_df(mutateDataSource %>% dplyr::select(lower, upper, f_int32), schemaTemplate)
  
  assert_df_match(
    RefArray$mutate_by(
      dataArray, keys = c('lower', 'upper'), updated_fields = c('f_int32')
    )$to_df_all(),
    mutateDataSource
  )
})

test_that("mutate: key:1 dim, update_fields: 1 attr", {
  mutateDataSource = ArrayContent %>% 
    dplyr::filter(da %% 2 == 0) %>% 
    dplyr::mutate(f_int32 = 1:3)
  dataArray = conn$array_from_df(mutateDataSource %>% dplyr::select(da, f_int32), schemaTemplate)
  
  assert_df_match(
    RefArray$mutate_by(
      dataArray, keys = c('da'), updated_fields = c('f_int32')
    )$to_df_all(),
    mutateDataSource
  )
})

test_that("mutate: key:2 dims, update_fields: 1 attr", {
  mutateDataSource = ArrayContent %>% 
    dplyr::filter(da %% 2 == 0) %>% 
    dplyr::mutate(f_int32 = 1:3)
  dataArray = conn$array_from_df(mutateDataSource %>% dplyr::select(da, db, f_int32), schemaTemplate)
  
  assert_df_match(
    RefArray$mutate_by(
      dataArray, keys = c('da', 'db'), updated_fields = c('f_int32')
    )$to_df_all(),
    mutateDataSource
  )
})

test_that("mutate: key: 1 dim + 1 attr, update_fields: 2 attrs", {
  mutateDataSource = ArrayContent %>% 
    dplyr::filter(da %% 2 == 0) %>% 
    dplyr::mutate(f_int32 = 1:3, upper = "changed")
  dataArray = conn$array_from_df(mutateDataSource %>% dplyr::select(db, lower, f_int32, upper), schemaTemplate)
  
  assert_df_match(
    RefArray$mutate_by(
      dataArray, keys = c('lower', 'db'), updated_fields = c('f_int32', 'upper')
    )$to_df_all(),
    mutateDataSource
  )
})

test_that("mutate_by: no reserved fields", {
  mutateDataSource = ArrayContent %>% 
    dplyr::filter(da %% 2 == 0) %>% 
    dplyr::mutate(f_int32 = 1:3, upper = "changed")
  dataArray = conn$array_from_df(
    mutateDataSource %>% dplyr::select(db, lower, upper, f_int32, f_int64, f_bool, f_double),
    schemaTemplate
  )
  assert_df_match(
    RefArray$mutate_by(
      dataArray, keys = c('lower', 'db'), updated_fields = dataArray$attrs %-% c('lower', 'db')
    )$to_df_all(),
    mutateDataSource
  )
})

test_that("mutate_by: data_array already conforms to target schema", {
  mutateDataSource = ArrayContent %>% 
    dplyr::filter(da %% 2 == 0) %>% 
    dplyr::mutate(f_int32 = 1:3, upper = "changed")
  dataArray = conn$array_from_df(
    mutateDataSource,
    schemaTemplate, force_template_schema = T
  )
  assert_df_match(
    RefArray$mutate_by(
      dataArray # no keys or updated_fields required. keys = dataArray$dims, updated_fields = dataArray$attrs
    )$to_df_all(),
    mutateDataSource
  )
  assert_df_match(
    RefArray$mutate_by(
      dataArray, updated_fields = c("lower", "upper", "f_int32")
    )$to_df_all(),
    mutateDataSource
  )
  expect_warning({
    result = RefArray$mutate_by(
      dataArray, keys = c("da", "db", "lower"), updated_fields = c("upper", "f_int32")
    )$to_df_all()
  }, "lower")
  assert_df_match(
    result,
    mutateDataSource
  )
})

RefArray$remove_array()

