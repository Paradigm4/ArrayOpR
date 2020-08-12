context("ArrayOp: semi_join")


schemaTemplate = conn$array_op_from_schema_str(
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

RefArray = conn$
  array_op_from_df(ArrayContent, schemaTemplate)$
  change_schema(schemaTemplate)$
  persist(.gc = FALSE)


assert_df_match = function(result_op, expected_df, afl_patterns) {
  resultAfl = result_op$to_afl()
  sapply(afl_patterns, function(x) expect_match(resultAfl, x))
  expect_identical(result_op$to_schema_str(), RefArray$to_schema_str())
  # Scidb doesn't have a deterministic ordering rule, so we need to sort the result data frame and then compare
  expect_equal(
    dplyr::arrange(data.frame(result_op$to_df()), da),
    dplyr::arrange(expected_df, da)
  )
}

# Filter mode ----

test_that("filter, 1-dim", {
  df = data.frame(da = c(3,5,8,11))
  
  assert_df_match(
    RefArray$semi_join(df, filter_threshold=10, upload_threshold=20),
    dplyr::semi_join(ArrayContent, df),
    "filter"
  )
})

test_that("filter, 1-attr", {
  values = c(-10, -11, -12)
  df = data.frame(f_int32 = values)
  
  assert_df_match(
    RefArray$semi_join(df, filter_threshold=10, upload_threshold=20),
    dplyr::semi_join(ArrayContent, df),
    "filter"
  )
})

test_that("filter, 2-attrs", {
  values1 = c(-20, -17, -16)
  values2 = c("no_match", "d", "e")
  df = data.frame(f_int32 = values1, lower = values2)
  
  assert_df_match(
    RefArray$semi_join(df, filter_threshold=10, upload_threshold=20),
    dplyr::semi_join(ArrayContent, df),
    "filter"
  )
})

test_that("filter, 1-dim + 1-attr", {
  values1 = c(-1, 4, 5)
  values2 = c("no_match", "d", "e")
  df = data.frame(da = values1, lower = values2)
  
  assert_df_match(
    RefArray$semi_join(df, filter_threshold=10, upload_threshold=20),
    dplyr::semi_join(ArrayContent, df),
    "filter"
  )
})

test_that("filter, lower/upper bounds on same dimension", {
  df = data.frame(da_low = c(1,3), da_hi = c(5, 8))
  
  assert_df_match(
    RefArray$semi_join(df, 
                   lower_bound = list(da = 'da_low'), 
                   upper_bound = list(da = 'da_hi'),
                   filter_threshold=10, upload_threshold=20),
    dplyr::filter(ArrayContent, (da >= 1 & da <= 5) | (da >=3 & da <=8)),
    "filter"
  )
})

test_that("filter, lower/upper bounds on different dimensions", {
  df = data.frame(da = c(1,3), db = c(105, 108))
  
  assert_df_match(
    RefArray$semi_join(df, 
                   lower_bound = list(da = 'da'), 
                   upper_bound = list(db = 'db'),
                   filter_threshold=10, upload_threshold=20),
    dplyr::filter(ArrayContent, (da >= 1 & db <= 105) | (da >=3 & db <=108)),
    "filter"
  )
})

# Cross_between mode ----

test_that("cross_between, 2 dimension, build", {
  df = data.frame(da = c(1:5, -1), db = c(101:105, -1))
  
  assert_df_match(
    RefArray$semi_join(df, filter_threshold=10, upload_threshold=20),
    dplyr::semi_join(ArrayContent, df, filter_threshold=5, upload_threshold=20),
    c("cross_between", "build")
  )
})

test_that("cross_between, error, no attribute allowed", {
  df = data.frame(da = c(1:5, -1), db = c(101:105, -1), lower = letters[1:6])
  expect_error(RefArray$semi_join(df, filter_threshold=10, upload_threshold=20),
               "not reference dimensions")
})


test_that("cross_between, lower/upper bounds on same dimension", {
  df = data.frame(da_low = c(1,3), da_hi = c(5, 8))
  
  assert_df_match(
    RefArray$semi_join(df, 
                   lower_bound = list(da = 'da_low'), 
                   upper_bound = list(da = 'da_hi'),
                   filter_threshold=2, upload_threshold=20),
    dplyr::filter(ArrayContent, (da >= 1 & da <= 5) | (da >=3 & da <=8)),
    "cross_between"
  )
})

test_that("cross_between, lower/upper bounds on different dimensions", {
  df = data.frame(da = c(1,3), db = c(105, 108))
  
  assert_df_match(
    RefArray$semi_join(df, 
                   lower_bound = list(da = 'da'), 
                   upper_bound = list(db = 'db'),
                   field_mapping = list(),
                   filter_threshold=2, upload_threshold=20),
    dplyr::filter(ArrayContent, (da >= 1 & db <= 105) | (da >=3 & db <=108)),
    "cross_between"
  )
})

# index_lookup mode ----

test_that("index_lookup, 1 dimension, build", {
  df = data.frame(da = 1:15)
  
  assert_df_match(
    RefArray$semi_join(df, filter_threshold=5, upload_threshold=20),
    dplyr::semi_join(ArrayContent, df),
    c("index_lookup", "build")
  )
})


test_that("index_lookup, 1 attribute, build", {
  df = data.frame(lower = letters[5:15])
  
  assert_df_match(
    RefArray$semi_join(df, filter_threshold=5, upload_threshold=20),
    dplyr::semi_join(ArrayContent, df),
    c("index_lookup", "build")
  )
})

test_that("index_lookup, 1 dimension, upload", {
  df = data.frame(da = 1:15)
  
  assert_df_match(
    RefArray$semi_join(df, filter_threshold = 5, upload_threshold = 10),
    dplyr::semi_join(ArrayContent, df),
    c("index_lookup", "Rarrayop|R_array")
  )
})


test_that("index_lookup, 1 attribute, upload", {
  df = data.frame(lower = letters[5:15])
  
  assert_df_match(
    RefArray$semi_join(df, filter_threshold = 2, upload_threshold = 5),
    dplyr::semi_join(ArrayContent, df),
    c("index_lookup", "Rarryop|R_array")
  )
})

RefArray$remove_self()



