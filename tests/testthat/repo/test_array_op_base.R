context("Test newArrayOp base class")

newArrayOp = function(...) ArrayOpBase$new(...)

# Scidb array basics ----------------------------------------------------------------------------------------------


test_that("Raw afl, dimensions, attributes work as expected", {
  op = newArrayOp("rawafl", c("da", "db"), c("ac", "ad"))
  expect_identical(op$to_afl(), "rawafl")
  expect_identical(op$dims, c("da", "db"))
  expect_identical(op$attrs, c("ac", "ad"))
  # Since no dtypes are passed to initilize function, all fields are not annotated with dtypes.
  expect_error(op$get_field_types(), 'not annotated')
})

test_that("Raw afl, dimensions, attributes work as expected with data types", {
  op = newArrayOp("rawafl", c("da", "db"), c("ac", "ad"), 
    dtypes = list(ac='string', ad='int32', da='int64', db='int64'))
  expect_identical(op$to_afl(), "rawafl")
  expect_identical(op$dims, c("da", "db"))
  expect_identical(op$attrs, c("ac", "ad"))
  # By default, get_field_types returns dims+attrs data types
  expect_identical(op$get_field_types(), list(da='int64', db='int64', ac='string', ad='int32'))
  expect_identical(op$get_field_types(op$dims_n_attrs), list(da='int64', db='int64', ac='string', ad='int32'))
  expect_identical(op$get_field_types('da'), list(da='int64'))
  expect_identical(op$get_field_types('ac'), list(ac='string'))

})


# Where ----------------------------------------------------------------------------------------------------------

test_that("Where an Array using filter expressions", {
  op = newArrayOp("rawafl", c("da", "db"), c("ac", "ad"), 
    dtypes = list(ac='string', ad='int32', da='int64', db='int64'))
  # Original op won't be affected
  expect_identical(op$to_afl(), 'rawafl')
  # Multiple filter expressions are treated as AND replation
  assert_afl_equal(op$where(da < 0)$to_afl(), "filter(rawafl, da < 0)")
  assert_afl_equal(op$where(da < 0, ad == 42)$to_afl(), "filter(rawafl, da < 0 and ad = 42)")
  # Use special functions AND/OR to compose complex filter expressions
  assert_afl_equal(op$where(OR(da != 0, db == 3))$to_afl(), "filter(rawafl, da <> 0 or db = 3)")
  
  # Error if filter on non-existent fields
  expect_error(op$where(nonExistent == 42), 'not found')
})

test_that("Resultant newArrayOp has the same schema as the original", {
  op = newArrayOp("rawafl", c("da", "db"), c("ac", "ad"), 
    dtypes = list(ac='string', ad='int32', da='int64', db='int64'))
  result = op$where(da <0 , ad == 42)
  expect_identical(result$dims, c('da', 'db'))
  expect_identical(result$attrs, c('ac', 'ad'))
  expect_identical(result$get_field_types(), op$get_field_types())
})


# Select ----------------------------------------------------------------------------------------------------------

test_that("Select fields do not change afl output", {
  op = newArrayOp("rawafl", c("da", "db"), c("ac", "ad"), dtypes = list(ac='string', ad='int32', da='int64', db='int64'))
  for(result in c(
    op$select('ac'),
    op$select('ad'),
    op$select('da', 'db', 'ac', 'ad'),
    op$select()
  )){
    # The original op will not be changed
    expect_equal(length(op$selected), 0)
    resultSelected = result$selected
    assert_afl_equal(result$to_afl(), "rawafl")
    # Derive another op
    another = result$select('da', 'ad')
    assert_afl_equal(another$to_afl(), 'rawafl')
    expect_identical(another$selected, c('da', 'ad'))
    expect_identical(result$selected, resultSelected)  # result's selected fields are not changed
  }
})

test_that("Cannot select non-existent fields", {
  op = newArrayOp("rawafl", c("da", "db"), c("ac", "ad"), dtypes = list(ac='string', ad='int32', da='int64', db='int64'))
  expect_error(op$select('non-existent'))
  expect_error(op$select('da', 'non-exis tent'))
})


# Reshape ---------------------------------------------------------------------------------------------------------

Source = newArrayOp("s", c("da", "db"), c("ac", "ad"), dtypes = list(ac='dtac', ad='dtad', da='dtda', db='dtdb'))

# 
# dim_mode = 'keep' by default
# 
test_that("Select one attr", {
  for(t in c(
    Source$reshape('ac')
    , Source$reshape(list('ac', 'da'))
    , Source$reshape(c('ac', 'da', 'db'))
    , Source$reshape(c('da', 'ac'))
    , Source$reshape(c('ac', 'da', 'db'), dim_mode = 'keep')
  )){
    expect_identical(t$attrs, 'ac')
    expect_identical(t$dims, c('da', 'db'))
    expect_identical(t$get_field_types('ac'), list(ac='dtac'))
    assert_afl_equal(t$to_afl(), "project(s, ac)")
  }
})
test_that("Select two attrs", {
  for(t in c(
    Source$reshape(c('ad', 'ac'))
    , Source$reshape(list('ad', 'ac', 'da'))
    , Source$reshape(c('ad', 'ac', 'da', 'db'))
    , Source$reshape(c('ad', 'da', 'ac'))
    , Source$reshape(c('ad', 'ac', 'da', 'db'), dim_mode = 'keep')
  )){
    expect_identical(t$attrs, c('ad', 'ac'))
    expect_identical(t$dims, c('da', 'db'))
    expect_identical(t$get_field_types(c('ad', 'ac')), list(ad='dtad', ac='dtac'))
    assert_afl_equal(t$to_afl(), "project(s, ad, ac)")
  }
})
test_that("Select dims only", {
  for(t in c(
    Source$reshape(c('da'), artificial_field = 'z')
    , Source$reshape(list('da', 'db'), artificial_field = 'z')
    , Source$reshape(list('db', 'da'), artificial_field = 'z')
    , Source$reshape(c('da', 'db'), dim_mode = 'keep', artificial_field = 'z')
  )){
    expect_identical(t$attrs, 'z')
    expect_identical(t$dims, c('da', 'db'))
    expect_identical(t$get_field_types(c('da', 'db')), list(da='dtda', db='dtdb'))
    assert_afl_equal(t$to_afl(), "project(apply(s, z, null), z)")
  }
})

# 
# dim_mode = 'drop' 
# 
test_that("Select existing attributes/dimensions in dim_mode = 'drop'", {
  # Select attrs
  t = Source$reshape('ac', dim_mode = 'drop', artificial_field = 'z')
  expect_identical(t$dims, 'z')
  expect_identical(t$attrs, 'ac')
  expect_identical(t$dims_n_attrs, c('z', 'ac'))
  expect_identical(t$get_field_types(), list(z = 'int64', ac = 'dtac'))
  assert_afl_equal(t$to_afl(), "project(unpack(s, z), ac)")
  # Select dims
  t = Source$reshape('da', dim_mode = 'drop', artificial_field = 'z')
  expect_identical(t$dims, 'z')
  expect_identical(t$attrs, 'da')
  expect_identical(t$dims_n_attrs, c('z', 'da'))
  expect_identical(t$get_field_types(), list(z = 'int64', da = 'dtda'))
  assert_afl_equal(t$to_afl(), "project(unpack(s, z), da)")
  # Select dims and attrs
  t = Source$reshape(c('ac', 'da'), dim_mode = 'drop', artificial_field = 'z')
  expect_identical(t$dims, 'z')
  expect_identical(t$attrs, c('ac', 'da'))
  expect_identical(t$dims_n_attrs, c('z', 'ac', 'da'))
  expect_identical(t$get_field_types(), list(z = 'int64', ac = 'dtac', da = 'dtda'))
  assert_afl_equal(t$to_afl(), "project(unpack(s, z), ac, da)")
})

test_that("Select new fields in dim_mode = 'drop'", {
  # Select new fields only
  t = Source$reshape(list(nfa = 'strlen(ad)', nfb = '42'), 
    dtypes = list(nfa='int32', nfb='int64'), dim_mode = 'drop', artificial_field = 'z')
  expect_identical(t$dims, 'z')
  expect_identical(t$attrs, c('nfa', 'nfb'))
  expect_identical(t$get_field_types(), list(z = 'int64', nfa = 'int32', nfb = 'int64'))
  assert_afl_equal(t$to_afl(), "project(apply(unpack(s, z), nfa, strlen(ad), nfb, 42), nfa, nfb)")
  
  # Select new fields and existing attrs
  t = Source$reshape(list('ac', nfa = 'strlen(ad)'), dtypes = list(nfa='int32'), dim_mode = 'drop', artificial_field = 'z')
  expect_identical(t$dims, 'z')
  expect_identical(t$attrs, c('ac', 'nfa'))
  expect_identical(t$get_field_types(), list(z = 'int64', ac = 'dtac', nfa = 'int32'))
  assert_afl_equal(t$to_afl(), "project(apply(unpack(s, z), nfa, strlen(ad)), ac, nfa)")
  
  # Select new fields and existing dims
  t = Source$reshape(list(nfa = 'strlen(ad)', 'da'), dtypes = list(nfa='int32'), dim_mode = 'drop', artificial_field = 'z')
  expect_identical(t$dims, 'z')
  expect_identical(t$attrs, c('nfa', 'da'))
  expect_identical(t$get_field_types(), list(z = 'int64', nfa = 'int32', da = 'dtda'))
  assert_afl_equal(t$to_afl(), "project(apply(unpack(s, z), nfa, strlen(ad)), nfa, da)")
})

