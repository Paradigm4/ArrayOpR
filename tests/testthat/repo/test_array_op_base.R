context("Test ArrayOp base class")


# Scidb array basics ----------------------------------------------------------------------------------------------


test_that("Raw afl, dimensions, attributes work as expected", {
  op = ArrayOp$new("rawafl", c("da", "db"), c("ac", "ad"))
  expect_identical(op$to_afl(), "rawafl")
  expect_identical(op$dims, c("da", "db"))
  expect_identical(op$attrs, c("ac", "ad"))
  # Since no dtypes are passed to initilize function, all fields are not annotated with dtypes.
  expect_error(op$get_field_types(), 'not annotated')
})

test_that("Raw afl, dimensions, attributes work as expected with data types", {
  op = ArrayOp$new("rawafl", c("da", "db"), c("ac", "ad"), 
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
  op = ArrayOp$new("rawafl", c("da", "db"), c("ac", "ad"), 
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

test_that("Resultant ArrayOp has the same schema as the original", {
  op = ArrayOp$new("rawafl", c("da", "db"), c("ac", "ad"), 
    dtypes = list(ac='string', ad='int32', da='int64', db='int64'))
  result = op$where(da <0 , ad == 42)
  expect_identical(result$dims, c('da', 'db'))
  expect_identical(result$attrs, c('ac', 'ad'))
  expect_identical(result$get_field_types(), op$get_field_types())
})


# Select ----------------------------------------------------------------------------------------------------------

test_that("Select fields do not change afl output", {
  op = ArrayOp$new("rawafl", c("da", "db"), c("ac", "ad"), dtypes = list(ac='string', ad='int32', da='int64', db='int64'))
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
  op = ArrayOp$new("rawafl", c("da", "db"), c("ac", "ad"), dtypes = list(ac='string', ad='int32', da='int64', db='int64'))
  expect_error(op$select('non-existent'))
  expect_error(op$select('da', 'non-exis tent'))
})
