context("Test CustomizedOp class")

test_that("Names and types of fields are stored as provided", {
  op = CustomizedOp$new('raw_afl', c('da', 'db'), c('aa', 'ab'), field_types = list(da='int64', db='int64', aa='string', ab='int32'))
  expect_identical(op$to_afl(), 'raw_afl')
  expect_identical(op$to_afl(T), 'raw_afl')
  expect_identical(op$to_afl(F), 'raw_afl')
  expect_identical(op$to_df_afl(), 'raw_afl')
  expect_identical(op$to_df_afl(T), 'raw_afl')
  expect_identical(op$dims, c('da', 'db'))
  expect_identical(op$attrs, c('aa', 'ab'))
  expect_null(op$selected)
  expect_identical(op$get_field_types('aa'), list(aa='string'))
  expect_identical(op$get_field_types('da'), list(da='int64'))
  expect_identical(op$get_field_types(c('db', 'da', 'ab')), list(db='int64', da='int64', ab='int32'))
  # By default CustomizedOp doesn't validate fields
  expect_identical(op$get_absent_fields(c('non-1', 'non-2')), as.character(NULL))

  # Used as an operand
  subsetOp = SubsetOp$new(op, e(da > 0, aa == 'val'))
  assert_afl_equal(subsetOp$to_afl(), "filter(raw_afl, da > 0 and aa = 'val')")
})

test_that("Explicitly specify 'validate_fields=T' ", {
  op = CustomizedOp$new('raw_afl', c('da', 'db'), c('aa', 'ab'),
                    field_types = list(da='int64'), validate_fields = T)
  expect_identical(op$to_afl(), 'raw_afl')
  expect_identical(op$to_df_afl(), 'raw_afl')
  expect_identical(op$dims, c('da', 'db'))
  expect_identical(op$attrs, c('aa', 'ab'))
  expect_null(op$selected)
  expect_identical(op$get_field_types('aa'), EMPTY_NAMED_LIST)
  expect_identical(op$get_field_types('da'), list(da='int64'))
  expect_identical(op$get_field_types(c('db', 'da', 'ab')), list(da='int64'))
  # if validate_fields = T, CustomizedOp inherits base class validation logic
  expect_identical(op$get_absent_fields(c('non-1', 'non-2')), c('non-1', 'non-2'))
  expect_identical(op$get_absent_fields(c('non-1', 'da')), c('non-1'))

  # Used as an operand and perform field validation
  subsetOp = SubsetOp$new(op, e(da > 0, aa == 'val'))
  assert_afl_equal(subsetOp$to_afl(), "filter(raw_afl, da > 0 and aa = 'val')")

  expect_error(SubsetOp$new(op, e(non > 3)))
  expect_error(SubsetOp$new(op, e(da > 0, non == 'val')))
})

test_that("Project selected fields", {
  op = CustomizedOp$new('raw_afl', c('da', 'db'), c('aa', 'ab'), c('ab', 'aa'))
  assert_afl_equal(op$to_afl(), 'project(raw_afl, ab, aa)')
  op = CustomizedOp$new('raw_afl', c('da', 'db'), c('aa', 'ab'), c('da', 'aa'))
  assert_afl_equal(op$to_afl(), 'project(raw_afl, aa)')
  assert_afl_equal(op$to_afl(drop_dims=T), 'project(apply(raw_afl, da, da), da, aa)')
})
