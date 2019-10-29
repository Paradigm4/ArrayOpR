context("Test ArrayOpBase's convenience methods")


# transform_unpack ------------------------------------------------------------------------------------------------

test_that("Select a subset of existing fields", {
  x = AnyArrayOp$new('x', c('da', 'db'), c('aa', 'ab'))
  t = x$transform_unpack(list('aa'), unpack_dim_name = 'z')
  expect_identical(t$dims_n_attrs, c('z', 'aa'))
  expect_identical(t$get_field_types('aa'), list(aa='dt_aa'))
  assert_afl_equal(t$to_afl(), "project(unpack(x, z), aa)")
})

test_that("Select new fields", {
  x = AnyArrayOp$new('x', c('da', 'db'), c('aa', 'ab'))
  t = x$transform_unpack(list(newField = 'aa + ab', newConstant = "42", newStringField = "'value'"), unpack_dim_name = 'z')
  expect_identical(t$dims_n_attrs, c('z', 'newField', 'newConstant', 'newStringField'))
  assert_afl_equal(t$to_afl(),
      "project(
        apply(unpack(x, z), newField, aa+ab, newConstant, 42, newStringField, 'value'),
        newField, newConstant, newStringField
      )")
})

test_that("Select new fields and existing fields", {
  x = AnyArrayOp$new('x', c('da', 'db'), c('aa', 'ab'))
  # Notice 'da', 'ab' are existing fields
  t = x$transform_unpack(list(newField = 'aa + ab', newConstant = "42", 'da', 'ab'), unpack_dim_name = 'z')
  expect_identical(t$dims_n_attrs, c('z', 'newField', 'newConstant', 'da', 'ab'))
  assert_afl_equal(t$to_afl(),
      "project(
        apply(unpack(x, z), newField, aa+ab, newConstant, 42),
        newField, newConstant, da, ab
      )")
})

test_that("Result field types inherits from the operand, and can be specified", {
  x = AnyArrayOp$new('x', c('da', 'db'), c('aa', 'ab'))
  fields = c('z', 'newField', 'newConstant', 'da', 'ab')
  # Without a 'dtypes' arg, only existing fields inherit their field types from the operand
  t = x$transform_unpack(list(newField = 'aa + ab', newConstant = "42", 'da', 'ab'), unpack_dim_name = 'z')
  expect_identical(t$get_field_types(fields), list(da='dt_da', ab='dt_ab'))
  # Provide a 'dtypes' for new fields
  t = x$transform_unpack(list(newField = 'aa + ab', newConstant = "42", 'da', 'ab'),
                         dtypes = list(newConstant='int64', newField='string'), unpack_dim_name = 'z')
  expect_identical(t$get_field_types(fields), list(newField='string', newConstant='int64', da='dt_da', ab='dt_ab'))
})

# convert_df ------------------------------------------------------------------------------------------------------

test_that("Build data.frame content for WriteOp", {
  target = CustomizedOp$new('target', c('da', 'db'), c('aa', 'ab'),
                        field_types = list(da='int64', db='int64', aa='string', ab='int32'))

  df = data.frame(da = c(1,2), aa=c('aa1', 'aa2'), ab=c(3,4), db = c(5, 6))
  dataset = target$convert_df(df, mode='build')

  assert_afl_equal(dataset$to_afl(),
    "build(<da:int64, aa:string, ab:int32, db:int64>[j],
                    '[(1, \\'aa1\\', 3, 5), (2, \\'aa2\\', 4, 6)]', true)")
})

test_that("Build data.frame content for WriteOp with customized dimension name", {
  target = CustomizedOp$new('target', c('da', 'db'), c('aa', 'ab'),
                        field_types = list(da='int64', db='int64', aa='string', ab='int32'))

  df = data.frame(da = c(1,2), aa=c('aa1', 'aa2'), ab=c(3,4), db = c(5, 6))
  dataset = target$convert_df(df, mode='build', build_dimension = 'xxx')

  assert_afl_equal(dataset$to_afl(),
    "build(<da:int64, aa:string, ab:int32, db:int64>[xxx],
                    '[(1, \\'aa1\\', 3, 5), (2, \\'aa2\\', 4, 6)]', true)")
})

test_that("Build data.frame content with NULL values", {
  target = CustomizedOp$new('target', 'da', c('aa', 'ab'),
                        field_types = list(da='int64', aa='string', ab='int32'))

  df = data.frame(da = c(1,2, NA, 4), aa=c('aa1', NA, NA, 'aa2'), ab=c(NA,4,NA, 5))
  dataset = target$convert_df(df, mode='build', build_dimension = 'xxx')


  assert_afl_equal(dataset$to_afl(),
    "build(<da:int64, aa:string, ab:int32>[xxx],
                    '[(1, \\'aa1\\',), (2,, 4), (,,), (4, \\'aa2\\', 5)]', true)")
})

test_that("Missing fields in dataset assigned with 'missing_values'", {
  target = CustomizedOp$new('target', c('da', 'db'), c('aa', 'ab'),
                        field_types = list(da='int64', db='int64', aa='string', ab='int32'))

  df = data.frame(da = c(1,2), aa=c('aa1', 'aa2'), ab=c(3,4))
  dataset = target$convert_df(df, mode='build', missing_values = list(db = 'j+42'))

  assert_afl_equal(dataset$to_afl(),
    "apply(
        build(<da:int64, aa:string, ab:int32>[j],
              '[(1, \\'aa1\\', 3), (2, \\'aa2\\', 4)]', true),
    db, j+42)")
})

test_that("Missing fields in dataset but without arg 'missing_values'", {
  target = CustomizedOp$new('target', c('da', 'db'), c('aa', 'ab'),
                        field_types = list(da='int64', db='int64', aa='string', ab='int32'))

  df = data.frame(da = 1, aa='aa', ab=2)
  # df doen't have all template's field and no 'missing_fields' is provided.
  expect_error(target$convert_df(df, mode='build'), "missing template fields")

  # If we skip validating fields, the build result may not have all the template fields, which is useful in complicated
  # use cases where we need to join multiple array expressions for `insert` or `store` operations
  converted = target$convert_df(df, mode = 'build', validate_fields = F)
  assert_afl_equal(converted$to_afl(), "build(<da:int64, aa:string, ab:int32>[j], '[(1, \\'aa\\', 2)]', true)")
})

test_that("Extra columns in df causes an error", {
  target = CustomizedOp$new('target', 'da', 'aa', field_types = list(da='int64', aa='string'))

  df = data.frame(da = 1, aa='aa', ab=2, extra = 4)
  expect_error(target$convert_df(df, mode = 'build', validate_fields = F), 'extra')
})

test_that("Multiple missing fields in dataset assigned with 'missing_values'", {
  target = CustomizedOp$new('target', c('da', 'db'), c('aa', 'ab'),
                        field_types = list(da='int64', db='int64', aa='string', ab='int32'))

  df = data.frame(da = c(1,2), aa=c('aa1', 'aa2'))
  dataset = target$convert_df(df, mode='build', missing_values = list(ab = 'now()', db = 'j+42'))

  assert_afl_equal(dataset$to_afl(),
    "apply(
        build(<da:int64, aa:string>[j],
              '[(1, \\'aa1\\'), (2, \\'aa2\\')]', true), ab, now(),
    db, j+42)")
})




# rename_fields ---------------------------------------------------------------------------------------------------

test_that("Rename dimensions", {
   template = CustomizedOp$new('target', c('da', 'db'), c('aa', 'ab'),
                        field_types = list(da='int64', db='int64', aa='string', ab='int32'))
   renamed = template$rename_fields(list(da = 'DA'))
   newFields = c('DA', 'db', 'aa', 'ab')
   expect_identical(renamed$dims_n_attrs, newFields)
   expect_identical(renamed$get_field_types(newFields), list(DA='int64', db='int64', aa='string', ab='int32'))
   expect_identical(renamed$to_afl(), 'target')
})

test_that("Rename attributes", {
   template = CustomizedOp$new('target', c('da', 'db'), c('aa', 'ab'),
                        field_types = list(da='int64', db='int64', aa='string', ab='int32'))
   renamed = template$rename_fields(list(ab = 'AB', aa = 'AA'))
   newFields = c('da', 'db', 'AA', 'AB')
   expect_identical(renamed$dims_n_attrs, newFields)
   expect_identical(renamed$get_field_types(newFields), list(da='int64', db='int64', AA='string', AB='int32'))
   expect_identical(renamed$to_afl(), 'target')
})

test_that("Rename dimensions and attributes", {
   template = CustomizedOp$new('target', c('da', 'db'), c('aa', 'ab'),
                        field_types = list(da='int64', db='int64', aa='string', ab='int32'))
   renamed = template$rename_fields(list(ab = 'AB', da = 'DA'))
   newFields = c('DA', 'db', 'aa', 'AB')
   expect_identical(renamed$dims_n_attrs, newFields)
   expect_identical(renamed$get_field_types(newFields), list(DA='int64', db='int64', aa='string', AB='int32'))
   expect_identical(renamed$to_afl(), 'target')
})
