context("Test WriteOp")

test_that("Target and dataset schemas align perfectly when redimension=F", {
  target = CustomizedOp$new('target', c('da', 'db'), c('aa', 'ab'),
                        field_types = list(da='int64', db='int64', aa='string', ab='int32'))
  dataset = CustomizedOp$new('dataset', c('da', 'db'), c('aa', 'ab'),
                        field_types = list(da='int64', db='int64', aa='string', ab='int32'))
  writeOp = WriteOp$new(target, dataset, redimension=F)
  assert_afl_equal(writeOp$to_afl(), "insert(dataset, target)")
})

test_that("Redimension needed if dataset field data types not matching target", {
  target = CustomizedOp$new('target', c('da', 'db'), c('aa', 'ab'),
                        field_types = list(da='int64', db='int64', aa='string', ab='int32'))
  dataset = CustomizedOp$new('dataset', 'i', c('da', 'db', 'aa', 'ab'),
                        field_types = list(da='int32', db='int32', aa='string', ab='string'))
  writeOp = WriteOp$new(target, dataset)
  assert_afl_equal(writeOp$to_afl(), "insert(redimension(dataset, target), target)")
  writeOp = WriteOp$new(target, dataset, append = F)
  assert_afl_equal(writeOp$to_afl(), "store(redimension(dataset, target), target)")
})

test_that("Build data.frame content for WriteOp", {
  target = CustomizedOp$new('target', c('da', 'db'), c('aa', 'ab'),
                        field_types = list(da='int64', db='int64', aa='string', ab='int32'))

  #TODO: be careful of floating numbers. Digits may be cut off due to double/float -> string conversion
  # https://github.com/Paradigm4/SciDBR/issues/193
  # Look into scidb digits precision
  df = data.frame(da = c(1,2), aa=c('aa1', 'aa2'), ab=c(3,4), db = c(5, 6))
  dataset = target$convert_df(df, mode='build')

  writeOp = WriteOp$new(target, dataset)
  assert_afl_equal(writeOp$to_afl(),
    "insert(
      redimension(
              build(<da:int64, aa:string, ab:int32, db:int64>[j],
                    '[(1, \\'aa1\\', 3, 5), (2, \\'aa2\\', 4, 6)]', true),
      target),
    target)")
})

test_that("Build data.frame content for WriteOp with customized dimension name", {
  target = CustomizedOp$new('target', c('da', 'db'), c('aa', 'ab'),
                        field_types = list(da='int64', db='int64', aa='string', ab='int32'))

  df = data.frame(da = c(1,2), aa=c('aa1', 'aa2'), ab=c(3,4), db = c(5, 6))
  dataset = target$convert_df(df, mode='build', build_dimension = 'xxx')

  writeOp = WriteOp$new(target, dataset)
  assert_afl_equal(writeOp$to_afl(),
    "insert(
      redimension(
              build(<da:int64, aa:string, ab:int32, db:int64>[xxx],
                    '[(1, \\'aa1\\', 3, 5), (2, \\'aa2\\', 4, 6)]', true),
      target),
    target)")
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
  #TODO: apply( cross_join(build(...), max(arr, id)), new_index, iif(max_id is null, 0, max_id+1+j))
  dataset = target$convert_df(df, mode='build', missing_values = list(db = 'j+42'))

  writeOp = WriteOp$new(target, dataset)
  assert_afl_equal(writeOp$to_afl(),
    "insert(
      redimension(
        apply(
              build(<da:int64, aa:string, ab:int32>[j],
                    '[(1, \\'aa1\\', 3), (2, \\'aa2\\', 4)]', true), db, j+42),
      target),
    target)")
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
  #TODO: apply( cross_join(build(...), max(arr, id)), new_index, iif(max_id is null, 0, max_id+1+j))
  dataset = target$convert_df(df, mode='build', missing_values = list(ab = 'now()', db = 'j+42'))

  writeOp = WriteOp$new(target, dataset)
  assert_afl_equal(writeOp$to_afl(),
    "insert(
      redimension(
        apply(
              build(<da:int64, aa:string>[j],
                    '[(1, \\'aa1\\'), (2, \\'aa2\\')]', true), ab, now(), db, j+42),
      target),
    target)")
})


# aio_input -------------------------------------------------------------------------------------------------------

test_that("Loading array from file using aio_input auto generates AFL based on a template array", {
  template = CustomizedOp$new('template', c('da', 'db'), c('aa', 'ab'),
                        field_types = list(da='int64', db='int64', aa='string', ab='int32'))
  op = get_aio_op('file_path', template = template)
  assert_afl_equal(op$to_afl(),
    "project(
      apply(
        aio_input('path=file_path', 'num_attributes=4'),
        da, int64(a0), db, int64(a1), aa, a2, ab, int32(a3)
      ),
    da, db, aa, ab)")
})

test_that("WriteOp with aio_input", {
  template = CustomizedOp$new('template', c('da', 'db'), c('aa', 'ab'),
                        field_types = list(da='int64', db='int64', aa='string', ab='int32'))
  op = get_aio_op('file_path', template = template)
  writeOp = WriteOp$new(template, op)
  assert_afl_equal(writeOp$to_afl(),
    "insert(
      redimension(
        project(
          apply(
            aio_input('path=file_path', 'num_attributes=4'),
            da, int64(a0), db, int64(a1), aa, a2, ab, int32(a3)
          ),
        da, db, aa, ab),
        template),
    template)")
})

test_that("With aio settings", {
  template = CustomizedOp$new('template', c('da', 'db'), c('aa', 'ab'),
                        field_types = list(da='int64', db='int64', aa='string', ab='int32'))
  op = get_aio_op('file_path', template = template, aio_settings = list(header=1))
  assert_afl_equal(op$to_afl(),
    "project(
      apply(
        aio_input('path=file_path', 'num_attributes=4', 'header=1'),
        da, int64(a0), db, int64(a1), aa, a2, ab, int32(a3)
      ),
    da, db, aa, ab)")
})

test_that("With customized field conversion", {
  template = CustomizedOp$new('template', c('da', 'db'), c('aa', 'ab'),
                        field_types = list(da='int64', db='int64', aa='string', ab='int32'))
  op = get_aio_op('file_path', template = template, field_conversion = list(ab = 'int64(@)+42'))
  assert_afl_equal(op$to_afl(),
    "project(
      apply(
        aio_input('path=file_path', 'num_attributes=4'),
        da, int64(a0), db, int64(a1), aa, a2, ab, int64(a3)+42
      ),
    da, db, aa, ab)")
})

test_that("dcast with customized field conversion, multiple attributes occurence", {
  template = CustomizedOp$new('template', c('da', 'db'), c('aa', 'ab'),
                        field_types = list(da='int64', db='int64', aa='string', ab='int32'))
  op = get_aio_op('file_path', template = template,
    field_conversion = list(ab = 'dcast(@,int64(null))', db = "iif(@='Y', 24, dcast(@, int64(null))"))
  assert_afl_equal(op$to_afl(),
    "project(
      apply(
        aio_input('path=file_path', 'num_attributes=4'),
        da, int64(a0), db, iif(a1='Y', 24, dcast(a1, int64(null)), aa, a2, ab, dcast(a3, int64(null))
      ),
    da, db, aa, ab)")
})


test_that("With explict field types, template cannot be provided at the same time", {
  op = get_aio_op('file_path', field_types = list(aa='string', ab='bool'))
  assert_afl_equal(op$to_afl(),
    "project(
      apply(
        aio_input('path=file_path', 'num_attributes=2'),
        aa, a0, ab, bool(a1)
      ),
    aa, ab)")
})

