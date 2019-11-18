context("Test match function in newArrayOp base class")

newArrayOp = function(...) ArrayOpBase$new(...)

# Match with filter mode ------------------------------------------------------------------------------------------

MatchSource = newArrayOp("s", c("da", "db"), c("aa", "ab"), dtypes = list(aa='dtaa', ab='dtab', da='dtda', db='dtdb'))

test_that("Filter mode", {
  df = data.frame(da = c(1, 2))
  matchOp = MatchSource$match(df, op_mode = 'filter')
  assert_afl_equal(matchOp$to_afl(), "filter(s,
    da = 1 or da = 2
  )")
  expect_identical(MatchSource$dims_n_attrs, matchOp$dims_n_attrs)
  df = data.frame(da = c(1, 2), ab = c('a', 'b'))
  matchOp = MatchSource$match(df, op_mode = 'filter')
  assert_afl_equal(matchOp$to_afl(), "filter(s,
    (da = 1 and ab = 'a') or (da = 2 and ab = 'b')
  )")
  expect_identical(MatchSource$dims_n_attrs, matchOp$dims_n_attrs)
})

test_that("Filter mode with bounds", {
  df = data.frame(da = c(1, 2), db = c(3, 4))
  matchOp = MatchSource$match(df, op_mode = 'filter', lower_bound = list(da='da'), upper_bound = list(db='db'))
  assert_afl_equal(matchOp$to_afl(), "filter(s,
    (da >= 1 and db <= 3) or (da >= 2 and db <= 4)
  )")
  expect_identical(MatchSource$dims_n_attrs, matchOp$dims_n_attrs)
})

test_that("Filter mode with bounds on one field", {
  df = data.frame(da_low = c(1, 2), da_hi = c(3, 4))
  matchOp = MatchSource$match(df, op_mode = 'filter', lower_bound = list(da = 'da_low'), upper_bound = list(da = 'da_hi'))
  assert_afl_equal(matchOp$to_afl(), "filter(s,
    (da >= 1 and da <= 3) or (da >= 2 and da <= 4)
  )")
  expect_identical(MatchSource$dims_n_attrs, matchOp$dims_n_attrs)
})

test_that("Filter mode errors", {
  expect_error(MatchSource$match(
    newArrayOp('t', 'da', 'aa'),
    # The template cannot be an ArrayOp in filter mode,
    # otherwise we have to read its cell values to construct a filter expression
    op_mode = 'filter'
  ), "must be a data.frame")
  expect_error(MatchSource$match(
    data.frame(a = 1, non_matching_col = 'x'),
    op_mode = 'filter'
  ), "not matching")
  expect_error(MatchSource$match(
    data.frame(da = 1, db_low = 2, db_hi = 3), lower_bound = list(db = 'db_low'),
    op_mode = 'filter'
  ), "not matching")
  expect_error(MatchSource$match(
    data.frame(da = 1, db_low = 2, db_hi = 3), lower_bound = 'db_low',
    op_mode = 'filter'
  ), "named list")
})

# Match with cross_between mode -----------------------------------------------------------------------------------
MatchSource = newArrayOp("s", c("da", "db"), c("aa", "ab"), dtypes = list(aa='dtaa', ab='dtab', da='dtda', db='dtdb'))

test_that("cross_between on only matching dimensions", {
  for(t in c(
    newArrayOp('template', dims = 'da', attrs = 'non'),
    # Even there is matched attributes, they won't be included.
    newArrayOp('template', dims = 'da', attrs = c('aa', 'ab'))
  )){
    matchOp = MatchSource$match(t, op_mode = 'cross_between')
    assert_afl_equal(matchOp$to_afl(),
      "cross_between(
      s,
      project(
        apply(template, _da_low, int64(da), _da_high, int64(da),
        _db_low, -4611686018427387902, _db_high, 4611686018427387903),
        _da_low, _db_low, _da_high, _db_high
      )
    )")
  }
})

test_that("explicitly specify field match between the main operand and the template", {
  s = newArrayOp('operand', dims = c('da', 'db'), attrs = c('aa', 'ab'))
  t = newArrayOp('template', dims = c('tda', 'tdb'), attrs = c('taa', 'tab'))
  
  # It shouldn't matter whether the template attributes are attributes or dimensions.
  # They will be converted to attributes eventually via 'apply' and 'project'
  # assert_afl_equal(s$match(t, op_mode = 'cross_between', on_left = 'da', on_right = 'tda')$to_afl(),
  assert_afl_equal(s$match(t, op_mode = 'cross_between', field_mapping = list(da = 'tda'))$to_afl(),
    "cross_between(
      operand,
      project(
        apply(template, _da_low, int64(tda), _da_high, int64(tda),
                        _db_low, -4611686018427387902, _db_high, 4611686018427387903),
        _da_low, _db_low, _da_high, _db_high
      )
    )"
  )
})

test_that("cross_between with lower/upper bounds", {
  t = newArrayOp('template', dims = 'da', attrs = c('db_low', 'db_hi'))
  matchOp = MatchSource$match(t, op_mode = 'cross_between', 
    lower_bound = list(db = 'db_low'), upper_bound = list(db = 'db_hi'))
  assert_afl_equal(matchOp$to_afl(),
    "cross_between(
    s,
    project(
      apply(template, _da_low, int64(da), _da_high, int64(da), _db_low, db_low, _db_high, db_hi),
      _da_low, _db_low, _da_high, _db_high
    )
  )")
  # Only upper bound
  matchOp = MatchSource$match(t, op_mode = 'cross_between', upper_bound = list(db = 'db_hi'))
  assert_afl_equal(matchOp$to_afl(),
    "cross_between(
    s,
    project(
      apply(template, _da_low, int64(da), _da_high, int64(da), _db_low, -4611686018427387902, _db_high, db_hi),
      _da_low, _db_low, _da_high, _db_high
    )
  )")
  # Only lower bound
  matchOp = MatchSource$match(t, op_mode = 'cross_between', lower_bound = list(db = 'db_low'))
  assert_afl_equal(matchOp$to_afl(),
    "cross_between(
    s,
    project(
      apply(template, _da_low, int64(da), _da_high, int64(da), _db_low, db_low, _db_high, 4611686018427387903),
      _da_low, _db_low, _da_high, _db_high
    )
  )")
})

test_that("cross_between with customized lower/upper bounds (padding)", {
  t = newArrayOp('template', dims = 'da', attrs = c('db_low', 'db_hi'))
  matchOp = MatchSource$match(t, op_mode = 'cross_between', 
    lower_bound = list(db = 'db_low - 1234'), upper_bound = list(db = 'db_hi + 4231'))
  assert_afl_equal(matchOp$to_afl(),
    "cross_between(
    s,
    project(
      apply(template, _da_low, int64(da), _da_high, int64(da), _db_low, db_low - 1234, _db_high, db_hi + 4231),
      _da_low, _db_low, _da_high, _db_high
    )
  )")
  # Only upper bound
  matchOp = MatchSource$match(t, op_mode = 'cross_between', upper_bound = list(db = 'db_hi + 4231'))
  assert_afl_equal(matchOp$to_afl(),
    "cross_between(
    s,
    project(
      apply(template, _da_low, int64(da), _da_high, int64(da), _db_low, -4611686018427387902, _db_high, db_hi + 4231),
      _da_low, _db_low, _da_high, _db_high
    )
  )")
  # Only lower bound
  matchOp = MatchSource$match(t, op_mode = 'cross_between', lower_bound = list(db = 'db_low - 1234'))
  assert_afl_equal(matchOp$to_afl(),
    "cross_between(
    s,
    project(
      apply(template, _da_low, int64(da), _da_high, int64(da), _db_low, db_low - 1234, _db_high, 4611686018427387903),
      _da_low, _db_low, _da_high, _db_high
    )
  )")
  # lower/bound doesn't validate its expression
  matchOp = MatchSource$match(t, op_mode = 'cross_between', 
    lower_bound = list(db = 'any_expression_1'), upper_bound = list(db = 'any_expression_2'))
  assert_afl_equal(matchOp$to_afl(),
    "cross_between(
    s,
    project(
      apply(template, _da_low, int64(da), _da_high, int64(da), _db_low, any_expression_1, _db_high, any_expression_2),
      _da_low, _db_low, _da_high, _db_high
    )
  )")
})

test_that("explicitly specify field match between the main operand and the template", {
  s = newArrayOp('operand', dims = c('da', 'db'), attrs = c('aa', 'ab'))
  t = newArrayOp('template', dims = c('tda', 'tdb'), attrs = c('taa', 'tab'))
  
  # It shouldn't matter whether the template attributes are attributes or dimensions.
  # They will be converted to attributes eventually via 'apply' and 'project'
  # assert_afl_equal(s$match(t, op_mode = 'cross_between', on_left = 'da', on_right = 'tda')$to_afl(),
  assert_afl_equal(s$match(t, op_mode = 'cross_between', field_mapping = list(da = 'tda'))$to_afl(),
    "cross_between(
      operand,
      project(
        apply(template, _da_low, int64(tda), _da_high, int64(tda),
                        _db_low, -4611686018427387902, _db_high, 4611686018427387903),
        _da_low, _db_low, _da_high, _db_high
      )
    )"
  )
  
  # assert_afl_equal(s$match(t, op_mode = 'cross_between', on_left = 'da', on_right = 'tdb')$to_afl(),
  assert_afl_equal(s$match(t, op_mode = 'cross_between', field_mapping = list(da = 'tdb'))$to_afl(),
    "cross_between(
      operand,
      project(
        apply(template, _da_low, int64(tdb), _da_high, int64(tdb),
                        _db_low, -4611686018427387902, _db_high, 4611686018427387903),
        _da_low, _db_low, _da_high, _db_high
      )
    )"
  )
  
  # assert_afl_equal(s$match(t, op_mode = 'cross_between', on_left = 'da', on_right = 'taa')$to_afl(),
  assert_afl_equal(s$match(t, op_mode = 'cross_between', field_mapping = list(da = 'taa'))$to_afl(),
    "cross_between(
      operand,
      project(
        apply(template, _da_low, int64(taa), _da_high, int64(taa),
                        _db_low, -4611686018427387902, _db_high, 4611686018427387903),
        _da_low, _db_low, _da_high, _db_high
      )
    )"
  )
  
  # The order of keys should be accounted for in the 'apply' expression
  assert_afl_equal(s$match(t, op_mode = 'cross_between',
    # on_left = c('da', 'db'), on_right = c('taa', 'tdb'))$to_afl(),
    field_mapping = list(da = 'taa', db = 'tdb'))$to_afl(),
    "cross_between(
      operand,
      project(
        apply(template, _da_low, int64(taa), _da_high, int64(taa),
                        _db_low, int64(tdb), _db_high, int64(tdb)),
        _da_low, _db_low, _da_high, _db_high
      )
    )"
  )
  
  assert_afl_equal(s$match(t, op_mode = 'cross_between',
    # on_left = c('db', 'da'), on_right = c('taa', 'tdb'))$to_afl(),
    field_mapping = list(db = 'taa', da = 'tdb'))$to_afl(),
    "cross_between(
      operand,
      project(
        apply(template, _da_low, int64(tdb), _da_high, int64(tdb),
                        _db_low, int64(taa), _db_high, int64(taa)),
        _da_low, _db_low, _da_high, _db_high
      )
    )"
  )
})

test_that("upper/lower bound can not overlap with field_mapping", {
  t = newArrayOp('template', dims = c('da'), attrs = c('db_hi', 'db_low'))
  expect_error(MatchSource$match(t, op_mode = 'cross_between', field_mapping = list(da = 'da', db = 'db_hi'),
    lower_bound = list(db = 'db_low'), upper_bound = list(db = 'db_hi')), "cannot overlap")
  expect_error(MatchSource$match(t, op_mode = 'cross_between', field_mapping = list(da = 'da', db = 'db_hi'),
    upper_bound = list(db = 'db_hi')), "cannot overlap")
  expect_error(MatchSource$match(t, op_mode = 'cross_between', field_mapping = list(da = 'da', db = 'db_hi'),
    lower_bound = list(db = 'db_low')), "cannot overlap")
})

test_that("cross_between mode only works on dimensions", {
  MatchSource = newArrayOp("s", c("da", "db"), c("aa", "ab"))
  t = newArrayOp('template', dims = c('x'), attrs = c('aa', 'ab'))
  expect_error(MatchSource$match(t, op_mode = 'cross_between'), "source's dimensions")
  expect_error(MatchSource$match(t, op_mode = 'cross_between', field_mapping = list(aa = 'aa')), "source's dimensions")
})
