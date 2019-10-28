context("Test MatchOp")

# MatchOp requires 2 operands
# 1. a main ArrayOp, can be any ArrayOpBase sub-class
# 2. an R data frame or an ArrayOp, used as a template to reduce rows/cells in the main ArrayOp
# Operand 2 must have at-least one field/column matching the operand 1.


# Filter mode -----------------------------------------------------------------------------------------------------

test_that("Filter mode", {
  s = ArraySchema('operand', 'ns', dims = c('da', 'db'), attrs = c('aa', 'ab'))
  df = data.frame(da = c(1, 2))
  matchOp = MatchOp(s, df, op_mode = 'filter')
  assert_afl_equal(matchOp$to_afl(), "filter(ns.operand,
    da = 1 or da = 2
  )")

  df = data.frame(da = c(1, 2), ab = c('a', 'b'))
  matchOp = MatchOp(s, df, op_mode = 'filter')
  assert_afl_equal(matchOp$to_afl(), "filter(ns.operand,
    (da = 1 and ab = 'a') or (da = 2 and ab = 'b')
  )")
})

test_that("Filter mode is only available with an R data.frame template", {
  expect_error(MatchOp(
    ArraySchema('operand', 'ns', dims = 'd', attrs = 'a'),
    # The template cannot be an ArrayOp in filter mode,
    # otherwise we have to read its cell values to construct a filter expression
    ArraySchema('template', 'ns', dims = 'd', attrs = 'a'),
    op_mode = 'filter'
  ))
})

# Join mode -------------------------------------------------------------------------------------------------------

test_that("Join mode with a data frame template", {
  s = AnyArrayOp('operand', dims = c('da', 'db'), attrs = c('aa', 'ab'))
  df = data.frame(da = c(1, 2))
  matchOp = MatchOp(s, df, op_mode = 'join')
  assert_afl_equal(matchOp$to_afl(), sprintf("equi_join(operand, %s,
    'left_names=da', 'right_names=da'
    )", data.table::address(df))
  )

  # By default 'keep_dimensions=0', operand's dimension 'db' is dropped
  expect_identical(matchOp$get_field_types(c('da', 'db', 'aa', 'ab')), list(da='dt_da', aa='dt_aa', ab='dt_ab'))

  df = data.frame(da = c(1, 2), ab = c('a', 'b'))
  matchOp = MatchOp(s, df, op_mode = 'join')
  assert_afl_equal(matchOp$to_afl(), sprintf("equi_join(operand, %s,
    'left_names=da,ab', 'right_names=da,ab'
  )", data.table::address(df)))
  expect_identical(matchOp$get_field_types(c('da', 'db', 'aa', 'ab')),
                   list(da='dt_da', aa='dt_aa', ab='dt_ab'))
})

test_that("Join mode with an ArrayOp template", {
  s = AnyArrayOp('operand', dims = c('da', 'db'), attrs = c('aa', 'ab'))

  # Field names in the main and template should match. But whether they are dims or attrs doesn't matter
  for(t in list(
    ArraySchema('template', 'ns', dims = 'da', attrs = 'ab')
    , ArraySchema('template', 'ns', dims = 'non', attrs = c('da', 'ab'))
    , ArraySchema('template', 'ns', dims = c('da', 'ab'), attrs = 'non')
  )){
    matchOp = MatchOp(s, t, op_mode = 'join')
    assert_afl_equal(matchOp$to_afl(), "equi_join(operand, ns.template,
      'left_names=da,ab', 'right_names=da,ab'
      )")
    expect_identical(matchOp$get_field_types(c('da', 'db', 'aa', 'ab')),
                     list(da='dt_da', aa='dt_aa', ab='dt_ab'))
  }
})

test_that("Join mode with customized field matching", {
  s = ArraySchema('operand', 'ns', dims = c('da', 'db'), attrs = c('aa', 'ab'))

  t = ArraySchema('template', 'ns', dims = 'ta', attrs = 'tb')
  matchOp = MatchOp(s, t, op_mode = 'join', on_left = 'da', on_right = 'ta')
  assert_afl_equal(matchOp$to_afl(), "equi_join(ns.operand, ns.template,
      'left_names=da', 'right_names=ta'
  )")

  t = ArraySchema('template', 'ns', dims = 'ta', attrs = 'tb')
  matchOp = MatchOp(s, t, op_mode = 'join', on_left = c('da', 'ab'), on_right = c('ta', 'tb'))
  assert_afl_equal(matchOp$to_afl(), "equi_join(ns.operand, ns.template,
      'left_names=da,ab', 'right_names=ta,tb'
  )")
})

test_that("Join mode with a join_settings", {
  s = ArraySchema('operand', 'ns', dims = c('da', 'db'), attrs = c('aa', 'ab'))
  t = ArraySchema('template', 'ns', dims = 'da', attrs = 'non')
  matchOp = MatchOp(s, t, op_mode = 'join', settings = list(algorithm='hash'))
  assert_afl_equal(matchOp$to_afl(), "equi_join(ns.operand, ns.template,
      'left_names=da', 'right_names=da', 'algorithm=hash'
      )")
})


# cross_between mode ----------------------------------------------------------------------------------------------

test_that("ArrayOp template with matching fields", {
  s = ArraySchema('operand', 'ns', dims = c('da', 'db'), attrs = c('aa', 'ab'))
  t = ArraySchema('template', 'ns', dims = 'da', attrs = 'non')
  matchOp = MatchOp(s, t, op_mode = 'cross_between')
  assert_afl_equal(matchOp$to_afl(),
  "cross_between(
    ns.operand,
    project(
      apply(ns.template, _da_low, int64(da), _da_high, int64(da),
      _db_low, -9223372036854775807, _db_high, 9223372036854775807),
      _da_low, _db_low, _da_high, _db_high
    )
  )")
})

test_that("R data.frame template with matching fields ", {
  s = ArraySchema('operand', 'ns', dims = c('da', 'db'), attrs = c('aa', 'ab'))
  df = data.frame(da = c(1, 2))
  matchOp = MatchOp(s, df, op_mode = 'cross_between')
  assert_afl_equal(matchOp$to_afl(), sprintf(
  "cross_between(
    ns.operand,
    project(
      apply(%s, _da_low, int64(da), _da_high, int64(da),
      _db_low, -9223372036854775807, _db_high, 9223372036854775807),
      _da_low, _db_low, _da_high, _db_high
    )
  )", data.table::address(df)))
  df = data.frame(db = 'a', da = 42)
  matchOp = MatchOp(s, df, op_mode = 'cross_between')
  assert_afl_equal(matchOp$to_afl(), sprintf(
  "cross_between(
    ns.operand,
    project(
      apply(%s, _da_low, int64(da), _da_high, int64(da),
      _db_low, int64(db), _db_high, int64(db)),
      _da_low, _db_low, _da_high, _db_high
    )
  )", data.table::address(df)))
})

test_that("cross_between automatically preserve the main operand's fields", {
  s = ArraySchema('s', NULL, dims = c('rda', 'rdb'), attrs = c('raa', 'rab'))
  t = ArraySchema('t', NULL, dims = c('rda', 'rdb'), attrs = c('raa', 'rab', 'lda', 'ldb'))
  matchOp1 = MatchOp(s, t, op_mode = 'cross_between')
  matchOp2 = MatchOp(t, s, op_mode = 'cross_between')
  expect_identical(matchOp1$get_field_names(.OWN), s$get_field_names(.OWN))
  expect_identical(matchOp2$get_field_names(.OWN), t$get_field_names(.OWN))
  assert_afl_equal(MatchOp(s, t, op_mode = 'cross_between')$to_afl(),
    "cross_between(
      s,
      project(
        apply(t,_rda_low,int64(rda),_rda_high,int64(rda),_rdb_low,int64(rdb),_rdb_high,int64(rdb)),
        _rda_low,_rdb_low,_rda_high,_rdb_high
      )
    )")
  assert_afl_equal(MatchOp(t, s, op_mode = 'cross_between')$to_afl(),
    "cross_between(
      t,
      project(
        apply(s,_rda_low,int64(rda),_rda_high,int64(rda),_rdb_low,int64(rdb),_rdb_high,int64(rdb)),
        _rda_low,_rdb_low,_rda_high,_rdb_high
      )
    )")
})

test_that("explicitly specify field match between the main operand and the template", {
  s = ArraySchema('operand', NULL, dims = c('da', 'db'), attrs = c('aa', 'ab'))
  t = ArraySchema('template', NULL, dims = c('tda', 'tdb'), attrs = c('taa', 'tab'))

  # It shouldn't matter whether the template attributes are attributes or dimensions.
  # They will be converted to attributes eventually via 'apply' and 'project'
  assert_afl_equal(MatchOp(s, t, op_mode = 'cross_between', on_left = 'da', on_right = 'tda')$to_afl(),
    "cross_between(
      operand,
      project(
        apply(template, _da_low, int64(tda), _da_high, int64(tda),
                        _db_low, -9223372036854775807, _db_high, 9223372036854775807),
        _da_low, _db_low, _da_high, _db_high
      )
    )"
  )

  assert_afl_equal(MatchOp(s, t, op_mode = 'cross_between', on_left = 'da', on_right = 'tdb')$to_afl(),
    "cross_between(
      operand,
      project(
        apply(template, _da_low, int64(tdb), _da_high, int64(tdb),
                        _db_low, -9223372036854775807, _db_high, 9223372036854775807),
        _da_low, _db_low, _da_high, _db_high
      )
    )"
  )

  assert_afl_equal(MatchOp(s, t, op_mode = 'cross_between', on_left = 'da', on_right = 'taa')$to_afl(),
    "cross_between(
      operand,
      project(
        apply(template, _da_low, int64(taa), _da_high, int64(taa),
                        _db_low, -9223372036854775807, _db_high, 9223372036854775807),
        _da_low, _db_low, _da_high, _db_high
      )
    )"
  )

  # The order of keys should be accounted for in the 'apply' expression
  assert_afl_equal(MatchOp(s, t, op_mode = 'cross_between',
    on_left = c('da', 'db'), on_right = c('taa', 'tdb'))$to_afl(),
    "cross_between(
      operand,
      project(
        apply(template, _da_low, int64(taa), _da_high, int64(taa),
                        _db_low, int64(tdb), _db_high, int64(tdb)),
        _da_low, _db_low, _da_high, _db_high
      )
    )"
  )

  assert_afl_equal(MatchOp(s, t, op_mode = 'cross_between',
    on_left = c('db', 'da'), on_right = c('taa', 'tdb'))$to_afl(),
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


# Customized mode -------------------------------------------------------------------------------------------------

test_that("Customized AFL in place of the main operand", {
  s = AnyArrayOp('s', dims = c('da', 'db'), attrs = c('aa', 'ab'))
  afl = 'any AFL that results in the same schema as the main operand'
  matchOp1 = MatchOp(s, NULL, op_mode = 'customized', afl = afl)
  matchOp2 = MatchOp(s, 'anything', op_mode = 'customized', afl = afl)
  for(op in c(matchOp1, matchOp2)){
    expect_identical(op$get_field_names(.OWN), s$get_field_names(.OWN))
    expect_identical(op$get_field_names(.SEL), s$get_field_names(.SEL))
    expect_identical(op$to_afl(), afl)
  }
})

test_that("Customized AFL in place of the main operand", {
  s = AnyArrayOp('s', dims = c('da', 'db'), attrs = c('aa', 'ab'), selected = c('da', 'ab'))
  afl = 'any AFL that results in the same schema as the main operand'
  matchOp1 = MatchOp(s, NULL, op_mode = 'customized', afl = afl)
  matchOp2 = MatchOp(s, 'anything', op_mode = 'customized', afl = afl)
  for(op in c(matchOp1, matchOp2)){
    expect_identical(op$get_field_names(.OWN), s$get_field_names(.OWN))
    expect_identical(op$get_field_names(.SEL), s$get_field_names(.SEL))
    expect_identical(op$to_afl() , .afl(afl %project% 'ab') )
  }
})

