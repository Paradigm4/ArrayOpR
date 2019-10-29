context("Test ArrayOpBase class")

# Fields ------------------------------------------------------------------

# For now, we only provide access to fields names.
# Due to SciDB details, we may need field types as well.

test_that("ArrayOpBase provides no fields by default.
Subclasses should implement fields differently depending on use cases", {
  sc = ArrayOpBase$new()
  expect_identical(sc$dims, as.character(c()))
  expect_identical(sc$attrs, as.character(c()))
  expect_identical(sc$selected, as.character(c()))
})

test_that("Owned fields are a union of dimensions and attributes", {
  sc = AnyArrayOp$new(array_expr = 'operand', dims = c('da', 'db'), attrs = c('aa', 'ab'), selected = c('sa', 'sb'))
  expect_identical(sc$dims_n_attrs, c('da', 'db', 'aa', 'ab'))
})


# Field types -----------------------------------------------------------------------------------------------------

test_that("Owned fields are a union of dimensions and attributes", {
  sc = AnyArrayOp$new(array_expr = 'operand', dims = c('da', 'db'), attrs = c('aa', 'ab'), selected = c('sa', 'sb'))
  expect_identical(sc$get_field_types(sc$dims_n_attrs), list(da = 'dt_da', db = 'dt_db', aa = 'dt_aa', ab = 'dt_ab'))
  expect_identical(sc$get_field_types('da'), list(da = 'dt_da'))
  expect_identical(sc$get_field_types('aa'), list(aa = 'dt_aa'))
  expect_identical(sc$get_field_types(c()), EMPTY_NAMED_LIST)
  expect_identical(sc$get_field_types('non-existent'), EMPTY_NAMED_LIST)
})


# Validate fields existence ---------------------------------------------------------------------------------------
# ArrayOpBase$get_absent_fields(fields) returns absent fields.
# The function relies on fields in sub-class
#
test_that("Base class has no fields, so any field is absent", {
  base = ArrayOpBase$new()
  expect_identical(base$get_absent_fields(c('a', 'b', 'c')), c('a', 'b', 'c'))
  expect_identical(base$get_absent_fields(c()), c())
})

test_that("Subclass with fields", {
  sc = AnyArrayOp$new(array_expr = 'operand', dims = c('da', 'db'), attrs = c('aa', 'ab'))
  expect_identical(sc$get_absent_fields(c('a', 'b')), c('a', 'b'))
  expect_identical(sc$get_absent_fields(c('da', 'non')), c('non'))
  expect_identical(sc$get_absent_fields(c('aa', 'db', 'non')), c('non'))
  expect_identical(sc$get_absent_fields(c('da', 'ab')), as.character(c()))
})


# Validate filter expressions -------------------------------------------------------------------------------------

test_that("Valid filter expression", {
  sc = AnyArrayOp$new(array_expr = 'operand', dims = c('da', 'db'), attrs = c('aa', 'ab'))
  for(filterExpr in list(
    e_merge(e(da == 1, aa == 'val'))
    # %like% is a special function that will translate to 'rsub' Scidb function
    , e_merge(e(da == 1, aa %like% 'val'))
    # We do not validate non-existent customized function
    , e_merge(e(da == 1, aa %non_existen_funct% 'val'))
  )){
    status = sc$validate_filter_expr(filterExpr)
    expect_true(status$success)
  }
})

test_that("Cannot filter on non-existent fields", {
  sc = AnyArrayOp$new(array_expr = 'operand', dims = c('da', 'db'), attrs = c('aa', 'ab'))
  for(status in list(
    sc$validate_filter_expr(e_merge(e(non == 1))),
    sc$validate_filter_expr(e_merge(e(non == 1, aa == 'val'))),
    sc$validate_filter_expr(e_merge(e(non == 1, db == 3)))
  )){
    expect_false(status$success)
    expect_identical(status$absent_fields, 'non')
  }
})

test_that("Cannot compare to non-atomic values", {
  sc = AnyArrayOp$new(array_expr = 'operand', dims = c('da', 'db'), attrs = c('aa', 'ab'))

  for(status in list(
    sc$validate_filter_expr(e_merge(e(da > !!list(1)))),
    sc$validate_filter_expr(e_merge(e(aa == !!list('val')))),
    sc$validate_filter_expr(e_merge(e(ab != !!ArrayOpBase$new())))
  )){

    expect_false(status$success)
    expect_false(.has_len(status$absent_fields))
    expect_true(.has_len(status$error_msgs))
  }
})

# AFL -------------------------------------------------------------------------------------------------------------

test_that("to_join_operand_afl and to_df_afl both detaul to to_afl", {
  x = AnyArrayOp$new('x', c('da', 'db', 'dc', 'dd'), c('aa', 'ab', 'ac', 'ad'))
  expect_identical(x$to_afl(), 'x')
  expect_identical(x$to_df_afl(), 'x')
  assert_afl_equal(x$to_join_operand_afl('da'), "x")
  assert_afl_equal(x$to_join_operand_afl(c('ad', 'aa')), "x")
  assert_afl_equal(x$to_join_operand_afl(c('da', 'aa', 'dc', 'ab')), "x")
})



