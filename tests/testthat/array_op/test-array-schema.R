context("Test ArraySchema")

test_that("operand is the fully qualified array name", {
  s = ArraySchema$new(array_name = 'operand', namespace = 'ns', dims = 'da', attrs = 'ab')
  expect_identical(s$to_afl(), 'ns.operand')

  # Namespace defaults to public
  s = ArraySchema$new(array_name = 'operand', dims = 'da', attrs = 'aa')
  expect_identical(s$to_afl(), 'public.operand')

  # In case we don't need a namespace, set it to NULL
  s = ArraySchema$new(array_name = 'operand', namespace = NULL, dims = 'da', attrs = 'aa')
  expect_identical(s$to_afl(), 'operand')
})

test_that("Attrs/Dims are directly stored. No selected_fields.", {
  s = ArraySchema$new(array_name = 'operand', namespace = 'ns', dims = 'da', attrs = 'aa')
  expect_identical(s$dims, 'da')
  expect_identical(s$attrs, 'aa')
  expect_identical(s$dims_n_attrs, c('da', 'aa'))
  expect_identical(s$selected, as.character(c()))
})

test_that("Other to*afl() returns to_afl()", {
  s = ArraySchema$new(array_name = 'operand', namespace = 'ns', dims = 'da', attrs = 'ab')
  expect_identical(s$to_df_afl(), 'ns.operand')
  expect_identical(s$to_join_operand_afl('da'), 'ns.operand')
  expect_identical(s$to_join_operand_afl('ab'), 'ns.operand')
})

test_that("ArraySchema used as an operand in another ArrayOp subclass", {
  s = ArraySchema$new(array_name = 'operand', namespace = 'ns', dims = 'da', attrs = 'ab')
  subsetOp = SubsetOp$new(s, filter_expr = e(da > 0, ab == 'val'))
  assert_afl_equal(subsetOp$to_afl(), "filter(ns.operand, da > 0 and ab = 'val')")
})
