context('Test ArgList class')


api_func = function(a, b, cc =3, ..., x, y = FALSE, z){
  newArgList(default_args = list(cc=cc, y=y))
}

outer_api_func = function(a, b, ..., x = 'x') {
  api_func(a = a, b = b, x = x, ...)
}

test_that('Length of ArgList is the count of detected args', {
  validate_length = function(argList, explicit, expectedCount) {
    if(missing(explicit))
      expect_equal(argList$get_len(), expectedCount)
    else
      expect_equal(argList$get_len(explicit), expectedCount)

  }
  # Default length accounts for all effective args
  validate_length(api_func(), expectedCount = 2)
  validate_length(api_func(1, 2), expectedCount = 4)
  validate_length(api_func(1, cc =2), expectedCount = 3)
  validate_length(api_func(cc =1, 2), expectedCount = 3)
  validate_length(api_func(a = 1, b = 2, z = 3), expectedCount = 5)

  # Default length accounts for all effective args

  validate_length(api_func(), T, 0)
  validate_length(api_func(1, 2), T, 2)
  validate_length(api_func(1, cc =2), T, 2)
  validate_length(api_func(cc =1, 2), T, 2)
  validate_length(api_func(a = 1, b = 2, z = 3), T, 3)
})


test_that('names(ArgList) returns effective args by default', {
  values = c('a', 'b', 'extra')
  expect_identical(names(api_func()), c('cc', 'y'))
  expect_identical(names(api_func(1, 2, 5)), c('a', 'b', 'cc', 'y'))
  expect_identical(names(api_func(1, 2, extra = 'extra')), c('a', 'b', 'extra', 'cc', 'y'))
  expect_identical(names(api_func(1, 2, extra = values[[3]])), c('a', 'b', 'extra', 'cc', 'y'))
})

test_that('Dected args are stored for [[ access', {
  al = api_func(1, 2, 5)
  expect_true(al[['a']] == 1)
  expect_true(al[['b']] == 2)
  expect_true(al[['cc']] == 5)

  expect_null(al[['x']])
  expect_identical(al[['y']], FALSE)
  expect_null(al[['z']])
  expect_null(al[['non_pre_defined_arg']])

  expect_true(al$has_args_count(c('a', 'b', 'cc', 'y'), 4))
  expect_true(al$has_args_count(c('a', 'b', 'cc', 'y'), 3, T))
  expect_true(al$has_args_count(c('x', 'y', 'cc'), 2))
  expect_true(al$has_args_count(c('x', 'y', 'cc'), 1, T))
})

test_that('Args can be converted to an ExprsList', {
  # Triple dots can include flexible field search patterns, e.g. _contains, _range,
  # implemented in args_to_expressions(...)

  expect_identical(api_func()$to_exprs_list(), e(cc == 3, y == FALSE))
  expect_identical(api_func(a = 42)$to_exprs_list(), e(a == 42, cc == 3, y == FALSE))
  expect_identical(api_func(extra = 42)$to_exprs_list(), e(extra == 42, cc == 3, y == FALSE))
  expect_identical(api_func(extra_contains = 'sub')$to_exprs_list(), e(extra %like% '.*sub.*', cc == 3, y == FALSE))

  expect_identical(api_func()$to_exprs_list(T), e())
  expect_identical(api_func(a = 42)$to_exprs_list(T), e(a == 42))
  expect_identical(api_func(extra = 42)$to_exprs_list(T), e(extra == 42))
  expect_identical(api_func(extra_contains = 'sub')$to_exprs_list(T), e(extra %like% '.*sub.*'))

})

test_that('Args can be converted to an ExprsList even when there are nested function calls', {

  expect_identical(outer_api_func()$to_exprs_list(), e(x == 'x', cc == 3, y == FALSE))
  expect_identical(outer_api_func(a = 42)$to_exprs_list(), e(a == 42, x == 'x', cc == 3, y == FALSE))
  expect_identical(outer_api_func(42)$to_exprs_list(), e(a == 42, x == 'x', cc == 3, y == FALSE))
  expect_identical(outer_api_func(extra = 42)$to_exprs_list(), e(extra == 42, x == 'x',  cc == 3, y == FALSE))
  expect_identical(outer_api_func(extra_contains = 'sub')$to_exprs_list(), e(extra %like% '.*sub.*', x == 'x', cc == 3, y == FALSE))

})


test_that('Get a subset of ArgList', {
  al = api_func(a = 1, b = 2, x = 'x')
  expect_true(al$has_all_args(c('a', 'b', 'x', 'cc', 'y')))
  expect_true(al$has_all_args(c('a', 'b', 'x'), T))

  als = al$subset(c('a', 'cc', 'z'))
  expect_equal(als$get_len(), 2)
  expect_equal(als$get_len(T), 1)
  expect_true(als$has_all_args(c('a', 'cc')))
  expect_true(als$has_all_args(c('a'), T))

  als = al$subset(c('a', 'z'), inverse = TRUE)
  expect_equal(als$get_len(), 4)
  expect_equal(als$get_len(T), 2)
  expect_true(als$has_all_args(c('b', 'x', 'cc', 'y')))
  expect_true(als$has_all_args(c('b', 'x'), T))

})

test_that('Edge cases in deeply nested function calls', {
  # This is just to test that ArgList class can handle such deeply nested function calls
  # compounded with partial name matching.
  # But we should NOT do this because it's hard to reason about.
  # Pass ArgList isntances instead.

  func_level1 = function(aa, ...){
    func_level2(aa = aa, ab = 'AB', ...)
  }

  func_level2 = function(aa, ab, ac = 'AC', ...){
    func_level3(ab = ab, aa = aa, ac = ac, ax = 'AX', ...)
  }

  func_level3 = function(aa, ab = 'inner ab', ac, ax, ext1, ext2, long_arg, ...){
    newArgList()
  }

  argList = func_level1()
  expect_identical(names(argList), c('ab', 'ac', 'ax'))
  # Default value is passed in from the most top caller function, not the most-inner one.
  expect_identical(argList[['ab']], 'AB')

  argList = func_level2()
  expect_null(argList[['ab']])  # Outer function masks the default argument value of the inner function!

  # func_level1(extra = 'Extra') does NOT detedt potential errors because
  #  primitive values are built into function calls, whereas non-primitive values are passed as indirect expressions
  # 'extra' arg is never formally defined in any function
  argList = func_level1(extra = c('a', 'b'))
  expect_identical(names(argList), c('ab', 'ac', 'ax', 'extra'))
  expect_identical(argList[['ax']], 'AX')

  # ext1 and ext2 exist as args in the most inner function, but not defined formally in the caller functions
  argList = func_level1(ext1 = c('a', 'b'), ext2 = c('cc', 'd'))
  expect_identical(names(argList), c('ab', 'ac', 'ax', 'ext1', 'ext2'))
  expect_identical(argList[['ext1']], c('a', 'b'))
  expect_identical(argList[['ext2']], c('cc', 'd'))

  argList = func_level1(long = c('a', 'b'))
  expect_identical(names(argList), c('ab', 'ac', 'ax', 'long_arg'))
  expect_identical(argList[['long_arg']], c('a', 'b'))

})


# ArgList tests arguments presense according to rules --------------------------------------------------------------------

test_that('Arglist tests arguments presense', {
  al = api_func(a = 1, b = c(2, 3, 4))
  expect_true(al$has_all_args(c('a', 'b')))
  expect_true(al$has_all_args(c('a', 'b', 'cc', 'y')))
  expect_identical(al$has_all_args(c('a', 'b', 'non')), FALSE)
  expect_identical(al$has_any_args(c('a', 'b', 'non')), TRUE)

  expect_equal(al$args_count(c('aa', 'bb', 'non')), 0)
  expect_equal(al$args_count(c('a', 'b', 'non')), 2)
})

test_that('Arglist tests arguments multiplicity', {
  al = api_func(a = 1, b = c(2, 3, 4), multi = c('a', 'b', 'cc'), scalar = 'single value')
  expect_false(al$has_scalar_args(c('a', 'b')))
  expect_false(al$has_multi_len_args(c('a', 'b')))
  # If there are non-existent args, has_scalar_args and has_multi_len_args both return FALSE
  expect_false(al$has_scalar_args(c('a', 'non')))
  expect_false(al$has_multi_len_args(c('non', 'b')))

  expect_true(al$has_scalar_args(c('a', 'scalar')))
  expect_true(al$has_multi_len_args(c('b', 'multi')))
})


# ArgList$rename(...) returns a new ArgList -----------------------------------------------------------------------

test_that('ArgList$rename(...) returns a new ArgList', {
  al = api_func(a = 1, b = 2, cc = 3, y = TRUE)
  expect_true(al$has_all_args(c('a', 'b', 'cc', 'y')))

  new_al = al$rename('a', 'AA', 'b', 'BB', 'y', 'YY')
  expect_true(new_al$has_all_args(c('AA', 'BB', 'cc', 'YY')))
  expect_equal(new_al[['AA']], 1)
  expect_equal(new_al[['BB']], 2)
  expect_equal(new_al[['YY']], TRUE)
  # Ensure old args that were renamed no longer exist.
  expect_true(!new_al$has_any_args(c('a', 'b', 'y')))

  # The old ArgList was not changed at all.
  expect_true(al$has_all_args(c('a', 'b', 'cc', 'y')))
})


# Convenience args assertion functions ----------------------------------------------------------------------------

test_that('Test assert_exclusive_args', {
  al = api_func(a = 1, b = 2, cc = 3, y = TRUE)
  al$assert_exclusive_args(c('a', 'non'))
  al$assert_exclusive_args(c('non1', 'non2'))
  expect_error(al$assert_exclusive_args(c('a', 'b', 'y')), "Argument 'a' cannot be specified with 'b'")
  expect_error(al$assert_exclusive_args(c('a', 'x', 'y')))
  expect_error(al$assert_exclusive_args(c('b', 'non1', 'non2', 'y')))
  expect_error(al$assert_exclusive_args(c('b', 'non1', 'non2', 'a')))
})
