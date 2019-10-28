context('Test expression utility functions')
library(rlang)

# Terms definition ------------------------------------------------------------------------------------------------

# ExprsList: a list of expressions (which can used in SubsetOp). Please note: names(ExprsList) == NULL


# e(...) creates an ExprsList -------------------------------------------------------------------------------------

test_that('e(...) generates an ExprsList without evaluating args unless specified by !!', {
  ans = 42
  expect_identical(e(a > 3, b > 4, c == ans), list(expr(a>3), expr(b>4), expr(c == ans)))
  expect_identical(e(a > 3, b > 4, c == !!ans), list(expr(a>3), expr(b>4), expr(c == 42)))
  expect_identical(e(), list())
  # Do not use named args, e.g. e(a=3), because it creates confusion in client code
  expect_error(e(a=3, b=4), 'equality tests')
})

test_that('API functions can use ... to specify a filter ExprsList', {
  # API functions can include ... as a part of the whole arg list
  api_func <- function(name, ..., opt1 = 1, opt2 = 'a') {
    return(e(...))
  }

  ans = 42
  expect_identical(
    api_func('schemaName', a > 3, b > 4, c == ans),
    list(expr(a>3), expr(b>4), expr(c == ans)) )
  expect_identical(
    api_func('name', a > 3, b > 4, c == !!ans, opt1 = 42, opt2 = 'b'),
    list(expr(a>3), expr(b>4), expr(c == 42)) )
  # Cannot use extra named args.
  expect_error(api_func('name', a = 3), 'equality tests')
  # Defined named args are allowed
  expect_identical(api_func('name', opt1=33), list())
})

test_that('API functions can use ... and a named arg that is an ExprsList', {
  # API functions may need two or more filter ExprsList in cases of joining multiple arrays
  api_func <- function(..., secFilter, opt1=1, opt2=2){
    list(e(...), secFilter)
  }

  ans = 42
  filters = api_func(a == b, q == ans, secFilter = e(c > d, ans == !!ans), opt2 = 4)
  expect_identical(filters[[1]], c(expr(a == b), expr(q == ans)))
  expect_identical(filters[[2]], c(expr(c > d), expr(ans == 42)))
})

test_that('API functions can combine e(...) and a pre-defined ExprsList', {
  # API functions may need two or more filter ExprsList in cases of joining multiple arrays
  api_func <- function(..., opt1=1, opt2=2){
    pre = e(score > 90)
    c(pre, e(...))
  }

  ans = 42
  combinedFilterExpr = api_func(ans == !!ans)
  expect_identical(combinedFilterExpr, c(expr(score > 90), expr(ans == 42)))
})

# e and .el_opts_from_triple_dots convert ... to ExprsList -------------------------------------

test_that('e(...) returns a list of expressions', {
  expect_identical(e(a == 3), list(expr(a == 3)))
  # == and similarly other arithmetic operators are converted into the same standard calls
  expect_identical(e(a == 3), list(expr(`==`(a, 3))))
  expect_identical(e(a == 3, c == d), list(expr(a == 3), expr(c == d)))

  # Functions don't have to be pre-defined since they are not evaluted in expression
  expect_identical(e(absent_func(42)), c(expr(absent_func(42))))
})


test_that("User variable substitution is supported", {
  ans = 42
  # Without special notations, any name is treated as abstract
  expect_identical(e(question() == ans), c(expr(question() == ans)))
  # We can use !! to replace the name with actual bound value (ie. substitute in R terms)
  expect_identical(e(question() == !!ans), c(expr(question() == 42)))

  name = 'jane' # single or double quote are equivalent
  expect_identical(e(find(!!name)), c(expr(find('jane'))))
  expect_identical(e(find(!!name)), c(expr(find("jane"))))
})

test_that("Identity of expressions is only defined by the abstract syntax tree (AST) so white spaces don't matter", {
  expect_identical(e(greet(person, msg())), c(expr(
    greet(person,
          msg())
    )))

  # But paranthese matter because `(` is also a valid function
  expect_true(!identical( e(greet()) , c(expr( (greet()) )) ))
  expect_true(identical( e((greet())) , c(expr( (greet()) )) ))
  expect_true(identical( e(greet()) , c(expr( greet() )) ))
  expect_identical(e( `(`(greet()) ), c(expr( (greet()) )))
})



# .arg_to_expressions ---------------------------------------------------------------------------------------------

test_that("Convert a named list to an ExprsList", {
  check <- function(argsList, expectedExprList) {
    converted <- do.call(.args_to_expressions, argsList)
    expect_identical(converted, expectedExprList)
  }

  check(list(), e())
  check(list(a=3, b='B', c=4), e(a==3, b=='B', c==4))
  check(list(a_range = c(1,2), b_contains = 'substr', c = c('a', 'b')),
        e(a >= 1, a <= 2, b %like% '.*substr.*', c %in% !!c('a', 'b')))

  # If a range bound is NA, do not create an expression.
  check(list(a_range = c(NA, 2), b_range = c(1, NA)), e(a <= 2, b >= 1))
})
