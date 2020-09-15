context('Test expression utility functions')

expr = rlang::expr
e = aflutils$e
args_to_expressions = aflutils$args_to_expressions
e_to_afl = aflutils$e_to_afl

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

# e convert ... to ExprsList -------------------------------------

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
    converted <- do.call(args_to_expressions, argsList)
    expect_equal(converted, expectedExprList)
  }

  check(list(), e())
  check(list(a=3, b='B', c=4), e(a==3, b=='B', c==4))
  check(list(a_contains = 'a', b_contains = c('a', 'b')), e(a %like% '.*a.*', b %like% c('.*a.*', '.*b.*')))
  check(list(a_range = c(1,2), b_contains = 'substr', c = c('a', 'b')),
        e(AND(a >= 1, a <= 2), b %like% '.*substr.*', c %in% !!c('a', 'b')))

  # If a range bound is NA, do not create an expression.
  check(list(a_range = c(NA, 2), b_range = c(1, NA)), e(a <= 2, b >= 1))
  
  check(list(a = T, b = F, c = TRUE, d = FALSE), e(a == TRUE, b == FALSE, c == TRUE, d == FALSE))
  check(list(a = NA, b_not = NA), e(is_null(a), is_not_null(b)))
  check(list(a = NULL, b_not = NULL), e(is_null(a), is_not_null(b)))
})

test_that("Explicitly provide a param list", {
  expect_equal(args_to_expressions(.dots = list(a=3, b=T, c=c('abc', 'def'), d_range=c(3,4))),
                   e(a==3, b==TRUE, c %in% c('abc', 'def'), AND(d >= 3, d <= 4)))
})

test_that("Error cases", {
  expect_error(args_to_expressions(a_range = c(2, 1)), "Illegal range values")
})



# e_to_afl --------------------------------------------------------------------------------------------

test_that("Generate afl filter expressions from R expressions", {
  assert_afl_equal(e_to_afl(e(a == TRUE)), "a = true")
  assert_afl_equal(e_to_afl(e(a == !!T)), "a = true")
  assert_afl_equal(e_to_afl(e(a == FALSE)), "a = false")
  assert_afl_equal(e_to_afl(e(a == !!F)), "a = false")
  
  assert_afl_equal(e_to_afl(e(a == 42)), "a = 42")
  assert_afl_equal(e_to_afl(e(is_null(a))), "a is null")
  assert_afl_equal(e_to_afl(e(not_null(a))), "a is not null")
  assert_afl_equal(e_to_afl(e(is_null(a), b != 0)), "a is null and b <> 0")
  
  assert_afl_equal(e_to_afl(e(a %in% !!c(1,2,3))), "(a = 1 or a = 2 or a = 3)")
  assert_afl_equal(e_to_afl(e(a %like% '.+a.+'), regex_func = 'rsub'), "(a <> '' and rsub(a, 's/.+a.+//i') = '')")
  
  assert_afl_equal(e_to_afl(e((a + b + "c") == 'value')),
                   "(a + b + 'c') = 'value'")
})

test_that("Regular expressions", {
  # rsub mode
  assert_afl_equal(e_to_afl(e(field %like% '.+a.+'), regex_func = 'rsub'), 
                   "(field <> '' and rsub(field, 's/.+a.+//i') = '')")
  assert_afl_equal(e_to_afl(e(field %like% '.+a.+'), regex_func = 'rsub'), 
                   "(field <> '' and rsub(field, 's/.+a.+//i') = '')")
  assert_afl_equal(e_to_afl(e(field %like% '.+a.+'), regex_func = 'rsub', ignore_case = F), 
                   "(field <> '' and rsub(field, 's/.+a.+//') = '')")
  
  # regex mode
  assert_afl_equal(e_to_afl(e(field %like% '.+a.+'), regex_func = 'regex'), 
                   "regex(field, '(?i).+a.+')")
  assert_afl_equal(e_to_afl(e(field %like% '.+a.+'), regex_func = 'regex', ignore_case = T), 
                   "regex(field, '(?i).+a.+')")
  assert_afl_equal(e_to_afl(e(field %like% '.+a.+'), regex_func = 'regex', ignore_case = F), 
                   "regex(field, '.+a.+')")
  
  # Other string match functions derived from %like%
  assert_afl_equal(e_to_afl(e(field %contains% 'a'), regex_func = 'rsub'), 
                   "(field <> '' and rsub(field, 's/.*a.*//i') = '')")
  assert_afl_equal(e_to_afl(e(field %contains% 'a'), regex_func = 'rsub', ignore_case = F), 
                   "(field <> '' and rsub(field, 's/.*a.*//') = '')")
  assert_afl_equal(e_to_afl(e(field %contains% 'a'), regex_func = 'regex'), 
                   "regex(field, '(?i).*a.*')")
  assert_afl_equal(e_to_afl(e(field %starts_with% 'a'), regex_func = 'regex', ignore_case = F), 
                   "regex(field, 'a.*')")
  assert_afl_equal(e_to_afl(e(field %ends_with% 'a'), regex_func = 'regex', ignore_case = T), 
                   "regex(field, '(?i).*a')")
  
  # escape special characters in contains, starts_with and ends_with
  assert_afl_equal(e_to_afl(e(field %contains% ' a*b(par\\en)\\ '), regex_func = 'regex'), 
                   "regex(field, '(?i) .*a\\*b\\(par\\\\\\en\\)\\\\\\ .*')")
  assert_afl_equal(e_to_afl(e(field %starts_with% ' ]"[ '), regex_func = 'regex'), 
                   "regex(field, '(?i) \\]\"\\[ .*')")
  assert_afl_equal(e_to_afl(e(field %ends_with% "a'a"), regex_func = 'regex'), 
                   "regex(field, '(?i).*a\\'a')")
  assert_afl_equal(e_to_afl(e(field %ends_with% "a'a"), regex_func = 'rsub'), 
                   "(field <> '' and rsub(field, 's/.*a\\'a//i') = '')")
  
  
  # Error regex_func mode
  expect_error(e_to_afl(e(field %like% '.+a.+'), regex_func = 'non-existent'), "Unknown regex function")
  expect_error(e_to_afl(e(field %like% !!c('a', 'b'))), "must be a single string")
  
})

test_that("Generate complex filter expression with logical operators", {
  # NOTE: &&/& takes precedence over ||/|  
  assert_afl_equal(e_to_afl(e(a == 3, b == 'val')), "a = 3 and b = 'val'")
  assert_afl_equal(e_to_afl(e(a == 3 && b == 'val')), "a = 3 and b = 'val'")
  assert_afl_equal(e_to_afl(e(a == 3 & b == 'val')), "a = 3 and b = 'val'")
  assert_afl_equal(e_to_afl(e(a == 3 || b == 'val')), "a = 3 or b = 'val'")
  assert_afl_equal(e_to_afl(e(a == 3 | b == 'val')), "a = 3 or b = 'val'")
  
  assert_afl_equal(e_to_afl(
    e(AND(a == 3, b == 4, c == 5))), 
    "a = 3 and b = 4 and c = 5")
  
  assert_afl_equal(e_to_afl(
    e(OR(a == 3, b == 4, c == 5))), 
    "a = 3 or b = 4 or c = 5")
  
  # Use paranthese to change operands association
  assert_afl_equal(e_to_afl(
    e( a == 3 & (b == 4 | c == 5) )),
    "a = 3 and (b = 4 or c = 5)")
  # More verbose way to do the same as above. Notice the parathenses around OR operator. 
  # If ommitted, no paratheses are generated
  assert_afl_equal(e_to_afl(
    e( AND(a == 3,  (OR(b == 4 | c == 5))) )),
    "a = 3 and (b = 4 or c = 5)")
})

