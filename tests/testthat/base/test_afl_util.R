context("Test AFL utility functions")

anArray = ArrayOpBase$new('array', dims = c('da', 'db'), attrs = c('aa', 'ab'))

# afl(...)  -------------------------------------------------------------------------------------------------------
# R special infix functions %...% are translated to scidb operators
# 
test_that("Supported operand types: character, numeric, logical, NULL and ArrayOp classes", {
  expect_identical(afl('a' %op% 'b'), "op(a,b)")
  # numeric operands are converted to strings
  expect_identical(afl(3 %op% 4), "op(3,4)")
  expect_identical(afl(3.1415 %op% 2.17), "op(3.1415,2.17)")
  # logical values are converted to theri lower case string representations
  expect_identical(afl('array' %op% c(T, F, T)), "op(array,true,false,true)")
  # NULL is handled specially
  expect_identical(afl('a' %op% NULL), "op(a)")
  expect_identical(afl('a' %op% .), "op(a)")
  # Association from left to right when there are multiple operators
  expect_identical(afl('a' %op% 'b' %op2% 'c'), "op2(op(a,b),c)")
  # strings can be quoted
  expect_identical(afl("'a'" %op% "'b'"), "op('a','b')")
  
})


test_that("ArrayOp instance automatically converted to string with to_afl() method", {
  # ArrayOp is evaluted to character with its `to_afl()` method
  expect_identical(afl(anArray %filter% 'true'), "filter(array,true)")
  # Or call ArrayOp$to_afl() explicitly
  expect_identical(afl(anArray$to_afl() %filter% 'true'), "filter(array,true)")
})

test_that("Unsupported data types", {
  expect_error(afl(list()), "unsupported operand data type")
  expect_error(afl(data.frame()), "unsupported operand data type")
})

test_that("Use case for common scidb operators", {
  newFields = c('a', 'b')
  newFiledExpressions = c('a_expr', 'b_expr')
  expect_identical(afl(anArray %apply% afl_join_fields(newFields, newFiledExpressions)), 
    "apply(array,a,a_expr,b,b_expr)")
  
  projectedFields = c('a', 'b')
  expect_identical(afl(anArray %project% projectedFields), "project(array,a,b)")
  
  # Add NULL for unary operator
  expect_identical(afl(anArray %op_count% NULL), "op_count(array)")
  
  expect_identical(afl(anArray %grouped_aggregate% c('count(*)', 'chrom', 'pos', 'ref', 'alt')),
    "grouped_aggregate(array,count(*),chrom,pos,ref,alt)")
})

test_that("Compose complex AFL by chaining operators and nesting afl(...)", {
  assert_afl_equal(afl(
    anArray %apply% c('a', 'a', 'b', 'b') 
      %filter% "a > 1 and b = 'bval'"
      %project% c('a', 'b')
    %equi_join%
      c(afl('another_array' %filter% 'x = 1'),
        "'left_names=a,b', 'right_names=x,y'")
    %project%
      c('a', 'b')
    ),
    "project(
      equi_join(
        project(
          filter(
                apply(array, a, a, b, b), 
                a > 1 and b = 'bval'
                ),
          a, b
        ),
        filter(
          another_array,
          x = 1
        ),
        'left_names=a,b',
        'right_names=x,y'
      ), 
      a, b
    )"
  )
})

# afl2(...)  -------------------------------------------------------------------------------------------------------
# A clener afl statement generator function
# 
test_that("Supported operand types: character, numeric, logical, NULL and ArrayOp classes", {
  expect_identical(afl2('a' | op), "op(a)")
  expect_identical(afl2('a' | op()), "op(a)")
  
  expect_identical(afl2('a' | op('b')), "op(a,b)")
  expect_identical(afl2('a' | op('b', 40+2)), "op(a,b,42)")
  # numeric operands are converted to strings
  expect_identical(afl2(3 | op(4)), "op(3,4)")
  expect_identical(afl2(3.1415 | op(2.17)), "op(3.1415,2.17)")
  # logical values are converted to theri lower case string representations
  expect_identical(afl2('array'| op(T, F, T)), "op(array,true,false,true)")
  # Association from left to right when there are multiple operators
  expect_identical(afl2('a' | op('b') | op2('c')), "op2(op(a,b),c)")
  # strings can be quoted
  expect_identical(afl2("'a'" | op("'b'")), "op('a','b')")
  
})

anArray = ArrayOpBase$new('array', dims = c('da', 'db'), attrs = c('aa', 'ab'))

test_that("ArrayOp instance automatically converted to string with to_afl() method", {
  # ArrayOp is evaluted to character with its `to_afl()` method
  expect_identical(afl2(anArray | filter('true')), "filter(array,true)")
  # Or call ArrayOp$to_afl() explicitly
  expect_identical(afl2(anArray$to_afl() | filter('true')), "filter(array,true)")
})

test_that("Unsupported data types", {
  expect_error(afl2(list()), "unsupported operand data type")
  expect_error(afl2(data.frame()), "unsupported operand data type")
})

test_that("Use case for common scidb operators", {
  newFields = c('a', 'b')
  newFiledExpressions = c('a_expr', 'b_expr')
  expect_identical(afl2(anArray | apply(paste(newFields, newFiledExpressions, collapse = ',', sep = ','))), 
    "apply(array,a,a_expr,b,b_expr)")
  
  projectedFields = c('a', 'b')
  expect_identical(afl2(anArray | project(projectedFields)), "project(array,a,b)")
  
  # Add NULL for unary operator
  expect_identical(afl2(anArray | op_count), "op_count(array)")
  
  expect_identical(afl2(anArray | grouped_aggregate('count(*)', 'chrom', 'pos', 'ref', 'alt')),
    "grouped_aggregate(array,count(*),chrom,pos,ref,alt)")
})

test_that("Compose complex AFL by chaining operators and nesting afl(...)", {
  assert_afl_equal(afl2(
    anArray | 
      apply('a', 'a', 'b', 'b') |
      filter("a > 1 and b = 'bval'") |
      project('a', 'b') |
    equi_join(
      'another_array' | filter('x = 1'),
        "'left_names=a,b'", 
        "'right_names=x,y'"
      ) |
    project('a', 'b')
    ),
    "project(
      equi_join(
        project(
          filter(
                apply(array, a, a, b, b), 
                a > 1 and b = 'bval'
                ),
          a, b
        ),
        filter(
          another_array,
          x = 1
        ),
        'left_names=a,b',
        'right_names=x,y'
      ), 
      a, b
    )"
  )
})
