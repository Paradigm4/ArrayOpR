context("Run AFL")

CONN = arrayop::get_default_connection()

test_that("run afl query", {
  operators = CONN$query("list('operators')")
  expect_true(nrow(operators) > 1)
  browser()
})
