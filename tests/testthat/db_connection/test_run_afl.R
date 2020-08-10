context("Run AFL from a ScidbConnection")


test_that("run afl query", {
  operators = CONN$query("list('operators')")
  expect_true(nrow(operators) > 1)
})

test_that("create an array then remove it", {
  name = utility$random_array_name()
  
  show_array = function(){
    CONN$query(sprintf("show(%s)", name))
  }
  
  CONN$execute(sprintf("create temp array %s <a:string> [z]", name))
  # array should exist
  expect_identical(nrow(show_array()), 1L)
  
  # Remove the array
  CONN$execute(sprintf("remove(%s)", name))
  expect_error(show_array())
  
})
