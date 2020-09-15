context("Run AFL from a ScidbConnection")


test_that("run afl query", {
  operators = conn$query("list('operators')")
  expect_true(nrow(operators) > 1)
})

test_that("create an array then remove it", {
  name = dbutils$random_array_name()
  
  show_array = function(){
    conn$query(sprintf("show(%s)", name))
  }
  
  conn$execute(sprintf("create temp array %s <a:string> [z]", name))
  # array should exist
  expect_identical(nrow(show_array()), 1L)
  
  # Remove the array
  conn$execute(sprintf("remove(%s)", name))
  expect_error(show_array())
  
})
