context("Create/Remove arrays")

test_that("Create arrays", {
  name = "new_array"
  # The array should not exist before we create it
  expect_true(!name %in% get_array_names())
  
  anArray = repo$get_array(sprintf("%s.%s <a:string, b:bool, c:datetime> [z]", NS, name))
  repo$.create_array(anArray)
  expect_true(name %in% get_array_names())
  
  # Cannot create the array while it exists
  expect_error(repo$.create_array(anArray))
  
  repo$.remove_array(anArray)
  expect_true(!name %in% get_array_names())
})
