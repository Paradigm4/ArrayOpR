context("Create/Remove arrays")

test_that("Create arrays", {
  name = "new_array"
  # The array should not exist before we create it
  expect_true(!name %in% testNS$get_array_names())
  
  anArray = testNS$create_local_arrayop(name, "<a:string, b:bool, c:datetime> [z]")
  repo$.create_array(anArray)
  expect_true(name %in% testNS$get_array_names())
  
  # Cannot create the array while it exists
  expect_error(repo$.create_array(anArray))
  
  # Remove array
  repo$.remove_array(anArray)
  expect_true(!name %in% testNS$get_array_names())
})
