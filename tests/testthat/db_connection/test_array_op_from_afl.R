context("array_op from AFL: transient and stored")


# transient array_op from AFL ----

test_that("array_op from afl", {
  rawAfl = "apply(list('operators'), extra, 'abc')"
  arr = conn$afl_expr(rawAfl)
  expect_identical(arr$to_afl(), rawAfl)
  expect_equal(arr$attrs, c("name", "library", "extra"))
})

# persistent array_op from stored AFL ----

test_that("persistent array_op from stored AFL", {
  rawAfl = "apply(list('operators'), extra, 'abc')"
  name = random_array_name()
  
  storedArr = conn$array_from_stored_afl(rawAfl, name)
  retrievedArr = conn$array(name)
  
  expect_identical(storedArr$to_afl(), name)
  expect_equal(
    storedArr$to_df(), 
    retrievedArr$to_df()
  )
  
  storedArr$remove_self()
})

test_that("persistent array_op from stored AFL", {
  rawAfl = "apply(list('operators'), extra, 'abc')"
  
  storedArr = conn$array_from_stored_afl(rawAfl, .temp = T, .gc = F)
  
  expect_identical(storedArr$array_meta_data()$temporary, TRUE)
  
  storedArr$remove_self()
})


# From transient to persistent ----

test_that("store transient array_op from afl as a persistent one", {
  rawAfl = "apply(list('operators'), extra, 'abc')"
  name = random_array_name()
  
  conn$execute(afl(rawAfl | store(name)))
  
  transientArr = conn$afl_expr(rawAfl)
  storedArr = conn$array(name)
  
  expect_identical(transientArr$to_afl(), rawAfl)
  expect_identical(storedArr$to_afl(), name)
  expect_identical(storedArr$attrs, transientArr$attrs)
  expect_equal(
    storedArr$to_df(), 
    transientArr$to_df()
  )
  
  storedArr$remove_self()
})

