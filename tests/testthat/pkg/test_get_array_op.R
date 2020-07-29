context("Get array_op from scidb")

CONN = get_default_connection()

.name = "testarray_xyz"

.setup = function(){
  CONN$execute(sprintf("create temp array %s <a:string> [z]", .name))
}

.teardown = function(){
  CONN$execute(afl(.name | remove))
}

.setup()

# Tests begin

test_that("get array_op by array name", {
  arr = CONN$array_op_from_name(.name)
  expect_identical(arr$to_afl(), .name)
})

test_that("get array_op by afl", {
  rawAfl = "apply(list('operators'), extra, 'abc')"
  arr = CONN$array_op_from_afl(rawAfl)
  expect_identical(arr$to_afl(), rawAfl)
  expect_equal(arr$attrs, c("name", "library", "extra"))
})

test_that("get array_op by afl and stored it as array", {
  rawAfl = "apply(list('operators'), extra, 'abc')"
  name = "testarray_stored_afl"
  arr = CONN$array_op_from_afl(rawAfl, store_as_array = name)
  expect_identical(arr$to_afl(), name)
  expect_equal(arr$attrs, c("name", "library", "extra"))
  expect_equal(
    CONN$query(afl(arr | op_count))$count, 
    CONN$query(afl(rawAfl | op_count))$count 
  )
  
  CONN$execute(afl(name | remove))
})

# Tests end

.teardown()
