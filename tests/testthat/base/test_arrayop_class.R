context("Non-db related ArrayOp class methods")

ArrayOp = ArrayOpV19$new

# Create ArrayOp instances ----

expect_array_schema = function(a1, attrs, dims){
  expect_identical(a1$attrs, attrs)
  expect_identical(a1$dims, dims)
}

test_that("manually create new instances", {
  
  attrs = .strsplit("aa ab ac")
  dims = .strsplit("da db")
  
  a = ArrayOp("array", attrs = attrs, dims = dims)
  
  expect_array_schema(a, attrs, dims)
  expect_array_schema(a$filter(aa > 0), attrs, dims)
  expect_array_schema(a$spawn(), attrs, dims)
  expect_array_schema(a$spawn(afl_str = "new_array_name"), attrs, dims)
  expect_array_schema(a$spawn(added = "extra_attr"), c(attrs, "extra_attr"), dims)
  
  # cannot operate on non-existent fields
  expect_error(a$filter(non_existent > 0), "non_existent")
  expect_error(a$select('non_existent'), "non_existent")
})
