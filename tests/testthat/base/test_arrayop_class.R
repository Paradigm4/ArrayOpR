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
  expect_array_schema(a$spawn(renamed = list(aa = "aa1")), .strsplit('aa1 ab ac'), dims)
  
  # cannot operate on non-existent fields
  expect_error(a$filter(non_existent > 0), "non_existent")
  expect_error(a$select('non_existent'), "non_existent")
  
  expect_error(a$spawn(renamed = list(aa1 = 'aa')), "aa1")
})

test_that("ArrayOp active bindings", {
  a = ArrayOp("array", attrs = c("fa", "fb"), dims = c("da", "db"), 
              dtypes = list(fa = "string COMPRESSION 'zlib'", fb = "int32"),
              dim_specs = list(da = "0:*:0:*"))
  
  expect_identical(a$dtypes, list(fa = "string COMPRESSION 'zlib'", fb = "int32", da = "int64", db = "int64"))
  expect_identical(a$raw_dtypes, list(fa = "string", fb = "int32", da = "int64", db = "int64"))
  
  expect_identical(a$to_schema_str(), "<fa:string COMPRESSION 'zlib',fb:int32> [da=0:*:0:*;db]")
})
