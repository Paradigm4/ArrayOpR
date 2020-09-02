context("Get array_op from scidb array names and schemas")

# Tests begin ---- 

# from array name ---- 

test_that("array_op from scidb array name", {
  name = random_array_name()
  arr = conn$create_new_scidb_array(name, "<a:string> [z]", .temp = T)
  expect_identical(arr$to_afl(), name)
  expect_identical(conn$array(name)$to_afl(), name)
  
  arr$remove_self()
})


# from schema string ----

verify_array = function(input_schema_str, expected_afl, expected_schema_str, expected_attrs, expected_dims){
  arr = conn$array_from_schema(input_schema_str)
  expect_identical(arr$to_afl(), expected_afl)
  expect_identical(arr$to_schema_str(), expected_schema_str)
  expect_identical(arr$attrs, expected_attrs)
  expect_identical(arr$dims, expected_dims)
}

verify_parsing_error = function(input_schema_str, error_pattern){
  if(missing(error_pattern))
    expect_error(conn$array_from_schema(input_schema_str))
  else 
    expect_error(conn$array_from_schema(input_schema_str), error_pattern)
}


test_that("array_op from schema str", {
 
  # space around the schema str are trimmed
  
  verify_array(
    "temp<a:string, b:int32>[z]", 
    "temp", "<a:string,b:int32> [z]", .strsplit("a b"), .strsplit("z")
  )
  verify_array(
    " non empty operators < a : string, b:int32 not null > [ x; y; z ]", 
    "operators", "<a:string,b:int32 not null> [x;y;z]", .strsplit("a b"), .strsplit("x y z")
  )
  
  verify_array(
    " array_name <a:string COMPRESSION 'zlib', b:int32, c:double> [i;j=0:*:0:*] ",
    "array_name",
    "<a:string COMPRESSION 'zlib',b:int32,c:double> [i;j=0:*:0:*]",
    .strsplit("a b c"), .strsplit("i j")
  )
  verify_array(
    "  <a:string, b:int32, c:double> [i;j=0:*:0:*] ",
    "",
    "<a:string,b:int32,c:double> [i;j=0:*:0:*]",
    .strsplit("a b c"), .strsplit("i j")
  )
  verify_array(
    "  <a:string, b:int32, c:double> [i] ",
    "",
    "<a:string,b:int32,c:double> [i]",
    .strsplit("a b c"), .strsplit("i")
  )
  verify_array(
    "  <a:string, b:int32, c:double> ",
    "",
    "<a:string,b:int32,c:double>",
    .strsplit("a b c"), NULL
  )
  
  # verify_parsing_error("", 'invalid schema')
  # verify_parsing_error("<>", 'invalid schema')
})

test_that("Error cases of schema parsing", {
  verify_parsing_error("", "Invalid")
  verify_parsing_error("<a:int32>>", "Invalid")
  verify_parsing_error("<a:int32> [", "Invalid")
  verify_parsing_error("<a:int32> []", "Invalid")
})


