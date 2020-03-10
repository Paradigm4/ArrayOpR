context("Repo array management")

mockDep = list(
  get_scidb_version = function()
    "19.3"
  , query = function(what, ...) list('query', what, ...)
  , execute = function(what, ...) list('execute', what, ...)
  , upload_df_to_scidb = NULL
  , store_afl_to_scidb = NULL
)

test_that("Get array from schema string", {
  repo = newRepo(dependency_obj = mockDep)
  literalArr = repo$get_array("NS2.another <a:string, b:int32> [da=*; db=0:1:2:3]")
  assert_afl_equal(literalArr$to_afl(), 'NS2.another')
  assert_afl_equal(literalArr$to_schema_str(), '<a:string, b:int32> [da=*; db=0:1:2:3]')
  assert_afl_equal(str(literalArr), "NS2.another <a:string, b:int32> [da=*; db=0:1:2:3]")
})

test_that("Get array from registered array alias", {
  repo = newRepo(dependency_obj = mockDep)
  
  # Throw an error if array alias not registered yet. 
  expect_error(repo$get_array('alias', 'not a reigstered array'))
  
  arrayOp = repo$get_array("myNamespace.raw_array_name <a:string zip, b:int32> [dim=0:1:2:3]")
  anotherArray = repo$get_array("NS2.another <a:string, b:int32> [da=*; db=0:1:2:3]")
  repo$register_arrayops(list('alias'=arrayOp))
  expect_identical(repo$get_array('alias'), arrayOp)
  expect_error(repo$get_array('alias_non_existent', 'not a reigstered array'))
  
  repo$register_arrayops(list('alias'=anotherArray))
  expect_identical(repo$get_array('alias'), anotherArray)
  
  # Register an alias as NULL effectively remove it from the array_alias_registry
  repo$register_arrayops(list('alias'=NULL))
  expect_error(repo$get_array('alias', 'not a reigstered array'))
  
})

test_that("Register array alias as the raw array name", {
  repo = newRepo(dependency_obj = mockDep)
  arrayOp = repo$get_array("ns.raw_array_name <a:string zip, b:int32> [dim=0:1:2:3]")
  anotherArray = repo$get_array("NS2.another <a:string, b:int32> [da=*; db=0:1:2:3]")
  repo$register_arrayops(list('alias'='ns.raw_array_name', 'another'=anotherArray))
  stub(repo$get_array, 'load_arrayop_from_scidb', arrayOp)
  expect_identical(repo$get_array('alias'), arrayOp)
  expect_identical(repo$get_array('another'), anotherArray)
  # Registered raw arrays are cached
  stub(repo$get_array, 'load_arrayop_from_scidb', function(...) stop("error"))
  # So no error here
  expect_identical(repo$get_array('alias'), arrayOp)
})

test_that("Register array with invalid array name", {
  repo = newRepo(dependency_obj = mockDep)
  arrayOp = repo$get_array("ns.raw_array_name <a:string zip, b:int32> [dim=0:1:2:3]")

  repo$register_arrayops(list('alias'='wrong name'))
  # Simulate a scidb query error
  stub(repo$get_array, 'dep$query', function(...) stop("wrong array name"))
  # An error is throw when we try to retrieve the array
  expect_error(repo$get_array('alias'), 'not a valid scidb array')
})

# Load a single array from scidb ----

test_that("Load arrays from scidb", {
  repo = newRepo(dependency_obj = mockDep)
  arrayOp = repo$get_array("ns.raw_array_name <a:string zip, b:int32> [dim=0:1:2:3]")
  stub(repo$load_arrayop_from_scidb, 'query_raw', list(schema=arrayOp$to_schema_str()))
  loaded = repo$load_arrayop_from_scidb('rawArrayName')
  expect_identical(loaded$to_schema_str(), arrayOp$to_schema_str())
})

test_that("Load arrays from scidb with exception", {
  repo = newRepo(dependency_obj = mockDep)
  arrayOp = repo$get_array("ns.raw_array_name <a:string zip, b:int32> [dim=0:1:2:3]")
  stub(repo$load_arrayop_from_scidb, 'query_raw', function(...) stop("Some scidb error"))
  expect_error(repo$load_arrayop_from_scidb('badRawArrayName'), "scidb error")
})

# Load arrays from a scidb namespace ----

test_that("Load arrays from a scidb namespace", {
  to_schema_str = function(x) x$to_schema_str()
  to_afl = function(x) x$to_afl()
  
  repo = newRepo(dependency_obj = mockDep)
  array1 = repo$get_array("array1 <a:string zip, b:int32> [dim=0:1:2:3]")
  array2 = repo$get_array("array2 <a:string, b:int32> [da=*; db=0:1:2:3]")
  array3 = repo$get_array("array3 <aa:string, ab:bool null> [da=*; db=0:1:2:3]")
  sourceArrays = c(array1, array2, array3)
  schemaStrs = sapply(sourceArrays, function(x) str(x))
  
  stub(repo$load_arrayops_from_scidb_namespace, "query_raw", list(schema=schemaStrs))
  loadedArrays = repo$load_arrayops_from_scidb_namespace('NS')
  expect_identical(length(loadedArrays), 3L)
  expect_identical(names(loadedArrays), sapply(sourceArrays, to_afl))
  expect_identical(rlang::set_names(sapply(loadedArrays, to_afl), NULL), 
                   sprintf("NS.%s", sapply(sourceArrays, to_afl)))
  expect_identical(rlang::set_names(sapply(loadedArrays, to_schema_str), NULL), sapply(sourceArrays, to_schema_str))
})

# Load arrays from a config ----

test_that("Load arrays from a config", {
  repo = newRepo(dependency_obj = mockDep)
  config = list(namespace='NS', arrays = list(
    list('alias' = 'aa', 'name' = 'rawA', 'schema' = "<a:string compression 'zlib', b:int32> [da=0:*:0:*]")
    , list('alias' = 'ab', 'name' = 'anotherNS.rawB', 'schema' = "<a:string, b:int32> [da=0:*:0:*]")
    , list('alias' = 'ac', 'name' = 'myns.array1')
    , list('alias' = 'ad', 'name' = 'array2')
  ))
  arrayList = repo$load_arrayops_from_config(config)
  arr1 = repo$get_array("NS.rawA<a:string compression 'zlib', b:int32> [da=0:*:0:*]")
  arr2 = repo$get_array("another.rawB <a:string, b:int32> [da=0:*:0:*]")
  expect_identical(arrayList[['aa']]$to_afl(), "NS.rawA")
  expect_identical(arrayList[['aa']]$to_schema_str(), "<a:string compression 'zlib',b:int32> [da=0:*:0:*]")
  expect_identical(arrayList[['ab']]$to_afl(), "anotherNS.rawB")
  expect_identical(arrayList[['ab']]$to_schema_str(), "<a:string,b:int32> [da=0:*:0:*]")
  # An array with empty schema will return its full array name only
  expect_identical(arrayList[['ac']], 'myns.array1')
  expect_identical(arrayList[['ad']], 'NS.array2')
  
  # Register the loaded arrayops from config
  repo$register_arrayops(arrayList)
  expect_identical(str(repo$get_array('aa')), "NS.rawA <a:string compression 'zlib',b:int32> [da=0:*:0:*]")
  expect_identical(str(repo$get_array('ab')), "anotherNS.rawB <a:string,b:int32> [da=0:*:0:*]")
  stub(repo$get_array, "load_arrayop_from_scidb", identity)
  expect_identical(repo$get_array('ac'), "myns.array1")
  expect_identical(repo$get_array('ad'), "NS.array2")

  # Return an empty list of 'arrays' section is empty.
  expect_identical(repo$load_arrayops_from_config(list(namespace='NS', arrays=list())), list())
})

test_that("Load arrays with bad config", {
  repo = newRepo(dependency_obj = mockDep)
  expect_error(repo$load_arrayops_from_config(list()), "'config' missing section")
  expect_error(
    repo$load_arrayops_from_config(list(namespace='NS', arrays = list(list('alias'='aa')))), 
    "bad config format")
  expect_error(
    repo$load_arrayops_from_config(list(namespace='NS', arrays = list(list('alias'='aa')))), 
    "bad config format")
  expect_error(
    repo$load_arrayops_from_config(list(namespace='NS', arrays = list(list('alias'='aa', 'name'=42, schema='schema')))), 
    "bad config format")
  expect_error(
    repo$load_arrayops_from_config(list(namespace='NS', arrays = list(list('alias'='aa', 'name'='A', schema='schema')))), 
    "invalid array schema")
})
