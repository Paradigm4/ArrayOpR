context("Repo array management")

mockDep = list(
  get_scidb_version = function()
    "19.3"
  , query = function(what, ...) list('query', what, ...)
  , execute = function(what, ...) list('execute', what, ...)
  , upload_df_to_scidb = NULL
  , store_afl_to_scidb = NULL
)

test_that("Get array from schema", {
  repo = newRepo(dependency_obj = mockDep)
  literalArr = repo$get_array("NS2.another <a:string, b:int32> [da=*; db=0:1:2:3]")
  assert_afl_equal(literalArr$to_afl(), 'NS2.another')
  assert_afl_equal(literalArr$to_schema_str(), '<a:string, b:int32> [da=*; db=0:1:2:3]')
  assert_afl_equal(str(literalArr), "NS2.another <a:string, b:int32> [da=*; db=0:1:2:3]")
})

test_that("Get array from alias", {
  repo = newRepo(dependency_obj = mockDep)
  
  # Throw an error if array alias not registered yet. 
  expect_error(repo$get_array('alias', 'not a reigstered array'))
  
  arrayOp = repo$get_array("myNamespace.raw_array_name <a:string zip, b:int32> [dim=0:1:2:3]")
  anotherArray = repo$get_array("NS2.another <a:string, b:int32> [da=*; db=0:1:2:3]")
  repo$register_array(list('alias'=arrayOp))
  expect_identical(repo$get_array('alias'), arrayOp)
  expect_error(repo$get_array('alias_non_existent', 'not a reigstered array'))
  
  repo$register_array(list('alias'=anotherArray))
  expect_identical(repo$get_array('alias'), anotherArray)
  
  # Register an alias as NULL effectively remove it from the array_alias_registry
  repo$register_array(list('alias'=NULL))
  expect_error(repo$get_array('alias', 'not a reigstered array'))
  
})
