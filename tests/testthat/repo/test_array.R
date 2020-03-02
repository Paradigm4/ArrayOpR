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
# 
# test_that("Get array from alias", {
#   repo = newRepo(dependency_obj = mockDep)
#   arrayOp = dao$get_array('alias_a')
#   expect_identical(arrayOp$to_afl(), "NS.A")
#   expect_identical(dao$get_array(arrayOp), arrayOp)
# })
