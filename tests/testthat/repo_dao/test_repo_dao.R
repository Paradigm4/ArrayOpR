context("Test RepoDAO class basic features")

mockDep = list(
  get_scidb_version = function()
    "19.3"
  , query = function(what, ...) list('query', what, ...)
  , execute = function(what, ...) list('execute', what, ...)
  , get_schema_df = function(...) stop("get_schema_df not implemented in the mockDep object")
)

repo = newRepo(dependency_obj = mockDep, config = yaml::yaml.load_file(relative_path('repo.yaml')))
dao = newRepoDao(repo = repo, db = NULL)

new_df = function(...) {
  data.frame(..., stringsAsFactors = F, row.names = NULL)
}

## Query and Execute functions are delegated to Repo class ----

test_that("Raw query", {
  expect_identical(dao$query("abc", .raw = T), list('query', 'abc'))
  expect_identical(dao$query("abc", extra_arg = 42, .raw = T), list('query', 'abc', extra_arg = 42))
})

test_that("Raw execute", {
  expect_identical(dao$execute("abc", .raw = T), list('execute', 'abc'))
  expect_identical(dao$execute("abc", extra_arg = 42, .raw = T), list('execute', 'abc', extra_arg = 42))
})

test_that("Get array", {
  arrayOp = dao$get_array('alias_a')
  expect_identical(arrayOp$to_afl(), "NS.A")
  expect_identical(dao$get_array(arrayOp), arrayOp)
  
  literalArr = dao$get_array("NS2.another <a:string, b:int32> [da=*; db=0:1:2:3]")
  assert_afl_equal(literalArr$to_afl(), 'NS2.another')
  assert_afl_equal(literalArr$to_schema_str(), '<a:string, b:int32> [da=*; db=0:1:2:3]')
  assert_afl_equal(str(literalArr), "NS2.another <a:string, b:int32> [da=*; db=0:1:2:3]")
})

## Expressions are parsed and evaluated ----
mockDep = list(
  get_scidb_version = function()
    "19.3"
  , query = identity
  , execute = identity
  , get_schema_df = function(...) stop("get_schema_df not implemented in the mockDep object")
)

repo = newRepo(dependency_obj = mockDep, config = yaml::yaml.load_file(relative_path('repo.yaml')))
dao = newRepoDao(repo = repo, db = NULL)

test_that("nrows", {
  stub(dao$nrow, 'query_raw', function(q, ...) new_df(i=0, count=q))
  expect_equal(dao$nrow('alias_a'), "op_count(NS.A)")
  expect_equal(dao$nrow(dao$get_array('alias_a')), "op_count(NS.A)")
  assert_afl_equal(dao$nrow('alias_a where (b > 3)'), "op_count(filter(NS.A, b > 3))")
  assert_afl_equal(dao$nrow('operator(other_ns.array)', .raw = T), "op_count(operator(other_ns.array))")
})

test_that("head", {
  # stub(dao$limit, 'query_raw', data.frame(i=0, count=42))
  assert_afl_equal(dao$limit('alias_a', 5), "limit(NS.A, 5)")
  assert_afl_equal(dao$limit('alias_a', 5), "limit(NS.A, 5)")
  assert_afl_equal(dao$limit('alias_a', 5, 3), "limit(NS.A, 5, 3)")
  
  arr = dao$get_array('alias_a')
  assert_afl_equal(dao$limit(arr$where(b > 3), 5), "limit(filter(NS.A, b > 3), 5)")
  assert_afl_equal(dao$limit('alias_a where (b > 3)', 5), "limit(filter(NS.A, b > 3), 5)")
  
  assert_afl_equal(dao$limit('operator(other_ns.array)', 5, .raw = T), "limit(operator(other_ns.array), 5)")
})
