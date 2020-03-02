context("Create a Repo instance from a dependency or a db object")

test_that("Create a Repo from a dependency object", {
  dep = list(get_scidb_version=function() '19.3', query = identity, execute = identity, 
             upload_df_to_scidb = identity, store_afl_to_scidb = identity)
  repo = newRepo(dependency_obj = dep)
  expect_true(!is.null(repo))
})

test_that("Create a Repo from a db ", {
  stub(newRepo, 'dependency_obj$get_scidb_version', '19.3')
  repo = newRepo(db = list(iquery=identity))
  expect_true(!is.null(repo))
})

# Query and Execute functions are delegated to the dependency_obj

mockDep = list(
  get_scidb_version = function()
    "19.3"
  , query = function(what, ...) list('query', what, ...)
  , execute = function(what, ...) list('execute', what, ...)
  , upload_df_to_scidb = NULL
  , store_afl_to_scidb = NULL
)

test_that("Query/Execute params are passed to dep", {
  repo = newRepo(dependency_obj = mockDep)

  expect_identical(repo$query('abc', .raw = TRUE), list('query', 'abc'))
  expect_identical(repo$query('abc', .raw = TRUE, 'extra_arg', nrow=3), list('query', 'abc', 'extra_arg', nrow=3))
  
  expect_identical(repo$execute('abc', .raw = TRUE), list('execute', 'abc'))
  expect_identical(repo$execute('abc', .raw = TRUE, 'extra_arg', nrow=3), list('execute', 'abc', 'extra_arg', nrow=3))
})
