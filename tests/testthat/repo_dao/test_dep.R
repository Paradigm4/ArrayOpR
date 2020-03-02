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
