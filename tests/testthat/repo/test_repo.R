context("Test Repo class")


# RepoBase class --------------------------------------------------------------------------------------------------

mockDep = list(
  get_version = function()
    "19.3"
  ,
  # Mock iquery(..., return = T) with a query that returns repeated afl x times
  query = function(afl, times = 2, ...) {
    paste(rep(afl, times), collapse = '')
  }
  ,
  # Mock iquery(..., return = F)
  execute = function(afl, ....) {
    sprintf("Cmd: %s", afl)
  }
)

mockDep2 = list(
  get_version = function()
    "18.1"
  ,
  # Mock iquery(..., return = T) with a query that returns repeated afl x times
  query = function(afl, times = 2, ...) {
    paste(rep(afl, times), collapse = '')
  }
  ,
  # Mock iquery(..., return = F)
  execute = function(afl, ....) {
    sprintf("Cmd: %s", afl)
  }
)

test_that("Version switch", {
  repo = newRepo(default_namespace = 'ns', dependency_obj = mockDep)
  expect_identical(repo$meta_info[['scidb_version']], '19.3')
  expect_identical(repo$meta_info[['repo_version']], 'RepoV19')
  
  repo2 = newRepo(default_namespace = 'ns', dependency_obj = mockDep2)
  expect_identical(repo2$meta_info[['scidb_version']], '18.1')
  expect_identical(repo2$meta_info[['repo_version']], 'RepoV18')
})

test_that("DB dependency delegation", {
  repo = newRepo(default_namespace = 'ns', dependency_obj = mockDep)
  expect_identical(repo$query('afl'), 'aflafl')
  expect_identical(repo$query('afl', times = 3), 'aflaflafl')
  expect_identical(repo$execute('afl'), 'Cmd: afl')
})
