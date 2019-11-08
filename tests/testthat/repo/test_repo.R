context("Test Repo class")


# RepoBase class --------------------------------------------------------------------------------------------------
# Mock iquery(..., return = T) with a query that returns repeated afl x times
dummy_query = function(afl, times = 2, ...) {
  paste(rep(afl, times), collapse = '')
}
# Mock iquery(..., return = F)
dummy_execute = function(afl, ....) {
  sprintf("Cmd: %s", afl)
}

mockDep = list(
  get_scidb_version = function()
    "19.3"
  , query = dummy_query
  , execute = dummy_execute
  , get_schema_df = identity
)

mockDep2 = list(
  get_scidb_version = function()
    "18.1"
  , query = dummy_query
  , execute = dummy_execute
  , get_schema_df = identity
)

test_that("Version switch", {
  repo = newRepo(default_namespace = 'ns', dependency_obj = mockDep)
  expect_identical(repo$meta[['scidb_version']], '19.3')
  expect_identical(repo$meta[['repo_version']], 'RepoV19')
  
  repo2 = newRepo(default_namespace = 'ns', dependency_obj = mockDep2)
  expect_identical(repo2$meta[['scidb_version']], '18.1')
  expect_identical(repo2$meta[['repo_version']], 'RepoV18')
})

test_that("DB dependency delegation", {
  repo = newRepo(default_namespace = 'ns', dependency_obj = mockDep)
  expect_identical(repo$query('afl'), 'aflafl')
  expect_identical(repo$query('afl', times = 3), 'aflaflafl')
  expect_identical(repo$execute('afl'), 'Cmd: afl')
})

test_that("No matched version", {
  dep = list(get_scidb_version = function() '20.2', query = identity, execute = identity, get_schema_df = identity)
  expect_error(newRepo('ns', dependency_obj = dep), 'unsupported scidb version')
})


# Schema management -----------------------------------------------------------------------------------------------

test_that("Reigstered schemas are stored in schema registry", {
  dep = list(get_scidb_version = function() '18.1', query = function(x) 42, execute = function(x) 'cmd', 
    get_schema_df = identity)
  repo = newRepo('ns', dep)
  repo$register_schema_alias_by_array_name('a', 'A')
  repo$register_schema_alias_by_array_name('b', 'public.B', is_full_name = T)
  expect_identical(repo$meta$schema_registry, list(a = 'ns.A', b = 'public.B'))
  
  # Register multiple schemas at once
  repo = newRepo('ns', dep)
  repo$register_schema_alias_by_array_name(alias = c('a', 'b'), array_name = c('A', 'B'))
  expect_identical(repo$meta$schema_registry, list(a = 'ns.A', b = 'ns.B'))
})

test_that("Create ArrayOp instance for registered array schema aliases", {
  dep = list(get_scidb_version = function() '18.1', query = function(x) 42, execute = function(x) 'cmd', 
    get_schema_df = function(x) data.frame(name = 'a', dtype = 'string', is_dimension = F))
  repo = newRepo('ns', dep)
  repo$register_schema_alias_by_array_name('a', 'A')
  schema = repo$get_alias_schema('a')
  expect_identical(schema$to_afl(), 'ns.A')
  expect_error(repo$get_alias_schema('non-existent'), 'not registered')
})

