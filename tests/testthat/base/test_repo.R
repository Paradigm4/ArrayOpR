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

# Load repo from a setting object (list) ----------

test_that("Load repo schemas from a setting object", {
  dep = list(get_scidb_version = function() '18.1', query = function(x) 42, execute = function(x) 'cmd', 
    get_schema_df = identity)
  config = list(namespace = 'ns', settings = list(), 
    arrays = list(
      list(alias = 'V', name = 'Variant', schema = "<ref:string, alt:string compression 'zlib', score:double> [chrom=1:24:0:1; pos=*]"),
      list(alias = 'G', name = 'Genotype', 
        attrs = list(ref='string', alt='string', score='double'), 
        dims = list(chrom='1:24:0:1', pos='*'))
    ))
  yamlStr = "# comment line
namespace: ns
arrays:
  - alias: V
    name: Variant
    schema: <ref:string, alt:string compression 'zlib', score:double> [chrom=1:24:0:1; pos=*]
  - alias: G
    name: Genotype
    dims:
      chrom: '1:24:0:1'
      # a single * has to be quoted otherwise will cause a yaml parse error
      pos: '*'
    attrs:
      ref: string
      alt: string
      score: double
"
  for(repo in c(
    newRepo(dep = dep, config = config)
    , newRepo(dep = dep, config = yaml::yaml.load(yamlStr))
  )){
    V = repo$get_alias_schema('V')
    G = repo$get_alias_schema('G')
    expect_identical(V$to_afl(), 'ns.Variant')
    expect_identical(V$dims, c('chrom', 'pos'))
    expect_identical(V$attrs, c('ref', 'alt', 'score'))
    assert_afl_equal(V$to_schema_str(), "<ref:string, alt:string compression 'zlib', score:double> [chrom=1:24:0:1; pos=*]")
    
    expect_identical(G$to_afl(), 'ns.Genotype')
    expect_identical(G$dims, c('chrom', 'pos'))
    expect_identical(G$attrs, c('ref', 'alt', 'score'))
    assert_afl_equal(G$to_schema_str(), "<ref:string, alt:string, score:double> [chrom=1:24:0:1; pos=*]")
  }
})
