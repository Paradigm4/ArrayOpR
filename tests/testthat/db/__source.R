context('Tests with scidb connection')

db = arrayop.unittest.get_scidb_connection()
expect_true(!is.null(db), "db is not null")

cleanUpNamespace = function(ns = NS){
  arrayNames = repo$query(
    sprintf("project(list(ns:%s), name)", ns)
  , .raw = T)[['name']]
  for(arr in arrayNames){
    fullName = sprintf("%s.%s", ns, arr)
    repo$execute(afl(fullName | remove), .raw = T)
  }
}

## Setup ----
config = yaml::yaml.load_file("repo.yaml")
# Run tests directly from console
# config = yaml::yaml.load_file("tests/testthat/db/repo.yaml")
NS = config$namespace

repo = newRepo(db = db)
tryCatch(
  {
    repo$execute(afl(NS | create_namespace))
    printf("Created namespace '%s'", NS)
  },
  error = function(e){
    printf("Namespace '%s' already exists.", NS)
  }
)
# Comment out for no debug
# repo$setting_debug = T
cleanUpNamespace(NS)
## Run tests ----
source('test_with_scidb.R', local = T)
## Teardown ----

cleanUpNamespace(NS)
tryCatch(
  repo$execute(afl(NS | drop_namespace))
)

rm(repo)
