context('Tests with scidb connection')

db = get_scidb_connection()
expect_true(!is.null(db), "db is not null")

cleanUpNamespace = function(ns = NS){
  arrayNames = repo$query(afl(
    sprintf("list(ns:%s)", ns) %project% 'name'
  ), .raw = T)[['name']]
  for(arr in arrayNames){
    fullName = sprintf("%s.%s", ns, arr)
    log_job(NULL, sprintf("Removing array %s", fullName))
    repo$execute(afl(fullName %remove% NULL), .raw = T)
  }
}

## Setup ----
config = yaml::yaml.load_file(relative_path("repo.yaml"))
# Run tests directly from console
# config = yaml::yaml.load_file("tests/testthat/db/repo.yaml")
NS = config$namespace
repo = newRepo(db = db)
cleanUpNamespace(NS)
## Run tests ----
source(relative_path('test_with_scidb.R'), local = T)
## Teardown ----

cleanUpNamespace(NS)
rm(repo)
