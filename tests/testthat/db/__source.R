context('Tests with scidb connection')

db = get_scidb_connection()
expect_true(!is.null(db), "db is not null")

## Setup ----
# config = yaml::yaml.load_file(relative_path("repo.yaml"))
# repo = newRepo(db = db, config = config)
# dao = newRepoDao(repo = repo, db = db)
repo = newRepo(db = db)
## Run tests ----
source(relative_path('test_with_scidb.R', local = T))
## Teardown ----
rm(repo)
