context('Tests with scidb connection')

# Settings ----

# To enable in-db tests, create a `setting_file` which contains a function with `db_function_name`.
# The db function should return a scidb connection which we will use to create a arrayop:Repo class instance.
# E.g. >>> cat ~/.arrayop/unittest.setting.R
# arrayop.unittest.get_scidb_connection = function() {
#   result = scidb::scidbconnect(host="localhost", username="your_user_name", password="pwd", port=8083, protocol="https")
#   options(scidb.aio = TRUE)
#   result
# }
CONSTANTS = list(
  setting_file = "~/.arrayop/unittest.setting.R",
  db_function_name = "arrayop.unittest.get_scidb_connection"
)

SETTING = list(
  allow_db_test = ALLOW_DB_TEST,
  if_unittest_setting_exist = file.exists(CONSTANTS$setting_file)
)


choose_in_db_tests = function(setting = SETTING) {
  if(!setting$allow_db_test) 
    return(cat("Skipping in-database tests due to ALLOW_DB_TEST = F\n"))
  if(!setting$if_unittest_setting_exist) 
    return(catf("Skipping in-database tests due to absent '%s' \n", CONSTANTS$setting_file))
  
  # Run the array unittest config R script
  source(CONSTANTS$setting_file, local = T, chdir = T)
  if(!exists(CONSTANTS$db_function_name))
    return(catf("Skipping in-database tests due to absent function '%s' in %s \n", 
                CONSTANTS$db_function_name,
                CONSTANTS$setting_file))
  
  db_func = get(CONSTANTS$db_function_name)
  db = db_func()
  repo = newRepo(db = db)
  
  expect_true(!is.null(repo), "repo should not be null!")
  
  # 'repo' class version is determined by the scidb version, which is not hard-coded.
  source("shared.R", local = T, chdir = T)
  
  testNS$db_setup()
  # After the db setup, there should be an empty namespace for our in-db unit tests
  # The namespace and array config are in shared.R
  # Two shared variables for all tests are 1. repo; 2. config (loaded from repo.yaml)
  
  # Run individual tests with a shared 'repo' instance
  for (test_file in list.files(".", "^test.+\\.R")) {
    source(test_file, local = T, chdir = T)
  }
  
  testNS$db_cleanup()
}

# Main entry for in-db tests ---
# May not run tests if unable to create a repo instance
choose_in_db_tests()


