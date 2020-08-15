context('Db tests setup')

# Settings ----

# To enable in-db tests, create a `setting_file` which contains a function with `db_function_name`.
# The db function should return a scidb connection which we will use to create a arrayop:Repo class instance.
# E.g. >>> cat ~/.arrayop/unittest.setting.R
# arrayop.unittest.get_scidb_connection = function() {
# auth = yaml::yaml.load_file("~/.scidb_auth")
# # Ensure a scidb conenction
# arrayop::connect(username = auth[["user-name"]], token = auth[["user-password"]], host = "127.0.0.1")
# }



choose_in_db_tests = function(setting = SETTING) {
  
  CONSTANTS = list(
    setting_file = "~/.arrayop/unittest.setting.R",
    db_function_name = "arrayop.unittest.get_scidb_connection"
  )

  if(!file.exists(CONSTANTS$setting_file)) {
    cat(sprintf("Skipping in-database tests due to absent '%s' \n", CONSTANTS$setting_file))
    return(invisible(NULL))
  }
  
  # Run the array unittest config R script
  source(CONSTANTS$setting_file, local = T, chdir = T)
  
  if(!exists(CONSTANTS$db_function_name)){
    cat(sprintf("Skipping in-database tests due to absent function '%s' in %s \n", 
                CONSTANTS$db_function_name,
                CONSTANTS$setting_file))
    return(invisible(NULL))
  }
  
  # Create a ScidbConnection instance
  conn = (get(CONSTANTS$db_function_name))()
  
  expect_true(!is.null(conn), "ScidbConnection conn should not be null!")
  
  # 'repo' class version is determined by the scidb version, which is not hard-coded.
  source("shared.R", local = T, chdir = T)
  
  # Create the globally shared testNS object for all db-tests
  TestNS$new(namespace = "arrayop_unittest", conn = conn)
  
}

# Main entry for in-db tests ---
# May not run tests if unable to create a repo instance
testNS = choose_in_db_tests()
conn = get_default_connection()

