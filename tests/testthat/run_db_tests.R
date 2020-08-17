# Tests that run with a scidb connection
# Only run these tests if a db connection is configured; otherwise skip them altogether
ALLOW_DB_TEST = FALSE
ALLOW_DB_TEST = TRUE

run_sub_folders = function(){
  # import shared objects
  conn = get_default_connection()
  random_array_name = utility$random_array_name
  
  source("db_array_op/__source.R", local = TRUE, chdir = TRUE)
  source("db_connection/__source.R", local = TRUE, chdir = TRUE)
}

if(!ALLOW_DB_TEST){
  cat("Skipping in-database tests due to ALLOW_DB_TEST = F\n")
  return(invisible(NULL))
} else {
  # db setup may fail if no config found or conneciton cannot be initiated
  try(source("db_tests_setup/__source.R", local = TRUE, chdir = TRUE), silent = T)
  
  if(!get_default_connection(F)$is_connected()){
    return(invisible(NULL))
  } else {
    run_sub_folders()
  }
}

