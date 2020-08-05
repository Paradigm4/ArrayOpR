# Tests that run with a scidb connection
# Only run these tests if a db connection is configured; otherwise skip them altogether
ALLOW_DB_TEST = FALSE
ALLOW_DB_TEST = TRUE

if(!ALLOW_DB_TEST){
  cat("Skipping in-database tests due to ALLOW_DB_TEST = F\n")
  return(invisible(NULL))
}

source("db_tests_setup/__source.R", local = TRUE, chdir = TRUE)

run_sub_folders = function(){
  source("db_array_op/__source.R", local = TRUE, chdir = TRUE)
  source("db_connection/__source.R", local = TRUE, chdir = TRUE)
}

if(is.null(testNS)){
  return(invisible(NULL))
} 

testNS$db_setup()
run_sub_folders()
testNS$db_cleanup()

