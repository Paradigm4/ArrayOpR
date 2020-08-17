context("Connect to SciDB")

test_that("default connection is connected", {
  expect_true(!is.null(get_default_connection()$scidb_version()))
})

test_that("get a non-default conn", {
  # Here we create a new connection with host = "localhost", 
  # whereas the default_conn has a host of 127.0.0.1
  connArgs = conn$conn_args()
  anotherConn = arrayop::connect(username = connArgs$username, token = connArgs$password, 
                                 host = "localhost", save_to_default_conn = F)
  expect_identical(anotherConn$scidb_version(), get_default_connection()$scidb_version())
  expect_true(!identical(anotherConn, get_default_connection()))
  
})

test_that("create scidb array", {
  validate_array_exists = function(arr, temporary = FALSE) {
    expect_true(arr$exists_persistent_array())
    expect_identical(arr$array_meta_data()$temporary, temporary)
  }
  
  a1 = conn$create_new_scidb_array(random_array_name(), "<fa:string, fb:int32, fc:bool, fd:double> [i]")
  a2 = conn$create_new_scidb_array(random_array_name(), "existing_name_does_not_matter <fa:string, fb:int32, fc:bool, fd:double> [i;j]")
  a3 = conn$create_new_scidb_array(random_array_name(), "<fa:string, fb:int32, fc:bool, fd:double> [i]", .temp = T)
  
  validate_array_exists(a1, F)
  validate_array_exists(a2, F)
  validate_array_exists(a3, T)
  
  a1$remove_self()
  a2$remove_self()
  a3$remove_self()
})
