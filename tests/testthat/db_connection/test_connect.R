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
