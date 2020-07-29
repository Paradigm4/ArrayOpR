context("Connect to SciDB")

test_that("default connection is connected", {
  expect_true(!is.null(get_default_connection()$scidb_version()))
})

test_that("connect with auth file", {
  conn = arrayop::connect(username = auth[["user-name"]], token = auth[["user-password"]], host = "127.0.0.1")
  # Connection is save as the default_conn by default  
  expect_true(nchar(get_default_connection()$scidb_version()) > 0)
  expect_identical(get_default_connection(), conn)
})

test_that("default_conn is saved under arrayop namespace", {
  expect_identical(auth[["user-name"]], get_default_connection()$conn_args()$username)
})

test_that("get a non-default conn", {
  # Here we create a new connection with host = "localhost", 
  # whereas the default_conn has a host of 127.0.0.1
  anotherConn = arrayop::connect(username = auth[["user-name"]], token = auth[["user-password"]], 
                                 host = "localhost", save_to_default_conn = F)
  expect_identical(anotherConn$scidb_version(), get_default_connection()$scidb_version())
  expect_true(!identical(anotherConn, get_default_connection()))
  
})
