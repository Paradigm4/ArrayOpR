context("Connect to SciDB")


auth = yaml::yaml.load_file("~/.scidb_auth")

test_that("connect with auth file", {
  conn = arrayop::connect(username = auth[["user-name"]], token = auth[["user-password"]], host = "127.0.0.1")
  # Connection is save as the default_conn by default  
  expect_true(nchar(arrayop::default_conn$scidb_version) > 0)
  expect_identical(arrayop::default_conn, conn)
})

test_that("default_conn is saved under arrayop namespace", {
  expect_identical(auth[["user-name"]], arrayop::default_conn$username)
})

test_that("get a non-default conn", {
  # Here we create a new connection with host = "localhost", 
  # whereas the default_conn has a host of 127.0.0.1
  anotherConn = arrayop::connect(username = auth[["user-name"]], token = auth[["user-password"]], 
                                 host = "localhost", save_to_default_conn = F)
  expect_identical(anotherConn$scidb_version, default_conn$scidb_version)
  expect_true(!identical(anotherConn, default_conn))
})
