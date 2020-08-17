context('ScidbConnection tests')

# after set up, there should be a valid scidb version
expect_true(nchar(conn$scidb_version()) > 2)

# source("test_connect.R", local = T)

for (test_file in list.files(".", "^test.+\\.R")) {
  source(test_file, local = T, chdir = T)
}
