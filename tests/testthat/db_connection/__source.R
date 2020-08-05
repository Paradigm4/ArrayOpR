context('ScidbConnection tests')

conn = testNS$conn

# after set up, there should be a valid scidb version
expect_true(nchar(conn$scidb_version()) > 2)

for (test_file in list.files(".", "^test.+\\.R")) {
  source(test_file, local = T, chdir = T)
}
