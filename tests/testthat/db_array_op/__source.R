context('array_op db tests')

conn = testNS$conn

# Run individual tests with a shared 'repo' instance
for (test_file in list.files(".", "^test_.+\\.R")) {
  source(test_file, local = T, chdir = T)
}

