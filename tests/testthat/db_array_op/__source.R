context('array_op db tests')


# Run all tests in current folder
for (test_file in list.files(".", "^test_.+\\.R")) {
  source(test_file, local = T, chdir = T)
}

