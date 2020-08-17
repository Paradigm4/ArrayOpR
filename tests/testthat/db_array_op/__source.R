context('array_op db tests')

source("test_mutate.R", local = T)

# Run all tests in current folder
for (test_file in list.files(".", "^test_.+\\.R")) {
  # source(test_file, local = T, chdir = T)
}

