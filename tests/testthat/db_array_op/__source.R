context('array_op db tests')

# source("test_auto_generated_fields.R", local = T)
# source("test_join.R", local = T)
# source("test_misc.R", local = T)
# source("test_semi_join.R", local = T)

# Run all tests in current folder
for (test_file in list.files(".", "^test_.+\\.R")) {
  source(test_file, local = T, chdir = T)
}

