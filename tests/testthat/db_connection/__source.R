context('ScidbConnection tests')

# after set up, there should be a valid scidb version 18+ (lower version not supported)
expect_true(conn$scidb_version()$major >= 18L) 

# source("test_fread.R", local = T)
# source("test_array_op_from_array.R", local = T)
source("test_array_op_from_df.R", local = T)

for (test_file in list.files(".", "^test.+\\.R")) {
  # source(test_file, local = T, chdir = T)
}
