context('ScidbConnection tests')


CONN = arrayop::get_default_connection()

random_array_name = utility$random_array_name

# after set up, there should be a valid scidb version
expect_true(nchar(CONN$scidb_version()) > 2)

for (test_file in list.files(".", "^test.+\\.R")) {
  source(test_file, local = T, chdir = T)
}
