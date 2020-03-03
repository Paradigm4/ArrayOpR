library(testthat)
library(mockery)
context('test entry')

# Order of sourced files matter. First sourced is loaded into package namespace first.

# Get current file and directory, so that we can 'source' relative paths.
# Otherwise, the enclosing directory is required which creates an unnecessary dependecy on the directory name.
# Should only be used at the top level of 'sourced' R files
relative_path <- function(filename, n = 3) {
  this.file <- parent.frame(n)$ofile
  this.dir <- dirname(this.file)
  return(file.path(this.dir, filename))
}

assert_afl_equal <- function(actual, expected) {
  actual <- gsub('\\s+', '', actual)
  expected <- gsub('\\s+', '', expected)
  testthat::expect_identical(actual, expected)
}

options(stringsAsFactors = FALSE)

# When runnning devtools::test(), the working directory is (package root)/tests/testthat/

# source("array_op/__source.R", local = TRUE)
source("base/__source.R", local = TRUE)
source("v18/__source.R", local = TRUE)
source("v19/__source.R", local = TRUE)
source("repo/__source.R", local = TRUE)

# Tests that run with a scidb connection
# Only run these tests if a db connection is configured; otherwise skip them altogether
if(file.exists("~/.arrayop/db.R")) {
  source("~/.arrayop/db.R", local = TRUE)
  if(exists('get_scidb_connection')) {
    source("db/__source.R", local = TRUE)
  }
}
