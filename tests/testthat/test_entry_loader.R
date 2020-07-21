library(testthat)
context('test entry')

# Order of sourced files matter. First sourced is loaded into package namespace first.

assert_afl_equal <- function(actual, expected) {
  actual <- gsub('\\s+', '', actual)
  expected <- gsub('\\s+', '', expected)
  testthat::expect_identical(actual, expected)
}

options(stringsAsFactors = FALSE)

# When runnning devtools::test(), the working directory is (package root)/tests/testthat/

source("base/__source.R", local = TRUE, chdir = TRUE)
source("v18/__source.R", local = TRUE, chdir = TRUE)
source("v19/__source.R", local = TRUE, chdir = TRUE)
source("repo/__source.R", local = TRUE, chdir = TRUE)

# Tests that run with a scidb connection
# Only run these tests if a db connection is configured; otherwise skip them altogether
ALLOW_DB_TEST = TRUE
ALLOW_DB_TEST = FALSE


source("db/__source.R", local = TRUE, chdir = TRUE)
