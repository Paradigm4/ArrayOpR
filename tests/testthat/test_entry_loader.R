library(testthat)
library(mockery)
options(stringsAsFactors = FALSE)
`%>%` = dplyr::`%>%`

context('test entry')

# Order of sourced files matter. First sourced is loaded into package namespace first.

assert_afl_equal <- function(actual, expected) {
  actual <- gsub('\\s+', '', actual)
  expected <- gsub('\\s+', '', expected)
  testthat::expect_identical(actual, expected)
}

# Get a str vector from one str split by white spaces
.strsplit = function(x, sep="\\s+") {
  strsplit(x, split = sep)[[1]]
}

# When runnning devtools::test(), the working directory is (package root)/tests/testthat/

dry_tests = function() {
  source("base/__source.R", local = TRUE, chdir = TRUE)
  source("v18/__source.R", local = TRUE, chdir = TRUE)
  source("v19/__source.R", local = TRUE, chdir = TRUE)
  source("repo/__source.R", local = TRUE, chdir = TRUE)
}


wet_tests = function() {
  source("run_db_tests.R", local = TRUE, chdir = TRUE)
}

# dry_tests()

wet_tests()

