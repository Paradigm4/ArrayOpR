library(testthat)

# Order of sourced files matter. First sourced is loaded into package namespace first.

# Get current file and directory, so that we can 'source' relative paths.
# Otherwise, the enclosing directory is required which creates an unnecessary dependecy on the directory name.
# Should only be used at the top level of 'sourced' R files
relative_path <- function(filename, n = 3) {
  this.file <- parent.frame(n)$ofile
  this.dir <- dirname(this.file)
  return(file.path(this.dir, filename))
}

# When runnning devtools::test(), the working directory is (package root)/tests/testthat/

source("array_op/__source.R", local = TRUE)
source("repo/__source.R", local = TRUE)
