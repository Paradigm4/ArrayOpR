
# Document the package --------------------------------------------------------------------------------------------

#' ArrayOp: A package for Object-Oriented SciDB Array Operations/Operands
#' 
#' ArrayOp abstracts away AFL generation by unifying array operations and array operands.
#' 
#' @section ArrayOpBase class and its sub-classes:
#' content1
#' @section Repo classes:
#' content2
#' @docType package
#' @name arrayop
NULL


# Set source file loading order -----------------------------------------------------------------------------------

# Global utility functions/classes

#' @include db_access.R
#' @include repo.R
#' @include repo_v19.R
#' @include repo_v18.R
#' @include repo_base.R
NULL

#' @include write_op.R
#' @include match_op.R
#' @include customized_op.R
#' @include join_op.R
#' @include subset_op.R
#' @include array_schema.R
#' @include array_op_base.R
#' @include dependency.R
#' @include afl_utils.R
NULL

#' @include arg_list.R
#' @include utils.R 
NULL
