
# Document the package --------------------------------------------------------------------------------------------

#' ArrayOp: A package for Object-Oriented SciDB Array Operations/Operands
#' 
#' ArrayOp package provides high-level, intutive API functions for data wrangling 
#' and hide detailed AFL generation from you. 
#' 
#' Synopsis
#' 1. Create to scidb
#' 2. Get arrayOp isntances
#' 3. Invoke verbs on arrayOp instances
#' 4. Or in rare cases, manually create AFL statements
#' 
#' @docType package
#' @name arrayop
NULL

# initialize an empty connection, which will be initialized once `arrayop::connect` is called
default_conn = empty_connection()

# Global utility functions/classes
