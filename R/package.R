
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

default_conn = new.env(parent = emptyenv())

#' Connect to SciDB server
#' 
#' @export
connect = function(username, token, 
                   host = "127.0.0.1",
                   port = 8083, protocol = "https", auth_type = "scidb",
                   ..., # other optional args for scidb::scidbconnect
                   save_to_default_conn = TRUE
                   ) {
  connectionArgs = list(
    host = host,
    username = username,
    password = token,
    port = port,
    protocol = protocol,
    auth_type = auth_type,
    ...
  )
  
  db = do.call(scidb::scidbconnect, connectionArgs)
  repo = newRepo(db = db)
  
  scidb_version = repo$.private$dep$get_scidb_version()
  savedNames = c("connectionArgs", "repo", "scidb_version", "username")
  
  targetEnv = if(save_to_default_conn) default_conn else new.env(parent = emptyenv())
  
  sapply(savedNames, function(x) assign(x, get(x), targetEnv))
  
  invisible(targetEnv)
}
