
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
  
  targetEnv = if(save_to_default_conn) default_conn else empty_connection()
  
  targetEnv$connect(repo = repo, scidb_version = scidb_version, connection_args = connectionArgs)
  
  invisible(targetEnv)
}

# ScidbConnection class ----

print.ScidbConnection = str.ScidbConnection = function(conn) conn$to_str()

empty_connection = function() ScidbConnection$new()

ScidbConnection <- R6::R6Class(
  "ScidbConnection",
  portable = FALSE,
  private = list(
    repo = NULL,
    .scidb_version = NULL,
    .conn_args = NULL
  ),
  active = list(
    scidb_version = function() .scidb_version,
    username = function() .conn_args[["username"]],
    host = function() .conn_args[["host"]]
  ),
  public = list(
    initialize = function(){
      
    },
    connect = function(repo, scidb_version, connection_args) {
      private$repo = repo
      private$.scidb_version = scidb_version
      private$.conn_args = connection_args
    },
    to_str = function() {
      sprintf("ScidbConnection: %s@%s [%s]", username, host, scidb_version)
    }
  )
)