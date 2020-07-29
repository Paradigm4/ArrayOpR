
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

#' Get the default ScidbConnection
#' 
#' Call `connect` to establish a connection 
#' @export
get_default_connection = function() { default_conn }

# ScidbConnection class ----

#' @export
print.ScidbConnection = function(conn) conn$print()
#' @export
str.ScidbConnection = function(conn) conn$print()

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
    # scidb_version = function() private$.scidb_version,
    # username = function() private$.conn_args[["username"]],
    # host = function() private$.conn_args[["host"]]
  ),
  public = list(
    initialize = function(){
      
    },
    connect = function(repo, scidb_version, connection_args) {
      private$repo = repo
      private$.scidb_version = scidb_version
      private$.conn_args = connection_args
    },
    scidb_version = function() .scidb_version,
    conn_args = function() .conn_args,
    print = function() {
      sprintf("ScidbConnection: %s@%s [%s]", 
              .conn_args[["username"]], 
              .conn_args[["host"]], 
              scidb_version()
              )
    }
    ,
    query = function(afl_str, only_attributes = FALSE){
      assert_single_str(afl_str)
      repo$query(afl_str, only_attributes = only_attributes)
    }
    ,
    execute = function(afl_str) {
      assert_single_str(afl_str)
      repo$execute(afl_str, only_attributes = only_attributes)
      invisible(self)
    }
    ,
    array = function(array_name) {
      repo$load_arrayop_from_scidb(array_name)
    }
    ,
    array_op_from_name = function(array_name) {
      repo$load_arrayop_from_scidb(array_name)
    }
    ,
    array_op_from_afl = function(afl_str, store_as_array = NULL) {
      if(!is.null(store_as_array))
        assert_single_str(store_as_array, "ERROR: param 'store_as_array' must be a single string or NULL")
      escapedAfl = gsub("'", "\\\\'", afl_str)
      schema = query(sprintf("project(show('%s', 'afl'), schema)", escapedAfl), only_attributes = T)
      schemaArray = repo$private$.get_array_from_schema_string(schema[["schema"]])
      
      if(!is.null(store_as_array)){
        execute(sprintf("store(%s, %s)", afl_str, store_as_array))
        schemaArray$create_new_with_same_schema(store_as_array)
      } else {
        schemaArray$create_new_with_same_schema(afl_str)
      }
    }
  )
)
