
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
  
  targetEnv = if(save_to_default_conn) default_conn else empty_connection()
  targetEnv$connect(connection_args = connectionArgs)
  
  invisible(targetEnv)
}

#' Get the default ScidbConnection
#' 
#' Call `connect` to establish a connection 
#' @export
get_default_connection = function() { 
  assert(default_conn$is_connected(), "ERROR: please call `arrayop::connect` to create a valid connection first")
  default_conn 
}

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
    .db = NULL,
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
    is_connected = function() {
      .has_len(.conn_args)
    }
    ,
    connect = function(connection_args = NULL) {
      if(is.null(connection_args)){
        connection_args = conn_args()
        stopifnot(!is.null(connection_args), 
                  "ERROR: no 'connection_args' found. Please connect with username, token, etc at least once.")
      }
      db = do.call(scidb::scidbconnect, connection_args)
      repo = newRepo(db = db)
      scidb_version = repo$.private$dep$get_scidb_version()
      
      private$repo = repo
      private$.scidb_version = scidb_version
      private$.conn_args = connection_args
      private$.db = db
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
    array_op_from_name = function(array_name) {
      assert_single_str(array_name, "ERROR: param 'array_name' must be a single string")
      repo$load_arrayop_from_scidb(array_name)
    }
    ,
    array_op_from_schema_str = function(schema_str) {
      assert_single_str(schema_str, "ERROR: param 'schema_str' must be a single string")
      repo$private$.get_array_from_schema_string(schema_str)
    }
    ,
    array_op_from_afl = function(afl_str) {
      assert_single_str(afl_str, "ERROR: param 'afl_str' must be a single string")
      escapedAfl = gsub("'", "\\\\'", afl_str)
      schema = query(sprintf("project(show('%s', 'afl'), schema)", escapedAfl), only_attributes = T)
      # schemaArray = repo$private$.get_array_from_schema_string(schema[["schema"]])
      schemaArray = array_op_from_schema_str(schema[["schema"]])
      schemaArray$create_new_with_same_schema(afl_str)
    }
    ,
    array_op_from_uploaded_df = function(df, template, 
                                         name = NULL, 
                                         .use_aio_input = TRUE, .temp = FALSE) {
      # array_template = if(is.list(template))
      #   self$ArrayOp('', attrs = names(template), dtypes = template)
      # else get_array(template)
      array_template = template
      # browser()
      dfFieldsNotInArray = names(df) %-% array_template$dims_n_attrs
      matchedFields = names(df) %n% array_template$dims_n_attrs
      assert_not_has_len(
        dfFieldsNotInArray,
        "ERROR: Data frame has non-matching field(s): %s for array %s", paste(dfFieldsNotInArray, collapse = ','),
        str(array_template))
      uploaded = scidb::as.scidb(
        private$.db, df, 
        name = name,
        use_aio_input = .use_aio_input, temp = .temp, 
        types =  array_template$get_field_types(names(df), .raw=TRUE)
      )
      
      res = array_op_from_name(uploaded@name)
      res$.set_meta('.ref', uploaded) # prevent GC
      res
    }
    ,
    array_op_from_build_literal = function(
      df, template, 
      build_dim_spec = .random_field_name(),
      skip_scidb_schema_check = FALSE
    ) {
      assert(nrow(df) >= 1, "ERROR: param 'df' must have at least one row")
      buildOp = template$build_new(df, build_dim_spec)
      if(skip_scidb_schema_check){
        buildOp
      } else {
        # We need to infer schema from the 'build' afl, but avoid unnecessary data transfer to scidb/shim.
        # So only the first row is sent to 'probe' the correct array schema
        probeOp = template$build_new(df[1,], build_dim_spec)
        remoteSchema = array_op_from_afl(probeOp$to_afl())
        # Still use the buildOp for actual data
        remoteSchema$create_new_with_same_schema(buildOp$to_afl())
      }
    }
  )
)
