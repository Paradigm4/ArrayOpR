
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
    .conn_args = NULL,
    # Private methods
    upload_df_or_vector = function(v, ...) {
      uploaded = scidb::as.scidb(private$.db, v, ...)
      res = array_op_from_schema_str(uploaded@meta$schema)$create_new_with_same_schema(uploaded@name)
      res$.set_meta('.ref', uploaded) # prevent GC
      res
    },
    # generate afl by recursively 'join' two arrays
    join_arrays_by_two = function(array_names) {
      
      .do.join = function(items) {
        splitByTwo = split(items, ceiling(seq_along(items)/2L))
        sapply(splitByTwo, function(x){
          if(length(x) == 1) x else 
            sprintf("join(%s, %s)", x[[1]], x[[2]])
        })
      }
      
      joinedItems = .do.join(array_names)
      while (length(joinedItems) > 1L) {
        joinedItems = .do.join(joinedItems)
      }
      joinedItems
    }
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
        assert(!is.null(connection_args), 
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
    array_op_from_stored_afl = function(afl_str, save_array_name = .random_array_name()) {
      assert_single_str(afl_str, "ERROR: param 'afl_str' must be a single string")
      assert_single_str(save_array_name, "ERROR: param 'save_array_name' must be a single string")
      
      execute(afl(afl_str | store(save_array_name)))
      array_op_from_name(save_array_name)
    }
    ,
    array_op_from_uploaded_df = function(
      df, 
      template, 
      name = .random_array_name(), 
      upload_by_vector = FALSE,
      .use_aio_input = FALSE, 
      .temp = FALSE,
      .gc = TRUE
    ) {
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
      
      # if(is.null(name)) name = .random_array_name()
      
      if(upload_by_vector){
        vectorArrays = sapply(matchedFields, function(fieldName) {
          private$upload_df_or_vector(
            df[[fieldName]], 
            name = sprintf("%s_%s_", name, fieldName), 
            attr = fieldName,
            types = as.character(array_template$get_field_types(fieldName, .raw = TRUE)),
            use_aio_input = .use_aio_input, temp = .temp, gc = .gc
          )
        })
        vectorArrayNames = sapply(vectorArrays, function(x) x$to_afl())
        joinAfl = join_arrays_by_two(vectorArrayNames)
        result = array_op_from_afl(joinAfl)
        result$.set_meta('.ref', vectorArrays)
        result
      } else {
        private$upload_df_or_vector(
          df,
          name = name, 
          types =  array_template$get_field_types(names(df), .raw=TRUE),
          use_aio_input = .use_aio_input, temp = .temp, gc = .gc
        )
      }
      
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
