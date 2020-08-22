
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
get_default_connection = function(.report_error_if_not_connected = TRUE) { 
  if(.report_error_if_not_connected)
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
      # res = array_op_from_schema_str(uploaded@meta$schema)$create_new_with_same_schema(uploaded@name)
      res = array_op_from_scidbr_obj(uploaded)
      res$.set_meta('.ref', uploaded) # prevent GC
      res
    },
    # generate afl by recursively 'join' two arrays
    join_arrays_by_two = function(array_names) {
      assert(length(array_names) > 1)
      
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
    },
    array_op_from_scidbr_obj = function(obj) {
      array_op_from_schema_str(obj@meta$schema)$create_new_with_same_schema(
        obj@name
      )
    },
    # Get an array as template from a 't' or a 'dataFrame' if 't' is null
    get_array_template = function(t, dataFrame = NULL){
      if(is.null(t)) t = infer_scidb_types_from_df(dataFrame)
      if(inherits(t, "ArrayOpBase")) return(t)
      if(is.list(t)){
        assert_named_list(t, "ERROR: get_array_template: a list as template must have names")
        return(repo$ArrayOp('', attrs = names(t), dtypes = t))
      }
      if(is.character(t) && length(t) == 1) {
        if(grepl("<", t))
          return(array_op_from_schema_str(t))
        else if(grepl("^\\w+\\.\\w+$", t))
          return(array_op_from_name(t))
      }
      
      stopf("ERROR: get_array_template: template must be an array_op or named list, but got: [%s]", 
            paste(class(t), collapse = ','))

    }
    ,
    # Return a named list of scidb data types in string format
    infer_scidb_types_from_df = function(dataFrame) {
      colClasses = lapply(dataFrame, function(vec){
        if(inherits(vec, "integer")) "int32"
        else if(inherits(vec, "integer64")) "int64"
        else if(inherits(vec, "numeric")) "double"
        else if(inherits(vec, c("factor", "character"))) "string"
        else if(inherits(vec, "logical")) "bool"
        else if(inherits(vec, c("Date", "POSIXct", "POSIXt"))) "datetime"
        else NA
      })
      naCols = Filter(is.na, colClasses)
      assertf(length(naCols) == 0, 
              "{.symbol} has unknown-typed column(s): [{.na_cols}]",
              .symbol = deparse(substitute(dataFrame)),
              .na_cols = paste(names(naCols), collapse = ','))
      colClasses
    }
    ,
    set_array_op_conn = function(array_op) {
      array_op$.private$set_conn(self)
      array_op
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
    query = function(afl_str){
      assert_single_str(afl_str)
      repo$query(afl_str)
    }
    ,
    query_attrs = function(afl_str){
      assert_single_str(afl_str)
      repo$query(afl_str, only_attributes = TRUE)
    }
    ,
    execute = function(afl_str) {
      assert_single_str(afl_str)
      repo$execute(afl_str)
      invisible(self)
    }
    ,
    create_new_scidb_array = function(name, schema_template, .temp = FALSE) {
      assert_single_str(name)
      assert_inherits(schema_template, c("character", "ArrayOpBase"))
      schema_array = if(is.character(schema_template)) 
        array_op_from_schema_str(schema_template) else schema_template
      self$execute(
        sprintf("CREATE %s ARRAY %s %s", 
                if(.temp) "TEMP" else "", 
                name, 
                schema_array$to_schema_str()
                )
      )
      array_op_from_name(name)
    }
    ,
    array_op_from_name = function(array_name) {
      assert_single_str(array_name, "ERROR: param 'array_name' must be a single string")
      result = repo$load_arrayop_from_scidb(array_name)
      set_array_op_conn(result)
      result
    }
    ,
    array_op_from_schema_str = function(schema_str) {
      assert_single_str(schema_str, "ERROR: param 'schema_str' must be a single string")
      result = repo$private$.get_array_from_schema_string(schema_str)
      set_array_op_conn(result)
      result
    }
    ,
    array_op_from_afl = function(afl_str) {
      assert_single_str(afl_str, "ERROR: param 'afl_str' must be a single string")
      escapedAfl = gsub("'", "\\\\'", afl_str)
      schema = query_attrs(sprintf("project(show('%s', 'afl'), schema)", escapedAfl))
      # schemaArray = repo$private$.get_array_from_schema_string(schema[["schema"]])
      schemaArray = array_op_from_schema_str(schema[["schema"]])
      result = schemaArray$create_new_with_same_schema(afl_str)
      set_array_op_conn(result)
      result
    }
    ,
    #' @description 
    #' Create a persistent array_op by storing AFL as an array
    #' @param .temp Whether to create a temporary scidb array.
    #' Only effective when `save_array_name = NULL`
    array_op_from_stored_afl = function(
      afl_str, 
      # save_array_name = utility$random_array_name(),
      save_array_name = NULL,
      .temp = FALSE,
      .gc = TRUE
    ) {
      assert_single_str(afl_str, "ERROR: param 'afl_str' must be a single string")
      if(!is.null(save_array_name))
        assert_single_str(save_array_name, "ERROR: param 'save_array_name' must be a single string or NULL")
      
      storedAfl = scidb::scidb(.db, afl_str)
      storedArray = scidb::store(.db, expr = storedAfl, name = save_array_name, 
                                 temp = .temp, gc = .gc)
      
      res = array_op_from_scidbr_obj(storedArray)
      res$.set_meta('.ref', storedArray)
      set_array_op_conn(res)
      res
    }
    ,
    array_op_from_df = function(
      df, 
      template = NULL,
      build_or_upload_threshold = 8000L,
      build_dim_spec = .random_field_name(),
      skip_scidb_schema_check = FALSE,
      ...
    ){
      cellCount = dim(df)[[1L]] * dim(df)[[2L]]
      if(cellCount <= build_or_upload_threshold){
        array_op_from_build_literal(
          df, 
          template = template,
          build_dim_spec = build_dim_spec,
          skip_scidb_schema_check = skip_scidb_schema_check
          )
      } else {
        array_op_from_uploaded_df(df, template = template, ...)
      }
    }
    ,
    array_op_from_uploaded_df = function(
      df, 
      template = NULL, 
      name = utility$random_array_name(), 
      upload_by_vector = FALSE,
      .use_aio_input = FALSE, 
      .temp = FALSE,
      .gc = TRUE
    ) {
      assert_inherits(df, "data.frame")
      assertf(nrow(df) > 0, "{.symbol} must be a non-empty data frame")
      array_template = get_array_template(template, df)
      # assert all df fields are in the array_template
      matchedFields = {
        dfFieldsNotInArray = names(df) %-% array_template$dims_n_attrs
        assertf(length(dfFieldsNotInArray) == 0,
                "Data frame {.symbol} has field(s) [{.fields}] that do not match the template '{.template}'",
                .symbol = deparse(substitute(df)), 
                .fields = paste(dfFieldsNotInArray, collapse = ','),
                .template = deparse(substitute(template)), 
                )
        names(df) %n% array_template$dims_n_attrs
      }
      
      result = 
      if(upload_by_vector){
        vectorArrays = sapply(matchedFields, function(fieldName) {
          private$upload_df_or_vector(
            df[[fieldName]], 
            name = sprintf("%s_%s_", name, fieldName), 
            attr = fieldName,
            types = as.character(array_template$.private$get_field_types(fieldName, .raw = TRUE)),
            use_aio_input = .use_aio_input, temp = .temp, gc = .gc
          )
        })
        vectorArrayNames = sapply(vectorArrays, function(x) x$to_afl())
        result = if(length(vectorArrays) == 1){
          vectorArrays[[1]]
        } else {
          joinAfl = join_arrays_by_two(vectorArrayNames)
          .result = array_op_from_afl(joinAfl)
          .result$.set_meta('.ref', vectorArrays)
          .result
        }
      }
      else {
        private$upload_df_or_vector(
          df,
          name = name, 
          types = array_template$.private$get_field_types(names(df), .raw=TRUE),
          use_aio_input = .use_aio_input, temp = .temp, gc = .gc
        )
      }
      set_array_op_conn(result)
      result
    }
    ,
    array_op_from_build_literal = function(
      df, template = NULL, 
      build_dim_spec = .random_field_name(),
      skip_scidb_schema_check = FALSE
    ) {
      assert(nrow(df) >= 1, "ERROR: param 'df' must have at least one row")
      array_template = get_array_template(template, df)
      buildOp = array_template$build_new(df, build_dim_spec)
      result = if(skip_scidb_schema_check){
        buildOp
      } else {
        # We need to infer schema from the 'build' afl, but avoid unnecessary data transfer to scidb/shim.
        # So only the first row is sent to 'probe' the correct array schema
        probeOp = array_template$build_new(head(df, 1), build_dim_spec)
        remoteSchema = array_op_from_afl(probeOp$to_afl())
        # Still use the buildOp for actual data
        remoteSchema$create_new_with_same_schema(buildOp$to_afl())
      }
      set_array_op_conn(result)
      result
    }
    ,
    fread = function(file_path, template = NULL, header = TRUE, sep = '\t', col.names = NULL, nrow = 10L) {
      assertf(file.exists(file_path))
      assert_inherits(header, "logical")
      
      if(is.null(template) || header){
        # if col.names not provided, then it's inferred from the result data frame by peeking into the input file
        dtParams = list(file = file_path, header = header, sep = sep, nrow = nrow) %>% 
          modifyList(list(col.names = col.names), keep.null = FALSE)
        peekedDf = do.call(data.table::fread, dtParams)
        file_headers = names(peekedDf)
        array_op_template = get_array_template(template, peekedDf)
      } else {
        array_op_template = get_array_template(template, NULL)
        file_headers = col.names %?% array_op_template$dims_n_attrs
      } 
      # NULL  # the assumed column headers (strings) in the input file
      
      array_op_template$load_file(
        file_path,
        file_headers = file_headers,
        aio_settings = list(header = if (header) 1L else 0L,
                            attribute_delimiter = sep)
      ) %>% set_array_op_conn
    }
  )
)
