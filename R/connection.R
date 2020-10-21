
#' Connect to SciDB server and get a ScidbConnection object.
#' 
#' @param username SciDB user name
#' @param token SciDB user password or token
#' @param port Port
#' @param protocol Protocol. "https" (preferred, secure) or "http"
#' @param auth_type Authentication type
#' @param ... other optional params used in scidb::scidbconnect
#' @param save_to_default_conn A boolean value that determines whether to save
#' the connection object as the default in the `arrayop` package scope so that 
#' it can be retrieved anywhere using `get_default_connection`.
#' 
#' In rare cases should we need to maintain multiple connection objects, we can
#' set `save_to_default_conn` to FALSE.
#' 
#' @return A Scidbconnection object.
#' 
#' @export
db_connect = function(username, token, 
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
#' Call `arrayop::db_connect` to establish a connection 
#' 
#' @param .report_error_if_not_connected Whether to report error if `db_connect`
#' has not been called before this function. Default TRUE to ensure a valid
#' connection object is returned or throw an error. Set to FALSE, if we need to
#' test if a connection object exists or not without causing an error. 
#' 
#' E.g. `conn_obj_available = get_default_connection(F)$has_connected()`
#' 
#' @return The default ScidbConnection object. Report error if `db_connect` is 
#' not called prior to this function.
#' @export
get_default_connection = function(.report_error_if_not_connected = TRUE) { 
  if(.report_error_if_not_connected)
    assert(default_conn$has_connected(), "please call `arrayop::db_connect` to create a valid connection first")
  default_conn 
}

# ScidbConnection class ----

#' @export
print.ScidbConnection = function(conn) conn$private$to_str()
#' @export
str.ScidbConnection = function(conn) conn$private$to_str()

empty_connection = function() ScidbConnection$new()

#' A Scidb connection
#' 
#' @description 
#' A connection object that talks to SciDB 
#' 
#' The connection object creates ArrayOp instances, execute AFL queries of arrayOps,
#' and download qurey results as R data frames.
ScidbConnection <- R6::R6Class(
  "ScidbConnection",
  portable = FALSE, 
  cloneable = FALSE,
  private = list(
    .scidb_version = NULL,
    .db = NULL,
    .conn_args = NULL,
    .arrayop_class = NULL,
    # Private methods
    .iquery = function(afl_str, return, only_attributes) {
      if(return){
        scidb::iquery(private$.db, afl_str, return = TRUE, only_attributes = only_attributes)
      } else {
        scidb::iquery(private$.db, afl_str, return = FALSE)
      }
    }
    ,
    new_ArrayOp = function(...) {
      .arrayop_class(...)
    }
    ,
    to_str = function() {
      glue("ScidbConnection: {.conn_args$username}@{.conn_args$host} ",
          "[{.scidb_version$major}.{.scidb_version$minor}.{.scidb_version$patch}]")
    }
    ,
    upload_df_or_vector = function(v, ...) {
      uploaded = scidb::as.scidb(private$.db, v, ...)
      res = array_op_from_scidbr_obj(uploaded)
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
    # Create an arrayOp from a scidbR object
    # 
    # Hold reference to the scidbR object to avoid premature removal of the bound
    # array
    array_op_from_scidbr_obj = function(obj) {
      result = array_from_schema(obj@meta$schema)$spawn(
        obj@name
      )$.private$confirm_schema_synced()
      result$.private$hold_refs(obj)
      result
    }
    ,
    # @description 
    # Create a persistent array_op by storing AFL as an array
    # @param .temp Whether to create a temporary scidb array.
    # Only effective when `save_array_name = NULL`
    array_from_stored_afl = function(
      afl_str, 
      # save_array_name = dbutils$random_array_name(),
      save_array_name = dbutils$random_array_name(),
      .temp = FALSE,
      .gc = TRUE
    ) {
      assert_single_str(afl_str, "ERROR: param 'afl_str' must be a single string")
      if(!is.null(save_array_name))
        assert_single_str(save_array_name, "ERROR: param 'save_array_name' must be a single string or NULL")
      
      newArray = self$create_array(save_array_name, self$afl_expr(afl_str), .temp = .temp)
      self$execute(afl(afl_str | store(save_array_name)))
      newArray$.private$set_gc(.gc)
      newArray
    }
    ,
    # Create an array_op from a schema string with an optional array name
    new_arrayop_from_schema_string = function(schema_str, .symbol) {
      # Return all matched substrings in 'x'
      # groups in pattern are ignored. The full pattern is looked up
      get_sub_recursive = function(pattern, x) {
        x = x[[1]]
        m = gregexpr(pattern, x)
        regmatches(x, m)[[1]]
      }
      
      # Return all matched groups explicit defined by parentheses.
      # If there are multiple matches, only the first is returned
      get_sub_groups = function(pattern, x) {
        x = x[[1]]
        m = regexec(pattern, x)
        regmatches(x, m)[[1]][-1]
      }
      
      parse_schema = function(schema_str) {
        parts = get_sub_groups(array_schema_pattern, schema_str)
        assertf(length(parts) > 1L, "Invalid array schema string `{.symbol}`: {.value}",
                .symbol = .symbol,
                .value = schema_str,
                .nframe = 2)
        arrayName = parts[[1]]
        attrStr = parts[[2]]
        dimStr = parts[[3]]
      
        attrVec = get_sub_recursive(single_attr_pattern, attrStr)
        dimVec = get_sub_recursive(single_dim_pattern, dimStr)
      
      
        list(array_name = arrayName,
          attrs = new_named_list(
            trimws(gsub(single_attr_pattern, "\\2", attrVec)),
            names = trimws(gsub(single_attr_pattern, "\\1", attrVec))),
          dims = new_named_list(
            trimws(gsub(single_dim_pattern, "\\3", dimVec)),
            names = trimws(gsub(single_dim_pattern, "\\1", dimVec)))
        )
      }
      
      # the first non-empty word next to `<` is parsed as the array name
      # because some afl operators return schema strings where array name is consisted of multiple words
      # E.g. show(list('operators'), 'afl') => "non empty operators <attrs...> [dims..]
      # 'operators' would be the array name in above example 
      array_schema_pattern = "([^< ]+)?\\s*(<[^<>]+>)\\s*(\\[[^]]+\\])?\\s*$"
      single_attr_pattern = "(\\w+)\\s*:\\s*([^;,:<>]+)"
      # add $ for Scidb data frames which have a leading $ in dimension name
      single_dim_pattern = "([[:alnum:]_$]+)(\\s*\\=\\s*([^];, \t]+))?"
      
      parsed = parse_schema(schema_str)
      private$new_ArrayOp(parsed$array_name,
                   dims = names(parsed$dims),
                   attrs = names(parsed$attrs),
                   dtypes = c(parsed$attrs, invert.list(list("int64" = names(parsed$dims)))),
                   dim_specs = parsed$dims)
    }
    ,
    # Get an array as template from a 't' or a 'dataFrame' if 't' is null
    #' @param array_like An array_op, schema string or existing scidb array name
    get_array_template = function(array_like, df = NULL, .df_symbol = deparse(substitute(df))){
      assertf(length(array_like) > 0 || length(df) > 0,
              "Error in get_array_template: no template found.")
      if(is.null(array_like)) array_like = infer_scidb_types_from_df(df, .symbol = .df_symbol)
      if(inherits(array_like, "ArrayOpBase")) return(array_like)
      if(is.list(array_like)){
        assert_named_list(array_like, "ERROR: get_array_template: a list as template must have names")
        return(private$new_ArrayOp('', attrs = names(array_like), dtypes = array_like))
      }
      if(is.character(array_like) && length(array_like) == 1) {
        if(grepl("<", array_like))
          return(array_from_schema(array_like))
        else if(grepl("^\\w+(\\.\\w+)?$", array_like))
          return(self$array(array_like))
      }
      
      stopf("ERROR: get_array_template: template must be an array_op or named list, but got: [%s]", 
            paste(class(array_like), collapse = ','))

    }
    ,
    # Return a named list of scidb data types in string format
    infer_scidb_types_from_df = function(dataFrame, .symbol = deparse(substitute(dataFrame))) {
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
              .symbol = .symbol,
              .na_cols = paste(names(naCols), collapse = ','))
      colClasses
    }
    ,
    set_array_op_conn = function(array_op) {
      array_op$.private$set_conn(self)
      array_op
    }
  )
  ,
  # public ----
  public = list(
    #' @description 
    #' Create a new ScidbConnection instance
    #' 
    #' This function is only for package internal use. 
    #' Please call `arrayop::db_connect` to get a ScidbConnection object
    #' @return A ScidbConenction instance
    initialize = function(){
      
    },
    #' @description 
    #' Whether this connection object is configured with connection arguments
    #' @return A boolean. TRUE if `db_connect` is called or FALSE otherwise.
    has_connected = function() {
      .not_empty(.conn_args)
    }
    ,
    #' @description 
    #' Connect to scidb with a list of connection arguments
    #' 
    #' Calling this function will update the current connection object's internal state.
    #' If the connection has timed out, just call this function without args will
    #' re-establish the connection with previously configured connection args.
    #' @param connection_args NULL or a list of connection args. 
    #' Default NULL means using the previous connection args. 
    #' A list of connection args follow the same names as params in `db_connect`.
    #' @return The same connection object with updated internal state.
    connect = function(connection_args = NULL) {
      if(is.null(connection_args)){
        connection_args = conn_args()
        assert(!is.null(connection_args), 
                  "ERROR: no 'connection_args' found. Please connect with username, token, etc at least once.")
      }
      db = do.call(scidb::scidbconnect, connection_args)
      private$.conn_args = connection_args
      private$.db = db
      private$.scidb_version = query("op_scidbversion()")
      # choose the right ArrayOp class version
      private$.arrayop_class = if(.scidb_version$major == 18){
        ArrayOpV18$new
      } else if(.scidb_version$major >= 19) {
        ArrayOpV19$new
      } else {
        stop(glue("Unsupported SciDB version: [{.scidb_version$major}.{.scidb_version$minor}.{.scidb_version$patch}]"))
      }
      invisible(self)
    },
    #' @description 
    #' Return an R data frame of the currently connected SciDB server version
    #' @return A data frame. One row and three numeric columns: "major", "minor", "patch"
    scidb_version = function() .scidb_version,
    #' @description 
    #' Return a list of connection args used to establish the scidb connection
    #' 
    #' Connection args follow the same names as `db_connect` args.
    #' @return A named list of connection args
    conn_args = function() .conn_args,
    #' @description 
    #' Download a data frame of the query result with all dimensions and attributes
    #' @param afl_str A string of AFL expression
    #' @return R data frame
    query_all = function(afl_str){
      assert_single_str(afl_str)
      private$.iquery(afl_str, return = TRUE, only_attributes = FALSE)
    }
    ,
    #' @description 
    #' Download a data frame of the query result with all attributes
    #' @param afl_str A string of AFL expression
    #' @return R data frame
    query = function(afl_str){
      assert_single_str(afl_str)
      private$.iquery(afl_str, return = TRUE, only_attributes = TRUE)
    }
    ,
    #' @description 
    #' Execute AFL expression without result
    #' 
    #' Use this for pure side effect and no result is downloaded.
    #' E.g. create arrays, remove arrays, remove array versions, update arrays.
    #' @param afl_str A string of AFL expression
    execute = function(afl_str) {
      assert_single_str(afl_str)
      private$.iquery(afl_str, return = FALSE)
      invisible(self)
    }
    ,
    #' @description 
    #' Create a new scidb array and return the arrayOp instance for it
    #' @param name Scidb array name. E.g. 'myNamespace.myArray'. If no namespace
    #' in array name, it will be created in the 'public' namespace.
    #' @param schema_template A scidb schema string or an arrayOp instance, used
    #' as the schema template for the newly created array. 
    #' @param .temp Boolean. Whether to create the array as a scidb temporary array
    #' @return An arrayOp instance of the newly created array
    create_array = function(name, schema_template, .temp = FALSE) {
      assert_single_str(name)
      assert_inherits(schema_template, c("character", "ArrayOpBase"))
      schema_array = if(is.character(schema_template)) 
        array_from_schema(schema_template) else schema_template
      self$execute(
        sprintf("CREATE %s ARRAY %s %s", 
                if(.temp) "TEMP" else "", 
                name, 
                schema_array$to_schema_str()
                )
      )
      self$array(name)
    }
    ,
    #' @description 
    #' Execute multiple AFL statments wrapped in one `mquery` with transcation gurantee
    #' 
    #' `mquery` currently only supports top operators `insert` and `delete`
    #' @param ... AFL strings and/or arrayOp isntances. `arrayOp$to_afl()` will 
    #' be called to generate AFL strings. 
    #' @param .dry_run A single boolean value, default FALSE. If TRUE, only print
    #' out the mquery; otherwise execute it in scidb without returns. 
    #' @examples 
    #' conn$execute_mquery(
    #'   target$filter(conc < 50)$update(target),
    #'   target$delete_cells(Plant %contains% "3", uptake > 10)
    #' )
    execute_mquery = function(..., .dry_run = FALSE) {
      stmts = c(...)
      lapply(stmts, function(x) assert_inherits(x, c("character", "ArrayOpBase")))
      stmtStrs = sapply(stmts, function(x) if(is.character(x)) x else x$to_afl())
      mqueryStmt = afl(as.character(stmtStrs) | mquery)
      if(.dry_run) {
        cat(glue("About to execute mquery:\n{mqueryStmt}"))
      } else {
        self$execute(mqueryStmt)
      }
    }
    ,
    #' @description 
    #' Get an ArrayOp instance of an existing scidb array
    #' 
    #' The scidb array denoted by the array_name must exsit in scidb.
    #' @param array_name Scidb array name. E.g. 'myNamespace.myArray'. If no namespace
    #' in array name, it will be searched in the 'public' namespace.
    #' @return An arrayOp instance
    array = function(array_name) {
      assert_single_str(array_name, "ERROR: param 'array_name' must be a single string")
      array_name = trimws(array_name)
      schema = query(sprintf("project(show(%s), schema)", array_name))
      # 'show' operator only returns partial array name (without namespace)
      result = array_from_schema(schema[["schema"]])$spawn(array_name)
      result$.private$confirm_schema_synced()
      set_array_op_conn(result)
    }
    ,
    #' @description 
    #' Create an ArrayOp instance from array schema
    #' 
    #' Useful in creating an arrayOp as a template for other arrayOp operations.
    #' If an array name provided in the schema_str, the array of the same name
    #' does not have to exist in scidb. Obviously, you cannnot download data from
    #' a non-existent array, but it can be used as template for other operations.
    #' 
    #' @param schema_str A scidb-format array schema. The array name is optional.
    #' E.g. `<fa:int32, fb:string COMPRESSION 'zlib'> [i;j]` creates an arrayOp
    #' with the specified attributes and dimensions, and an empty afl string.
    #' E.g. `myArray <fa:int32, fb:string COMPRESSION 'zlib'> [i;j]` creates an arrayOp
    #' with the specified attributes and dimensions, and encapsulates an afl string of "myArray".
    #' The array named "myArray" does not need to exist in scidb since this is done
    #' only in local R env without checking in scidb.
    #' 
    #' @return An arrayOp instance
    array_from_schema = function(schema_str) {
      assert_single_str(schema_str, "ERROR: param 'schema_str' must be a single string")
      result = private$new_arrayop_from_schema_string(
        schema_str, 
        .symbol = deparse(substitute(schema_str)))
      set_array_op_conn(result)
    }
    ,
    #' @description 
    #' Create an arrayOp instance from an AFL expression
    #' 
    #' Implemented with scidb 'show' operator.
    #' @param afl_str A AFL expression string. Can be any array operation in AFL 
    #' except for a scidb array name.
    #' @return An arrayOp instance with schema from scidb
    afl_expr = function(afl_str) {
      assert_single_str(afl_str, "ERROR: param 'afl_str' must be a single string")
      
      # If afl_str is a scidb array name
      if(!grepl("\\(", afl_str) && 
        grepl("^((\\w+)\\.)?(\\w+)$", afl_str)) 
        return(self$array(afl_str))
      
      # If afl_str is array operation
      escapedAfl = gsub("'", "\\\\'", afl_str)
      schema = query(sprintf("project(show('%s', 'afl'), schema)", escapedAfl))
      schemaArray = array_from_schema(schema[["schema"]])
      result = schemaArray$spawn(afl_str)
      result$.private$confirm_schema_synced()
      set_array_op_conn(result)
      result
    }
    ,
    #' @description 
    #' Get an arrayOp instance from an R data frame
    #' 
    #' Implemented by scidb 'build' operator or SciDBR `scidb::as.scidb` function
    #' which uploads a data frame to scidb. 
    #' If the number of cells (nrow * ncolumns) of the data frame is smaller than the 'build_or_upload_threshold',
    #' use 'build' operator to create a build literal array; otherwise,
    #' a persistent scidb array is created by uploading the R data frame into scidb.
    #' @param df an R data frame
    #' @param template The array schema template can be NULL, an arrayOp, or a scidb 
    #' schema string. If NULL, inferr scidb data types from the classes of data frame columns.
    #' If arrayOp, use the actual scidb field types for matching columns of the R data frame.
    #' If schmea string, infer field types the same way as an arrayOp instance.
    #' @param build_or_upload_threshold An integer, below which the scidb 'build' 
    #' operator is used to create a build literal; otherwise, upload the data frame
    #' into scidb with SciDBR's `scidb::as.scidb` function.
    #' @param build_dim_spec The build dimension spec if 'build' operator is chosen.
    #' Can be either a simple field name or a full dimension spec. 
    #' E.g. "z", or "z=0:*:0:100"
    #' @param as_scidb_data_frame Boolean. If FALSE (default), create a scidb
    #' data frame (no explicit dimensions); otherwise, create a regular scidb array.
    #' Applicable for 'build' literal only.
    #' @param skip_scidb_schema_check Boolean. If FALSE (default), check with scidb
    #' to determine the exact schema of result arrayOp; otherwise, infer the schema
    #' locally (which is not accurate; but saves a round trip to scidb server and 
    #' work in most cases if not used as an template).
    #' @param force_template_schema Boolean. If FALSE (default), do not change 
    #' the result arrayOp schema to be compatible with the template using 'redimension'
    #' operator. If TRUE, force the result arrayOp to use the same schema as the 
    #' template, which must be provided (not NULL).
    #' @param ... other params used in `upload_df` function.
    #' @return An arrayOp instance that encapsulates a build literal or uploaded R data frame(s)
    array_from_df = function(
      df, 
      template = NULL,
      build_or_upload_threshold = 8000L,
      build_dim_spec = .random_field_name(),
      as_scidb_data_frame = FALSE,
      skip_scidb_schema_check = FALSE,
      force_template_schema = FALSE,
      ...
    ){
      cellCount = dim(df)[[1L]] * dim(df)[[2L]]
      if(cellCount <= build_or_upload_threshold){
        compile_df(
          df, 
          template = template,
          build_dim_spec = build_dim_spec, 
          as_scidb_data_frame = as_scidb_data_frame,
          force_template_schema = force_template_schema,
          skip_scidb_schema_check = skip_scidb_schema_check
          )
      } else {
        upload_df(
          df, 
          template = template, 
          force_template_schema = force_template_schema, 
          ...)
      }
    }
    ,
    #' @description 
    #' Get an arrayOp instance from uploaded R data frame
    #' 
    #' Implemented by SciDBR `scidb::as.scidb` function which uploads a 
    #' data frame or vector(s) to scidb. 
    #' 
    #' By default, the uploaded R data frame is saved to scidb as a persistent array.
    #' If `upload_by_vecotr = TRUE`, multiple scidb arrays are created, each for 
    #' one of the R data frame column by uploading individual vectors, which is
    #' faster but suffers from bugs in ScidbR. 
    #' @param df an R data frame
    #' @param template The array schema template can be NULL, an arrayOp, or a scidb 
    #' schema string. If NULL, inferr scidb data types from the classes of data frame columns.
    #' If arrayOp, use the actual scidb field types for matching columns of the R data frame.
    #' If schmea string, infer field types the same way as an arrayOp instance.
    #' @param name A string as the uploaded scidb array name, only applicable when
    #' `upload_by_vector = FALSE` in which case a single scidb array is created.
    #' @param force_template_schema Boolean. If FALSE (default), do not change 
    #' the result arrayOp schema to be compatible with the template using 'redimension'
    #' operator. If TRUE, force the result arrayOp to use the same schema as the 
    #' template, which must be provided (not NULL).
    #' @param upload_by_vector Boolean. If TRUE, upload R data frame by its vectors,
    #' which is faster than upload R data frame as a whole but suffers from unresolved 
    #' ScidbR bugs. If FALSE (default), upload R data frame as a whole as a sicdb array.
    #' @param .use_aio_input Boolean, default FALSE. Whether to use 'aio_input' to 
    #' import the uploaded data frame on scidb server side. If TRUE, 'aio_input'
    #' is faster than the default 'input' operator, but suffers from some bugs in 
    #' the 'aio_input' scidb plugin.
    #' @param .temp Boolean, default FALSE. Whether to save the uploaded data frame
    #' as a temporary scidb array. 
    #' @param .gc Boolean, default TRUE. Whether to remove the uploaded scidb array
    #' once the encapsulating arrayOp goes out of scodb in R. 
    #' @return An arrayOp instance that encapsulates a build literal or uploaded R data frame(s)
    upload_df = function(
      df, 
      template = NULL, 
      name = dbutils$random_array_name(), 
      force_template_schema = FALSE,
      upload_by_vector = FALSE,
      .use_aio_input = FALSE, 
      .temp = FALSE,
      .gc = TRUE
    ) {
      assert_inherits(df, "data.frame")
      assertf(nrow(df) > 0, "{.symbol} must be a non-empty data frame")
      if(force_template_schema)
        assert_not_empty(template, "param template cannot be NULL when `force_template_schema=T`")
      
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
            types = as.character(array_template$.private$get_field_types(fieldName)),
            use_aio_input = .use_aio_input, temp = .temp, gc = .gc
          )
        })
        vectorArrayNames = sapply(vectorArrays, function(x) x$to_afl())
        result = if(length(vectorArrays) == 1){
          vectorArrays[[1]]
        } else {
          joinAfl = join_arrays_by_two(vectorArrayNames)
          .result = afl_expr(joinAfl)
          .result$.private$hold_refs(vectorArrays)
          .result
        }
      }
      else {
        private$upload_df_or_vector(
          df,
          name = name, 
          types = array_template$.private$get_field_types(names(df)),
          use_aio_input = .use_aio_input, temp = .temp, gc = .gc
        )
      }
      if(force_template_schema){
        oldResult = result
        result = oldResult$change_schema(array_template)
        result$.private$hold_refs(oldResult)
      }
      set_array_op_conn(result)
    }
    ,
    #' @description 
    #' Get an arrayOp instance by compiling an R data frame into a scidb build literal 
    #' 
    #' Implemented by scidb 'build' operator. No persistent scidb array will be created.
    #' @param df an R data frame
    #' @param template The array schema template can be NULL, an arrayOp, or a scidb 
    #' schema string. If NULL, inferr scidb data types from the classes of data frame columns.
    #' If arrayOp, use the actual scidb field types for matching columns of the R data frame.
    #' If schmea string, infer field types the same way as an arrayOp instance.
    #' @param build_dim_spec The build dimension spec if 'build' operator is chosen.
    #' Can be either a simple field name or a full dimension spec. 
    #' E.g. "z", or "z=0:*:0:100"
    #' @param as_scidb_data_frame Boolean. If FALSE (default), create a scidb
    #' data frame (no explicit dimensions); otherwise, create a regular scidb array.
    #' Applicable for 'build' literal only.
    #' @param skip_scidb_schema_check Boolean. If FALSE (default), check with scidb
    #' to determine the exact schema of result arrayOp; otherwise, infer the schema
    #' locally (which is not accurate; but saves a round trip to scidb server and 
    #' work in most cases if not used as an template).
    #' @param force_template_schema Boolean. If FALSE (default), do not change 
    #' the result arrayOp schema to be compatible with the template using 'redimension'
    #' operator. If TRUE, force the result arrayOp to use the same schema as the 
    #' template, which must be provided (not NULL).
    #' @return An arrayOp instance that encapsulates a build literal or uploaded R data frame(s)
    compile_df = function(
      df, template = NULL, 
      build_dim_spec = dbutils$random_field_name(),
      force_template_schema = FALSE,
      as_scidb_data_frame = FALSE,
      skip_scidb_schema_check = FALSE
    ) {
      assert(nrow(df) >= 1 && length(df) >= 1, "ERROR: param 'df' must not be empty")
      if(force_template_schema)
        assert_not_empty(template, "param template cannot be NULL when `force_template_schema=T`")
      
      array_template = get_array_template(template, df, .df_symbol = deparse(substitute(df)))
      buildOp = array_template$.private$build_new(df, build_dim_spec, as_scidb_data_frame = as_scidb_data_frame)
      result = if(skip_scidb_schema_check){
        buildOp
      } else {
        # We need to infer schema from the 'build' afl, but avoid unnecessary data transfer to scidb/shim.
        # So only the first row is sent to 'probe' the correct array schema
        probeOp = array_template$.private$build_new(head(df, 1), build_dim_spec, as_scidb_data_frame = as_scidb_data_frame)
        remoteSchema = afl_expr(probeOp$to_afl())
        # Still use the buildOp for actual data
        remoteSchema$
          spawn(buildOp$to_afl())$
          .private$confirm_schema_synced()
      }
      if(force_template_schema)
        result = result$change_schema(array_template)
      set_array_op_conn(result)
    }
    ,
    #' @description 
    #' Get an arrayOp instance that encapsulates an array operation using 
    #' 'aio_input' operator to read content from a file
    #' 
    #' Param convenctions similar to `data.table::fread ` function. 
    #' We can choose if column names/types should be inferred from peaking into
    #' the file by setting `header = T` and `nrow = 10` for how many rows to peek
    #' for inference. 
    #' 
    #' Unique to scidb, we can control how the file columns are converted and 
    #' whether to use multiple scidb instances to read multiple files in parallel.
    #' @param file_path A single string or a string vector, for a local file
    #' path or a list of paths. If multiple paths provided, the `instances` param
    #' must be set to the same number as `file_path`.
    #' @param template The array schema template can be NULL, an arrayOp, or a scidb 
    #' schema string. If NULL, inferr scidb data types by peeking into the file
    #' and read a small data frame of `nrows` with `data.table::fread`. Sensible
    #' data type conversion between R and scidb will be performed.
    #' If arrayOp, use the actual scidb field types for matching file columns.
    #' If schmea string, infer field types the same way as an arrayOp instance.
    #' @param header Boolean, default TRUE. Whether to use the first file row
    #' to infer file column names and data types.
    #' @param sep A single character string as the field delimiter, default `"\t"` 
    #' for TSV files. Set to `","` for CSV files.
    #' @param col.names NULL (default) or a string vector. 
    #' If `col.names = NULL, header = T`, file column names are inferred from the
    #' first file row.
    #' If `col.names = NULL, header = F, tempalte = NULL`, file column names follow the `data.table::fread`
    #' convention and are named as `V1, V2, ... etc`.
    #' If `col.names = NULL, header = F, tempalte = anTemplate`, assume file columns
    #' are in the same order as the template's dimensions + attributes. 
    #' If set to a string vector, its length must match the actual file columns, 
    #' and the acutal file column names are replaced with the provided `col.names`, but
    #' data types are still inferred from the actual file columns.
    #' @param mutate_fields NULL or a list of R expressions. When `auto_dcast = T`,
    #' this setting prevails. Similar to `ArrayOpBase$mutate`.
    #' E.g. `a = b + 2, name = first + "-" + last, chrom = if(chrom == 'x') 23 else if(chrom == 'y') 24 else chrom`
    #' @param auto_dcast Boolean, default FALSE. If TRUE, all non-string fields are 
    #' dcast'ed with `dcast(ax, int64(null)), where ax is the 0-indexed mapping 
    #' attribute name (e.g. a0, a1, etc), and int64 is the template field type.
    #' If FALSE, force coerce file columns into scidb types for all non-string fields,
    #' e.g. double(a0), int32(a1). Error will be thrown if incompatible field content
    #' is read during execution of this `fread` function, not the `fread` itself since
    #' it doesn't actually execute any operation.
    #' 
    #' Even if `auto_dcast = T` which is useful in many cases when file is not strictly
    #' formatted, we can still overwrite the `dcast` rule by setting a `mutate_fields` expression
    #' list, as seen in `ArrayOpBase$mutate`.
    #' @param nrow An integer, default 10. How many rows to peek into the file to infer
    #' column names and data types using `data.table::fread`.
    #' @param instances NULL (default) or an integer vector. For single file path,
    #' set to NULL. For multiple file paths, set the same number of instances as
    #' the file paths, each reading from a file path in parallel. 
    #' @param .aio_settings NULL (default) or a list of extra aio_input settings.
    #' Basic aio_input settings including path, num_attributes, and header are 
    #' generated automatically and should not be manually provided. See scidb doc
    #' on extra aio_input settings.
    #' @return An arrayOp instance that encapsulates a 'aio_input' operation with
    #' auto generated field mapping and data type conversion.
    fread = function(file_path, template = NULL, header = TRUE, sep = '\t',
                     col.names = NULL, mutate_fields = NULL, auto_dcast = FALSE, 
                     nrow = 10L, instances = NULL, .aio_settings=NULL) {
      assert_inherits(file_path, "character")
      assertf(all(file.exists(file_path)))
      if(length(file_path) > 1){
        assertf(is.numeric(instances) && length(instances) == length(file_path), 
          glue("Please provide {length(file_path)} instance ids (integers) ",
               "because length(file_path)={length(file_path)}, length(instances)={length(instances)}",
               "\nOnly when single file_path provided, can param `instances` be ommitted."),
          .symbol = deparse(substitute(instances)))
      }
      assert_inherits(header, "logical")
      assert_inherits(auto_dcast, c("logical"))
      
      # If there is no template provided, we peek into the file for column names/types
      if(is.null(template) || header){
        # Following data.table::fread semantics, if col.names not provided, 
        # they are either 1. inferred by first data row if header=T; 
        # or 2. assigned wtih V1, V2, etc..
        # If col.names is provided, it must be the same number as the actual columns 
        # and will overwite the column names from file
        dtParams = list(file = file_path[[1]], header = header, sep = sep, nrow = nrow) %>% 
          modifyList(list(col.names = col.names), keep.null = FALSE)
        peekedDf = do.call(data.table::fread, dtParams)
        file_headers = names(peekedDf)
        arrayTemplate = get_array_template(template, peekedDf)
      } else { # If template provided, we first convert it to an array_op
        arrayTemplate = get_array_template(template, NULL)
        # use col.names if provided, or assume file columns to be the same as 
        # template's fields (dims + attrs)
        file_headers = col.names %?% arrayTemplate$dims_n_attrs
      } 
      
      if(auto_dcast){
        mappingFields = file_headers %n% arrayTemplate$dims_n_attrs
        nonStrFields = arrayTemplate$
          .private$get_field_types(mappingFields, .raw = TRUE) %>% 
          Filter(function(x) x != 'string', .)
        autoDcastList = lapply(nonStrFields, function(x) sprintf("dcast(@, %s(null))", x))
        mutate_fields = modifyList(autoDcastList, as.list(mutate_fields))
      }
      
      aio_settings = list(header = if (header) 1L else 0L,
                          attribute_delimiter = sep) %>%
        modifyList(c(list(instances = instances), .aio_settings)) # if instance is NULL, then it's not included
      
      arrayTemplate$.private$load_file(
        file_path,
        file_headers = file_headers,
        aio_settings = aio_settings,
        field_conversion = mutate_fields
      ) %>% set_array_op_conn
    }
  )
)
