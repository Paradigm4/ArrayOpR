
#' Connect to SciDB server
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
#' @export
get_default_connection = function(.report_error_if_not_connected = TRUE) { 
  if(.report_error_if_not_connected)
    assert(default_conn$is_connected(), "ERROR: please call `arrayop::db_connect` to create a valid connection first")
  default_conn 
}

# ScidbConnection class ----

#' @export
print.ScidbConnection = function(conn) conn$private$to_str()
#' @export
str.ScidbConnection = function(conn) conn$private$to_str()

empty_connection = function() ScidbConnection$new()

ScidbConnection <- R6::R6Class(
  "ScidbConnection",
  portable = FALSE,
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
      array_from_schema(obj@meta$schema)$spawn(
        obj@name
      )$.private$confirm_schema_synced()
    }
    ,
    #' @description 
    #' Create a persistent array_op by storing AFL as an array
    #' @param .temp Whether to create a temporary scidb array.
    #' Only effective when `save_array_name = NULL`
    array_from_stored_afl = function(
      afl_str, 
      # save_array_name = dbutils$random_array_name(),
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
      
      result = array_op_from_scidbr_obj(storedArray)
      result$.private$confirm_schema_synced()
      result$.set_meta('.ref', storedArray)
      set_array_op_conn(result)
    }
    ,
    #' Create an array_op from a schema string with an optional array name
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
  ),
  active = list(
      # placeholder for active bindings, which do not work for package-level singletons.
    dbversion = function() private$.scidb_version
  ),
  public = list(
    initialize = function(){
      
    },
    is_connected = function() {
      .not_empty(.conn_args)
    }
    ,
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
    scidb_version = function() .scidb_version,
    conn_args = function() .conn_args,
    query_all = function(afl_str){
      assert_single_str(afl_str)
      private$.iquery(afl_str, return = TRUE, only_attributes = FALSE)
    }
    ,
    query = function(afl_str){
      assert_single_str(afl_str)
      private$.iquery(afl_str, return = TRUE, only_attributes = TRUE)
    }
    ,
    execute = function(afl_str) {
      assert_single_str(afl_str)
      private$.iquery(afl_str, return = FALSE)
      invisible(self)
    }
    ,
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
    array_from_schema = function(schema_str) {
      assert_single_str(schema_str, "ERROR: param 'schema_str' must be a single string")
      result = private$new_arrayop_from_schema_string(
        schema_str, 
        .symbol = deparse(substitute(schema_str)))
      set_array_op_conn(result)
    }
    ,
    afl_expr = function(afl_str) {
      assert_single_str(afl_str, "ERROR: param 'afl_str' must be a single string")
      escapedAfl = gsub("'", "\\\\'", afl_str)
      schema = query(sprintf("project(show('%s', 'afl'), schema)", escapedAfl))
      schemaArray = array_from_schema(schema[["schema"]])
      result = schemaArray$spawn(afl_str)
      result$.private$confirm_schema_synced()
      set_array_op_conn(result)
      result
    }
    ,
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
            types = as.character(array_template$.private$get_field_types(fieldName, .raw = TRUE)),
            use_aio_input = .use_aio_input, temp = .temp, gc = .gc
          )
        })
        vectorArrayNames = sapply(vectorArrays, function(x) x$to_afl())
        result = if(length(vectorArrays) == 1){
          vectorArrays[[1]]
        } else {
          joinAfl = join_arrays_by_two(vectorArrayNames)
          .result = afl_expr(joinAfl)
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
      if(force_template_schema)
        result = result$change_schema(array_template)
      set_array_op_conn(result)
    }
    ,
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
    #' Get a transient array_op by loading a file with aio_input
    #' 
    #' Mimic data.table::fread 
    #' @param auto_dcast logical, default F. If TRUE, all non-string fields are 
    #' dcast'ed with `dcast(ax, int64(null)), where ax is the 0-indexed mapping 
    #' attribute name (e.g. a0, a1, etc), and int64 is the template field type
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
