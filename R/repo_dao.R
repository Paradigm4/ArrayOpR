#' Create a RepoDao instance 
#' 
#' Other than Repo class, the created RepoDao instance handles higher-level use cases and includes convenience functions.
#' 
#' This should be only way to create a new RepoDao instance.
#' @param dependency_obj A named list that contains 4 functions: get_scidb_version, query, execute and get_schema_df
#' If left NULL, will require a scidb connection 'db' to create the dependency_obj automatically.
#' @param repo a Repo instance that processes low-level ArrayOp operations
#' @export
newRepoDao = function(repo, db, ignore_error = F, use_aio_input = T, ...) {
  RepoDAO$new(repo, db, ignore_error = ignore_error, use_aio_input = use_aio_input, ...)
}

RepoDAO <- R6::R6Class(
  "RepoDAO", portable = FALSE,
  private = list(
    repo = NULL,
    db = NULL,
    NS = NULL,
    parser = NULL,
    name_sanitize_func = function(original) gsub("^[_]+", '', gsub('[^a-zA-Z0-9_]+', '_', original))
    ,
    assert_no_fields = function(fields, fmt){
      assert_not_has_len(fields, fmt, paste(fields, collapse = ','))
    }
    ,
    evaluate_statement = function(stmt, .env = parent.frame()) {
      parsedExpr = parser$parse_arrayop_expr(stmt)
      aliases = names(repo$meta$schema_registry)
      envSchemas = list2env(
        rlang::set_names(lapply(aliases, repo$get_alias_schema), aliases),
        parent = .env
      )
      # 'evaluated' should be an ArrayOp object or a string
      eval(parsedExpr, envSchemas)
    }
    ,
    store = function(target, df = NULL, append = T, path = NULL) {
      anArray = get_array(target)
      uploaded = upload_df(df, anArray)
      
      arrayFieldsNotInDf = anArray$dims_n_attrs %-% names(df)
      writeOp = if(!.has_len(arrayFieldsNotInDf)){
        uploaded$write_to(anArray, append = append)
      } else {
        target_auto_increment = rlang::set_names(rlang::rep_along(arrayFieldsNotInDf, as.integer(0)), arrayFieldsNotInDf)
        uploaded$reshape(select=uploaded$attrs, dim_mode='drop', artificial_field='z')$
          write_to(anArray, source_auto_increment=c('z'=0), target_auto_increment=target_auto_increment)
      }
      execute_raw(writeOp)
    }
    ,
    .get_array_from_schema_string = function(schemaStr) {
      
      matched = stringr::str_match(schemaStr, "(\\S*)\\s*\\<(.+)\\>\\s*\\[(.+)\\]")
      assert(dim(matched)[[1]] == 1,
             "ERROR: RepoDAO$get_array_from_schema_string: Invalid schema string: %s", schemaStr)
      
      arrayName = matched[1,2]
      attrStr = matched[1,3]
      dimStr = matched[1,4]
      attrMatrix = stringr::str_match_all(attrStr, "(\\w+):\\s*([^;,:]+)")[[1]]
      dimMatrix = stringr::str_match_all(dimStr, "(\\w+)[=\\s]*([^;]*)")[[1]]
      
      dims = dimMatrix[,2]
      attrs = attrMatrix[,2]
      
      attrDtypes = as.list(rlang::set_names(attrMatrix[,3], attrs))
      dimDtypes = as.list(rlang::set_names(rep('int64', dim(dimMatrix)[[1]]), dims))
      
      repo$ArrayOp(arrayName, dims, attrs,
                   dtypes = c(attrDtypes, dimDtypes),
                   dim_specs=as.list(rlang::set_names(dimMatrix[,3], dims)))
    }
    ,
    execute_raw = function(what, ...) {
      try(repo$execute(what, ...), silent = settings$ignore_error)
    }
    ,
    query_raw = function(what, ...) {
      try(repo$query(what, ...), silent = settings$ignore_error)
    }
    ,
    # Get an operand represented by 'what'
    # @param what: arrayOp instance, or a string (either AFL statement or an R expression)
    # @param .raw: If .raw = FALSE and 'what' is a string, then 'what' is parsed to an arrayOp instance; 
    # Otherwise, .raw = TRUE, return 'what' as the operand 
    # @return: An arrayOp isntance or a AFL statement
    get_operand = function(what, .raw, .env) {
      assert(inherits(what, 'ArrayOpBase') || 
               (inherits(what, 'character') && length(what) == 1), 
             "ERROR: RepoDAO: get_operand: param 'what' must be a single ArrayOp instance or string, but got: [%s]",
             paste(class(what), collapse = ','))
      if(is.character(what) && !.raw) # 'what' is an R expression
        evaluate_statement(what, .env = .env)  # Eval the expression and return an arrayOp
      else
        what
    }
  )
  # Public ----
  ,
  public = list(
    settings = NULL,
    #' Initialize function
    #'
    #' @param repo A arrayop::Repo instance, which is versioned for scidb V18.x or V19.x
    #' @param db A scidb db instance
    #' @param ignore_error Whether to ignore errors in scidb queries. Default FALSE
    #' @param use_aio_input Whether to use aio_input for uploading data frames to scidb
    #' @param ... Other settings. E.g. key1 = value1, key2 = value2
    initialize = function(repo, db, ignore_error = F, use_aio_input = T, ...) {
      private$repo = repo
      private$db = db
      private$NS = repo$meta[['default_namespace']]
      private$parser = StatementParser$new()
      self$settings = list(ignore_error = ignore_error, use_aio_input = use_aio_input, ...)
    }
    ,
    #' Get an ArrayOp instance
    #'
    #' Param `what` can be a string or arrayOp instance. If a string, then `what` is either a registered array alias,
    #' or raw array schema (e.g. myArray <a:int32, b:string> [d=0:*:0:*])
    #'
    #' This function is provide unified access of arrayOp instances
    #' @param what An array alias, raw array schema or arrayOp instance
    #' @return An ArrayOp instance
    get_array = function(what) {
      # if(length(grepl("<.+>\\s+\\[.*\\]", what)) > 1 || length(inherits(what, "ArrayOpBase")) > 1) browser()
      # tryCatch({if(is.character(what)){
      if(is.character(what)){
        if(grepl("<.+>\\s+\\[.*\\]", what)){
          return(.get_array_from_schema_string(what))
        } else {
          return(repo$get_alias_schema(what))
        }
      } else if(inherits(what, "ArrayOpBase")){
        return(what)
      }
      stopf("ERROR: RepoDAO$get_array: param 'what' is not an array alias, schema string or an ArrayOp instance. class(what)=%s",
            paste(class(what), collapse = ','))
    }
    ,
    #' Read data frame
    #'
    #' Column names of the data frame are sanitized. Although implemented by data.table::fread, return a regular R
    #' data frame instead of `data.table`
    #' @param name A file path or raw file content, as definied in data.table::fread function
    #' @return A regular data frame instead of data.table
    read_df = function(name, ...) {
      res = data.table::fread(name, ..., data.table = F, na.strings = 'NA')
      names(res) <- name_sanitize_func(names(res))
      res
    }
    ,
    #' Create a new data frame
    #'
    #' This function exists to use default params `stringAsFactors = F` and `row.names = NULL`
    #' @param ... Arguments directly passed to `data.frame(...)`
    #' @return A data frame
    new_df = function(...) {
      data.frame(..., stringsAsFactors = F, row.names = NULL)
    }
    ,
    overwrite = function(target, df = NULL) {
      store(target, df = df, append = F)
    }
    ,
    insert = function(target, df = NULL) {
      store(target, df = df, append = T)
    }
    ,
    #' Run a query and get a data frame
    #'
    #' The query statement `stmt` is a string.
    #' @param stmt A string, where
    #' 1. array aliases are replaced by corresponding arrayOp instances
    #' 2. functions following arrays are called as arrayOp functions
    #' 3. extra arrayOp functions can be provided in parentheses
    #' 4. any other variables in `.env` will be replaced by its values
    #' @param ... Arguments passed directly to scidb::iquery function
    #' @param .dry_run If TRUE, only return evaluated query statement. Default FALSE
    #' @param .env An R env or list for R variable substitution
    #' @return A data frame
    query = function(what, ..., .dry_run = FALSE, .raw = FALSE, .env = parent.frame()) {
      op = get_operand(what, .raw = .raw, .env = .env)
      if(.dry_run)
        op
      else
        query_raw(op, ...)
    }
    ,
    #' Execute a query.
    #'
    #' The query statement `stmt` is a string. No result is returned.
    #' @param stmt A string, where
    #' 1. array aliases are replaced by corresponding arrayOp instances
    #' 2. functions following arrays are called as arrayOp functions
    #' 3. extra arrayOp functions can be provided in parentheses
    #' 4. any other variables in `.env` will be replaced by its values
    #' @param ... Arguments passed directly to scidb::iquery function
    #' @param .dry_run If TRUE, only return evaluated query statement. Default FALSE
    #' @param .env An R env or list for R variable substitution
    #' @return NULL
    execute = function(what, ..., .dry_run = FALSE, .raw = FALSE, .env = parent.frame()) {
      op = get_operand(what, .raw = .raw, .env = .env)
      if(.dry_run)
        op
      else
        execute_raw(op, ...)
    }
    # Functions for internal use only ---------------------------------------------------------------------------------
    ,
    .namespace_exists = function(ns = NS){
      query_raw(arrayop::afl("list('namespaces')" %filter%
                               arrayop::afl_filter_from_expr(arrayop::e(name == !!ns)) %op_count% NULL
      ))$count == 1
    }
    ,
    .ensure_namespace = function(ns = NS) {
      if(!.namespace_exists(ns))
        execute_raw(sprintf("create_namespace('%s')", ns)) else
          printf("Namespace %s already exists.", ns)
    }
    ,
    .clear_namespace = function(ns, .sure = FALSE) {
      if(!.sure) printf(
        "DANGER!!! Please set .sure = TRUE to run this function. All arrays in namespace '%s' will be removed.
This can NOT be undone!!!", ns)
      for(n in .get_array_names_from_default_namespace()){
        execute_raw(sprintf("remove(%s.%s)", ns, n))
      }
    }
    ,
    .ensure_repo_arrays = function() {
      absentArrays = names(repo$meta[['schema_registry']]) %-% .get_array_names_from_default_namespace()
      for(n in absentArrays){
        .create_array(n)
      }
    }
    ,
    .remove_array = function(alias){
      execute_raw(
        get_array(alias)$remove_array_cmd()
      )
    }
    ,
    .remove_array_versions = function(alias, ...) {
      execute_raw(
        get_array(alias)$remove_array_versions_cmd(...)
      )
    }
    ,
    .create_array = function(alias, new_array_name = NULL){
      anArray = repo$get_alias_schema(alias)
      if(is.null(new_array_name)) new_array_name = anArray$to_afl()
      execute_raw(
        anArray$create_array_cmd(new_array_name)
      )
    }
    ,
    .reset_array = function(alias) {
      .remove_array(alias)
      .create_array(alias)
    }
    ,
    .get_array_names_from_default_namespace = function() {
      query_raw(sprintf("project(list(ns:%s), name)", NS))[['name']]
    }
    ,
    .copy_arrays = function(source_ns, target_ns, array_names, limit = NULL, reset_target_before_copy = TRUE, .dry_run = T) {
      .ensure_namespace(target_ns)
      existingArrays = query_raw(sprintf("project(list(ns:%s), name)", target_ns), only_attributes = TRUE)$name
      for(arr in array_names){
        sourceArr = sprintf("%s.%s", source_ns, arr)
        targetArr = sprintf("%s.%s", target_ns, arr)
        if(reset_target_before_copy && !.dry_run){
          if(arr %in% existingArrays) execute_raw(sprintf("remove(%s)", targetArr))
        }
        source = if(is.numeric(limit)) sprintf("limit(%s,%d)", sourceArr, limit) else sourceArr
        stmt = arrayop::afl(source %store% targetArr)
        if(.dry_run)
          cat(stmt, '\n')
        else
          execute_raw(stmt)
      }
    }
    ,
    nrow = function(what, .dry_run = FALSE, .raw = FALSE, .env = parent.frame()) {
      # op = if(.raw) what else query(what, .dry_run = T)
      op = get_operand(what, .raw = .raw, .env = .env)
      res = query_raw(arrayop::afl(op %op_count% NULL))
      res$count
    }
    ,
    limit = function(what, count, offset = NULL, .raw = FALSE, .env = parent.frame()) {
      # op = if(.raw) what else query(what, .dry_run = T)
      op = get_operand(what, .raw = .raw, .env = .env)
      res = query_raw(arrayop::afl(op %limit% c(count, offset)))
      res
    }
   
    ,
    infer_array_schema = function(df, dim_specs = NULL, afl_literal = '', ...) {
      
      .r2scidb_type = function(rclass) {
        if(rclass == 'numeric') 'double'
        else if(rclass == 'integer') 'int32'
        else if(rclass == 'integer64') 'int64'
        else if(rclass == 'logical') 'bool'
        else if(rclass %in% c('character', 'factor')) 'string'
        else 'date'
      }
      
      dfColNames = name_sanitize_func(names(df))
      scidb_types = sapply(df, function(x) .r2scidb_type(class(x)))
      anArray = repo$ArrayOp(afl_literal, attrs = dfColNames, dims = names(dim_specs),
                             dtypes = as.list(rlang::set_names(scidb_types, dfColNames)),
                             dim_specs = dim_specs)
      anArray
    }
    ,
    #' @description
    #' Upload a data frame to scidb
    #'
    #' Upload a data frame into scidb with data types from the template.
    #' The data frame can have fewer columns than the template, but it can not have columns not found in the template.
    #'
    #' @param df a data frame whose column names has been sanitized
    #' @param template An array alias, schema string, arrayOp; or a named list of data types
    #' @param temp Whether to upload the df as a temporary array. Default TRUE.
    #' @param ... Other args passed to scidb::as.scidb
    #' @return An arrayOp instance with uploaded data.
    #' The resultant arrayOp$to_afl() is the temporary array name R_array****, unless specified explicitly with name='newName'
    #' The resultant arrayOp schema is consisted of 1. artificial dimension(s) from upload or aio_input;
    #' 2. attributes whose names are the data frame column names and data types are those in the template.
    upload_df = function(df, template, temp = TRUE, ...) {
      
      .scidb2r_type_func = function(type) {
        if(type %in% c('int8', 'int16', 'int32', 'uint8', 'uint16', 'uint32')) type = 'int32'
        else if(type %in% c('uint64')) type = 'int64'
        switch (type,
                'int32' = as.integer, 'int64' = bit64::as.integer64, 'bool' = as.logical, 'string' = as.character,
                'double' = as.double,
                'datetime' = function(x) as.POSIXct(x),
                stopf("ERROR: RepoDAO$upload_df: Unknown scidb type %s", type)
        )
      }
      
      convert_columns = function(rawDf) {
        dtypes = array_template$get_field_types(matchedFields, .raw = T)
        for(name in names(dtypes)){
          rawDf[[name]] = .scidb2r_type_func(dtypes[[name]])(df[[name]])
        }
        rawDf
      }
      
      array_template = if(is.list(template))
        repo$ArrayOp('', attrs = names(template), dtypes = template)
      else get_array(template)
      
      dfFieldsNotInArray = names(df) %-% array_template$dims_n_attrs
      matchedFields = names(df) %n% array_template$dims_n_attrs
      # browser() #repo_dao
      assert_not_has_len(
        dfFieldsNotInArray,
        "ERROR: Data frame has non-matching field(s): %s for array %s", paste(dfFieldsNotInArray, collapse = ','),
        str(array_template))
      
      df = convert_columns(df)
      uploaded = scidb::as.scidb(db, df, use_aio_input = settings$use_aio_input, temp = temp, ...)
      
      res = array_template$
        create_new(
          uploaded@name, dims = 'i', attrs = matchedFields,
          dtypes = c(array_template$get_field_types(matchedFields), 'i'='int64')
        )
      res$.set_meta('.ref', uploaded)
      res
    }
  )
)
