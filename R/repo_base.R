# Private function for the Repo class
default_dep_obj = function(db) {
  runQuery = function(query, ...) scidb::iquery(db, query, return = TRUE, ...)
  runCmd = function(query, ...) scidb::iquery(db, query, return = FALSE, ...)
  list(
    get_scidb_version = function() {
      df = runQuery("op_scidbversion()", only_attributes = T)[1, c('major', 'minor')]
      sprintf("%s.%s", df[['major']], df[['minor']])
    }
    ,
    query = runQuery
    ,
    execute = runCmd
    ,
    upload_df_to_scidb = function(df, ...){
      
    }
    ,
    store_afl_to_scidb = function(afl_stmt, ...){
      
    }
    ,
    # Keep this for debugging only
    .db = db
  )
}

# New Repo Class ----

#' Create a scidb-version-specific Repo instance 
#' 
#' The created Repo instance is automatically selected based on Repo-Scidb compatibility
#' 
#' This should be only way to create a new Repo instance, 
#' unless a specific Repo version is known/desired beforehand, which is useful in test cases.
#' @param dependency_obj A named list that contains 4 functions: get_scidb_version, query, execute and get_schema_df
#' If left NULL, will require a scidb connection 'db' to create the dependency_obj automatically.
#' @param db a scidb connection returned by scidb::scidbconnect. If provided, a `dependency_obj` is automatically created
#' with default implementations. 
#' @export
newRepo = function(dependency_obj = NULL, db = NULL){
  
  # Validate dependency_obj has all required methods
  requiredNames = c('get_scidb_version', 'query', 'execute', 'upload_df_to_scidb', 'store_afl_to_scidb')
  if(!.has_len(dependency_obj)){
    if(.has_len(db))
      dependency_obj = default_dep_obj(db)
    else
      stop("ERROR:newRepo: must provide a param 'dependency_obj' or 'db'. ")
  }
  assert_no_fields(requiredNames %-% names(dependency_obj), 
                   "ERROR:newRepo: param 'dependency_obj' misses function(s): %s")
  
  # Check scidb version
  fullScidbVersion = dependency_obj$get_scidb_version()
  # Use major version for now
  scidbVersion = gsub("\\..+", '', fullScidbVersion)
  
  repoClass = switch(scidbVersion, 
                     '18' = RepoV18,
                     '19' = RepoV19,
                     stopf("ERROR:newRepo:Unsupported scidb version '%s'", fullScidbVersion)
  )
  result = repoClass$new(dependency_obj)
  return(result)
}

#' RepoBase class manages ArrayOp objects in one scidb installation
#' @description 
#' A repository for scidb arrays and derived ArrayOp objects that reside in one scidb installation
#' @details 
#' RepoBase is the base class of scidb-version-specific Repo classes. 
#' Common ogic/behaviors across all supported scidb versions are placed here. 
#' Version-specific logic/behaviors should be implemented in RepoBase sub-classes. 
#' @export
Repo <- R6::R6Class(
  "Repo",
  portable = FALSE,
  # Private ----
  private = list(
    dep = NULL
    ,
    metaList = NULL
    ,
    parser = NULL
    ,
    array_alias_registry = NULL
    ,
    set_meta = function(key, value) {
      private$metaList[[key]] <- value
    }
    ,
    get_meta = function(key, default) {
      val = private$metaList[[key]]
      if(is.null(val)) default else val
    }
    ,
    evaluate_statement = function(stmt, .env = parent.frame()) {
      parsedExpr = parser$parse_arrayop_expr(stmt)
      aliases = names(array_alias_registry)
      envSchemas = list2env(
        rlang::set_names(lapply(aliases, self$get_array), aliases),
        parent = .env
      )
      # 'evaluated' should be an ArrayOp object or a string
      eval(parsedExpr, envSchemas)
    }
    ,
    get_afl_statement = function(what){
      aflStmt = if(is.character(what)) what
      else if(inherits(what, 'ArrayOpBase')) what$to_afl()
      else stopf("ERROR:Repo$get_afl_statement:param 'what' must be a string or arrayOp instance, but got [%s]",
                 paste(class(what), collapse = ','))
      return(aflStmt)
    }
    ,
    # Execute a scidb statement. No result is returned.
    # 
    # If scidb error occurs, the AFL will be printed.
    # @param what arrayOp instance or a raw AFL statement
    # @return NULL
    execute_raw = function(what, ...) {
      aflStmt = get_afl_statement(what)
      tryCatch({
        nullResult <- if(!setting_debug) dep$execute(aflStmt, ...) else 
          log_job_duration(dep$execute(aflStmt, ...), sprintf("Repo execute: %s", aflStmt))
      },
      error = function(e) {
        print_error("ERROR:Repo$execute:\n%s", aflStmt) # Print out AFL only when error occurs.
        stop(e)
      })
      return(nullResult)
    }
    ,
    # Run a scidb query. 
    # 
    # If scidb error occurs, the AFL will be printed.
    # @param what arrayOp instance or a raw AFL statement
    # @return a data frame
    query_raw = function(what, ...) {
      # If ... contains only_attributes = TRUE, then op's dimensions are effectively dropped.
      drop_dims = methods::hasArg('only_attributes') && list(...)[['only_attributes']]
      aflStmt = if(is.character(what)) what
        else if(inherits(what, 'ArrayOpBase'))
          what$to_df_afl(drop_dims)
        else stopf("ERROR:Repo$query_raw:param 'what' must be a string or arrayOp instance, but got [%s]",
                 paste(class(what), collapse = ','))
      tryCatch({
        df <- if(!setting_debug) dep$query(aflStmt, ...) else 
          log_job_duration(dep$query(aflStmt, ...), sprintf("Repo query: %s", aflStmt))
      },
      error = function(e) {
        print_error("ERROR:Repo$query:\n%s", aflStmt) # Print out AFL only when error occurs.
        stop(e)
      })
      return(df)
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
             "ERROR: Repo: get_operand: param 'what' must be a single ArrayOp instance or string, but got: [%s]",
             paste(class(what), collapse = ','))
      if(is.character(what) && !.raw) # 'what' is an R expression
        evaluate_statement(what, .env = .env)  # Eval the expression and return an arrayOp
      else
        what
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
      
      self$ArrayOp(arrayName, dims, attrs,
                   dtypes = c(attrDtypes, dimDtypes),
                   dim_specs=as.list(rlang::set_names(dimMatrix[,3], dims)))
    }
  )
  ,
  # Active bindings ----
  active = list(
    #' @field .private For internal testing only. Do not access this field to avoid unintended consequences!!!
    .private = function() private
    ,
    #' @field setting_debug Whether to enable debug mode. Default FALSE. 
    #' `Repo$setting_debug` to get current setting; or `Repo$setting_debug = T` to change current setting.
    setting_debug = function(value) if(missing(value)) get_meta('debug', FALSE) else set_meta('debug', as.logical(value))
    ,
    #' @field setting_use_aio_input Whether to use aio_input for data frame uploading. Default FALSE.
    #' `Repo$setting_use_aio_input` to get current setting; or `Repo$setting_use_aio_input = T` to change current setting.
    setting_use_aio_input = function(value) if(missing(value)) get_meta('use_aio_input', FALSE) else set_meta('use_aio_input', as.logical(value))
    ,
    #' @field setting_build_or_upload_threshold A threshold that determines whether to upload or build arrayOps. Default 5000.
    #' If number of data frame cells <= the threshold, choose 'build' operator where data frames are passed to scdib server in AFL; 
    #' otherwise, data frames are uploaded. 'build' mode is normally faster than 'upload'.
    #' `Repo$setting_build_or_upload_threshold` to get current setting; or `Repo$setting_build_or_upload_threshold = a_number` to change current setting.
    setting_build_or_upload_threshold = function(value) if(missing(value)) get_meta('build_or_upload_threshold', 5000L) else set_meta('build_or_upload_threshold', as.integer(value))
    ,
    #' @field setting_auto_match_filter_mode_threshold 
    #' A threshold that determines when to use 'filter' mode in `Repo$auto_match` function. Default 200.
    #' `Repo$setting_auto_match_filter_mode_threshold` to get current setting; or `Repo$setting_auto_match_filter_mode_threshold = a_number` to change current setting.
    setting_auto_match_filter_mode_threshold = function(value) if(missing(value)) get_meta('auto_match_filter_mode_threshold', 200L) else set_meta('auto_match_filter_mode_threshold', as.integer(value))
  )
  ,
  # Public ----
  public = list(
    #' @description
    #' Initialize function.
    #'
    #' Create a new Repo instance
    #' 
    #' Do NOT call this `initialize` function directly. Call `newRepo` instead to get a new Repo instance.
    initialize = function(dependency_object = NULL) {
      private$dep = dependency_object
      private$metaList = list()
      private$parser = StatementParser$new()
      private$array_alias_registry = list()
    }
    ,
    #' @description 
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
    #' @param .raw If TRUE and what is a string, do not evaluate, otherwise evaluate `what`. Default FALSE.
    #' Only applicable if `what` is a string.
    #' @param .env An R env or list for R variable substitution
    #' @return A data frame
    query = function(what, ..., .dry_run = FALSE, .raw = TRUE, .env = parent.frame()) {
      op = get_operand(what, .raw = .raw, .env = .env)
      if(.dry_run)
        op
      else
        query_raw(op, ...)
    }
    ,
    #' @description 
    #' Execute a query. No result is returned.
    #'
    #' The query statement `stmt` is a string. No result is returned.
    #' @param what A string, where
    #' 1. array aliases are replaced by corresponding arrayOp instances
    #' 2. functions following arrays are called as arrayOp functions
    #' 3. extra arrayOp functions can be provided in parentheses
    #' 4. any other variables in `.env` will be replaced by its values
    #' @param ... Arguments passed directly to scidb::iquery function
    #' @param .dry_run If TRUE, only return evaluated query statement. Default FALSE
    #' @param .raw If TRUE and what is a string, do not evaluate, otherwise evaluate `what`. Default FALSE.
    #' Only applicable if `what` is a string.
    #' @param .env An R env or list for R variable substitution
    #' @return NULL
    execute = function(what, ..., .dry_run = FALSE, .raw = TRUE, .env = parent.frame()) {
      op = get_operand(what, .raw = .raw, .env = .env)
      if(.dry_run)
        op
      else
        execute_raw(op, ...)
    }
    ,
    #' @description 
    #' Get an ArrayOp instance
    #'
    #' Param `what` can be a string or arrayOp instance. If a string, then `what` is either a registered array alias,
    #' or raw array schema (e.g. myArray <a:int32, b:string> \[d=0:*:0:*\])
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
          aliasArray = array_alias_registry[[what]]
          assert_has_len(aliasArray, "ERROR:Repo$get_array: '%s' is not a registered array alias.", what)
          # If the array alias is registered with an array name, then first time access trigers a `load_arrayop_from_scidb`
          # call. Later access will return the cached arrayOp instance.
          if(is.character(aliasArray)){
            cached = load_arrayop_from_scidb(aliasArray)
            private$array_alias_registry[[what]] <- cached
            return(cached)
          }
          return(aliasArray)
        }
      } 
      else if(inherits(what, "ArrayOpBase")){
        return(what)
      }
      stopf("ERROR:Repo$get_array: param 'what' is not an array alias, schema string or an ArrayOp instance. class(what)=%s",
            paste(class(what), collapse = ','))
    }
    ,
    #' @description 
    #' Register a list of arrays with aliases
    #'  
    #' Register a list of arrays which can be later accessed via their aliases
    #' 
    #' @param items A list where names are aliases and values are arrayOp instances or raw array names
    #' For arrayop values, they will be directly returned. 
    #' For string values, first access `Repo$get_array('alias')` will triger a `load_arrayop_from_scidb` call and 
    #' cache the returned arrayop for later access.
    #' @return NULL Use `Repo$get_array('alias')` to retrieve registered arrays
    register_arrayops = function(items){
      assert_named_list(items, "ERROR:Repo$register_arrayops: param 'items' must be a named list where every element has a name, and the value is an ArrayOp instance or raw array name.")
      private$array_alias_registry = utils::modifyList(private$array_alias_registry, items)
    }
    ,
    #' @description 
    #' Get an ArrayOp instance from scidb by its full name 
    #' 
    #' No cache is provided with this function. 
    #' See `register_arrayops` for details on registering an array alias with cache.
    #' 
    #' @param full_array_name A fully qualified array name (e.g. myNamespace.arrayName)
    #' @return An ArrayOp instance
    load_arrayop_from_scidb = function(full_array_name){
      assert_single_str(full_array_name,
             "ERROR:Repo$load_arrayop_from_scidb: param 'full_array_name' must be a single scidb array name")
      schemaStr = query_raw(afl(full_array_name | show | project('schema')))[['schema']]
      assert_single_str(schemaStr,
             "ERROR:Repo$load_arrayop_from_scidb: '%s' is not a valid scidb array", full_array_name)
      # schemaStr only contains an array name without the namespace. We need to change it to the full array name.
      get_array(schemaStr)$create_new_with_same_schema(full_array_name)
    }
    ,
    #' @description 
    #' Get a list of arrays from a scidb namespace
    #' 
    #' @param namespace A scidb namespace
    #' @return A list of ArrayOp instances where names are the array names (without namespace) and 
    #' values are the arrayOp instances whose to_afl() function return fully qualified array names
    load_arrayops_from_scidb_namespace = function(namespace){
      assert_single_str(namespace,
             "ERROR:Repo$load_arrayops_from_scidb_namespace: param 'namespace' must be a single scidb namespace")
      schemaStr = query_raw(afl(sprintf("list(ns:%s)", namespace) | project('schema')))[['schema']]
      assert(is.character(schemaStr),
             "ERROR:Repo$load_arrayops_from_scidb_namespace: '%s' is not a valid scidb namespace", namespace)
      rawArrays = sapply(schemaStr, get_array) # The arrays only have names with the namespace prefix
      arrayNamesWithoutNs = sapply(rawArrays, function(x) x$to_afl())
      fullNameArrays = sapply(rawArrays, function(x){
        x$create_new_with_same_schema(sprintf("%s.%s", namespace, x$to_afl()))
      })
      return(as.list(rlang::set_names(fullNameArrays, arrayNamesWithoutNs)))
    }
    ,
    #' @description 
    #' Load a list of ArrayOp instances from a config object
    #' 
    #' Only load and return a list of arrayOp instances. Repo's array registery will not be updated.
    #' If desired, you can `loaded = repo$load_arrayops_from_config; repo$register_arrayops(loaded)` to register loaded arrays.
    #' @param config A config object (R list) normally loaded from a yaml config file. The config list must have
    #' 'namespace' and 'arrays' keys. Value of 'namespace' is a single string for the default namespace.
    #' Value of 'arrays' is a list of unamed elements, where each element is a list of three items: 
    #' 1. 'alias': a single string for array alias
    #' 2. 'name': raw array name. If without a namespace, then the default namespace is applied.
    #'  Otherwise, if in 'namespace.rawArrayName', then use it directly disregard of the default namespace.
    #' 3. 'schema': a single string for array schema. E.g. "<aa:string, b:bool null> \[da=0:*:0:*\]
    #' If 'schema' missing, the array's full name will be returned, which later can be registered as well.
    #' @return A list of ArrayOp instances where names are array aliases and values are the ArrayOp instances
    load_arrayops_from_config = function(config) {
      assert(is.list(config), "ERROR:Repo$load_arrayops_from_config:'config' must be a list, but got [%s] instead.", paste(class(config), collapse = ','))
      assert_no_fields(c('namespace', 'arrays') %-% names(config),
                         "ERROR:Repo$load_arrayops_from_config:param 'config' missing section(s): %s")
      defaultNamespace = config$namespace
      arraySection = config$arrays
      aliases = sapply(arraySection, function(x) x$alias)
      assert_single_str(defaultNamespace, "ERROR:Repo$load_arrayops_from_config:config$namespace must be a single string")
      assert(is.list(arraySection), "ERROR:Repo$load_arrayops_from_config:config$arrays must be a list.")
      
      if(!.has_len(arraySection)) return(list())
      
      read_array = function(arrayConfigObj) {
        assert_no_fields(c('alias', 'name') %-% names(arrayConfigObj),
                           "ERROR:Repo$load_arrayops_from_config:bad config format: missing name(s) '%s'.\n%%s", 
                         deparse(arrayConfigObj))
        alias = arrayConfigObj[['alias']]
        name = arrayConfigObj[['name']]
        schema = arrayConfigObj[['schema']]
        assert_single_str(alias, "ERROR:Repo$load_arrayops_from_config:bad config format: 'alias' must be a single string")
        assert_single_str(name, "ERROR:Repo$load_arrayops_from_config:bad config format: 'name' must be a single string")
        isFullName = grepl('\\.', name)
        fullArrayName = if(isFullName) name else sprintf("%s.%s", defaultNamespace, name)
        if(.has_len(schema)){
          assert_single_str(schema, "ERROR:Repo$load_arrayops_from_config:bad config format: 'schema' must be a single string")
          parsedArray = try(get_array(sprintf("%s %s", fullArrayName, schema)), silent = TRUE)
          assert(inherits(parsedArray, 'ArrayOpBase'), "ERROR:Repo$laod_arrays_from_config:bad config format:invalid array schema: %s", schema)
        } else parsedArray = fullArrayName
        parsedArray
      }
      
      arrayInstances = lapply(arraySection, read_array)
      return(rlang::set_names(arrayInstances, aliases))
    }
    ,
    #' @description
    #' Upload a data frame to scidb
    #'
    #' Upload a data frame into scidb with data types from the template.
    #' The data frame can have fewer columns than the template, but it can NOT have columns absent in the template.
    #'
    #' @param df a data frame whose column names has been sanitized
    #' @param template An array alias, schema string, arrayOp; or a named list of data types
    #' @param temp Whether to upload the df as a temporary array. Default TRUE.
    #' @param use_aio_input Whether to use aio_input for upload. Default FALSE. Same meaning as in scidb::as.scidb
    #' @param ... Other args passed to scidb::as.scidb, e.g. gc = TRUE
    #' @return An arrayOp instance with uploaded data.
    #' The resultant arrayOp$to_afl() is the temporary array name R_array****, unless specified explicitly with name='newName'
    #' The resultant arrayOp schema is consisted of 1. artificial dimension(s) from upload or aio_input;
    #' 2. attributes whose names are the data frame column names and data types are those in the template.
    upload_df = function(df, template, temp = TRUE, use_aio_input = setting_use_aio_input, ...) {
      
      array_template = if(is.list(template))
        self$ArrayOp('', attrs = names(template), dtypes = template)
      else get_array(template)
      
      dfFieldsNotInArray = names(df) %-% array_template$dims_n_attrs
      matchedFields = names(df) %n% array_template$dims_n_attrs
      assert_not_has_len(
        dfFieldsNotInArray,
        "ERROR: Data frame has non-matching field(s): %s for array %s", paste(dfFieldsNotInArray, collapse = ','),
        str(array_template))
      uploaded = scidb::as.scidb(dep$.db, df, use_aio_input = use_aio_input, temp = temp, 
                                 types =  array_template$get_field_types(names(df), .raw=TRUE), ...)
      
      res = get_array(uploaded@meta$schema)$
        create_new_with_same_schema(uploaded@name)
      res$.set_meta('.ref', uploaded)
      res
    }
    ,
    #' Get an arrayOp instance from 'build' or upload depending on the threshold
    #'
    #' @param df: a dataframe that has template's fields as columns
    #' @param template: an arrayOp instance as a template in `build` or `upload` mode
    #' @param threshold: use `build` if number of df cells <= threshold; otherwise `upload`.
    #' Default: `repo$setting_build_or_upload_threshold`
    #' @param build_dim: used as `artificial_field` in 'build' mode
    #' @param ...: arguments used in `repo$upload`
    #'
    #' @return an arrayOp instance
    build_or_upload_df = function(
      df, 
      template,
      threshold = setting_build_or_upload_threshold,
      build_dim = 'z',
      ...
    ){
      cells = nrow(df) * length(names(df))
      if(cells <= threshold){
        template$build_new(df, artificial_field = build_dim)
      } else {
        upload_df(df, template = template, ...)
      }
    }
    ,
    #' @description 
    #' Save an arrayOp or AFL statement as a scidb array wrapped as a new arrayOp
    #' 
    #' No all things can be done in one scidb operation, sometimes we need to store intermediate results as arrays.
    #' This is when we need to save materialized arrayOp into a new array, which is then wrapped in a new arrayOp
    #' instance. To get the new array's raw name, call the result `arrayOp$to_afl()` function.
    #' 
    #' The ... params are passed in to scidb::store. 
    #' We can specify an array name `name='myArray'`(default:a random name in public namespace), create a temporary
    #' array `temp=T`(default:F), or suppress auto gc `gc=F`(default:T). Refer to scidb documentation for more details.
    #' 
    #' @param what an arrayOp or an AFL statement (string)
    #' @param ... Same params as used in scidb::store function, e.g. temp = F, gc = T, name = 'myArray'
    #' @return An arrayOp whose `to_afl()` is the newly created array name
    save_as_array = function(what, ...) {
      aflStmt = get_afl_statement(what)
      storedAfl = scidb::scidb(dep$.db, aflStmt)
      storedArray = scidb::store(dep$.db, storedAfl, ...)
      
      res = get_array(storedArray@meta$schema)$
        create_new_with_same_schema(storedArray@name)
      res$.set_meta('.ref', storedArray)
      res
    }
    ,
    #' Get an arrayOp instance by matching a data frame `df` against an arrayOp `reference`
    #'
    #' Whether `filter` or `index_lookup` mode is determined by the `setting_match_filter_mode_threshold`.
    #' Use `filter` mode if number of cells in `df` < `setting_match_filter_mode_threshold`
    #'
    #' @param ... params for `cross_between` mode
    #'
    #' @return an arrayOp instance with the same schema as `reference`
    auto_match = function(df, reference, filter_threshold = setting_auto_match_filter_mode_threshold, ...) {
      assert_no_fields(names(df) %-% reference$dims_n_attrs,
                       "ERROR: Repo$auto_match: param df has unmatched fields to the reference: '%s'")
      assert(inherits(reference, 'ArrayOpBase'), 
             "ERROR: Repo$auto_match: param 'reference' must be an ArrayOp instance, but got: [%s]", 
             paste(class(reference), collapse = ","))
      
      cells = base::nrow(df) * length(names(df))
      if(cells <= filter_threshold){
        reference$match(df, op_mode = "filter")
      } else {
        dfOp = build_or_upload_df(df, reference)
        op_mode = if(length(names(df)) == 1) "index_lookup" else "cross_between"
        reference$match(df, op_mode = op_mode, ...)
      }
    }
    ,
    # Convenience functions ----
    #' @description 
    #' Create a new array using the operand's schema
    #' @param array_or_alias The array operand as a template, either an arrayOp isntance or a registered array alias
    #' @param new_array_name The new array name. If NULL, it uses the operand's `to_afl()` result as the new name.
    .create_array = function(array_or_alias, new_array_name = NULL) {
      operand = get_array(array_or_alias)
      if(is.null(new_array_name)) new_array_name = operand$to_afl()
      assert_single_str(new_array_name, 
                        "ERROR:Repo$.create_array:param 'new_array_name' must be a single string, but got: [%s]",
                        paste(class(new_array_name), collapse = ','))
      execute_raw(operand$create_array_cmd(new_array_name))
    }
    ,
    #' @description 
    #' Remove array versions
    #' @param array_or_alias The array operand as a template, either an arrayOp isntance or a registered array alias
    .remove_array_versions = function(array_or_alias, ...) {
      execute_raw(
        get_array(array_or_alias)$remove_array_versions_cmd(...)
      )
    }
    ,
    #' @description 
    #' Remove array. Careful: Cannot be undone!!!
    #' @param array_or_alias The array operand as a template, either an arrayOp isntance or a registered array alias
    .remove_array = function(array_or_alias) {
      execute_raw(
        get_array(array_or_alias)$remove_array_cmd()
      )
    }
    ,
    #' @description 
    #' Get the number of rows of an arrayOp
    #' @param what An arrayOp, raw AFL or R expression string
    #' @param .raw If TRUE, and `what` is string, treat it as raw AFL. Otherwise parse `what` as R expression
    #' @param .env The env where `what` is evaluated as R expression. 
    #' Only applicable when `what` is R expression string and `.raw = F`
    nrow = function(what, .raw = TRUE, .env = parent.frame()) {
      op = get_operand(what, .raw = .raw, .env = .env)
      res = query_raw(arrayop::afl(op | op_count))
      res$count
    }
    #' @description 
    #' Get the first `count` rows of an array
    #' @param what An arrayOp, raw AFL or R expression string
    #' @param count How many rows to take
    #' @param offset How many rows to skip before taking
    #' @param ... Arguments passed directly to scidb::iquery function
    #' @param .raw If TRUE, and `what` is string, treat it as raw AFL. Otherwise parse `what` as R expression
    #' @param .env The env where `what` is evaluated as R expression. 
    #' Only applicable when `what` is R expression string and `.raw = F`
    ,
    limit = function(what, count, offset = NULL, ..., .raw = TRUE, .env = parent.frame()) {
      op = get_operand(what, .raw = .raw, .env = .env)
      res = query_raw(arrayop::afl(op | limit(count, offset)), ...)
      res
    }
  )
)

