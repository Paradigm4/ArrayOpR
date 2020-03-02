#' RepoBase class manages ArrayOp objects in one scidb installation
#' @description 
#' A repository for scidb arrays and derived ArrayOp objects that reside in one scidb installation
#' @details 
#' RepoBase is the base class of scidb-version-specific Repo classes. 
#' Common ogic/behaviors across all supported scidb versions are placed here. 
#' Version-specific logic/behaviors should be implemented in RepoBase sub-classes. 
#' @export
RepoBase <- R6::R6Class("RepoBase",
  private = list(
    dep = NULL
    ,
    cached_schemas = NULL
    ,
    metaList = NULL
    ,
    set_meta = function(key, value) {
      private$metaList[[key]] <- value
    }
    ,
    get_meta = function(key) {
      private$metaList[[key]]
    }
    ,
    new_arrayop_from_list = function(full_array_name, params){
      self$ArrayOp(full_array_name, params$dims, params$attrs, dtypes = params$dtypes, dim_specs = params$dim_specs)
    }
    ,
    new_arrayop_from_schema_df = function(full_array_name, schema_df) {
      schemaDf = schema_df
      mandatoryCols = c('name', 'dtype', 'is_dimension')
      assert(all(mandatoryCols %in% names(schemaDf)),
        "ERROR: Repo$load_schema_from_db: schema data frame should have all the columns: '%s', but got '%s'",
        mandatoryCols, names(schemaDf)
      )
      attrs = schemaDf[!schemaDf$is_dimension, ]
      dims = schemaDf[schemaDf$is_dimension, ]
      dtypes = as.list(structure(schemaDf$dtype, names = schemaDf$name))
      
      dimSpecVector = if(is.null(dims$dim_spec)) list() else dims$dim_spec
      dimSpecs = as.list(structure(dimSpecVector, names = dims$name))
      
      params = list(dims = dims$name, attrs = attrs$name, dtypes = dtypes, dim_specs = dimSpecs)
      return(private$new_arrayop_from_list(full_array_name, params))
    }
    ,
    # If both schema and attrs/dims are provided, only 'schema' takes precedence
    # Here we assume that all dimensions have 'int64' data type
    load_array_config = function(alias, name, schema, attrs, dims) {
      self$register_schema_alias_by_array_name(alias, name, is_full_name = grepl('\\.', name))
      fullArrayName = private$get_meta('schema_registry')[[alias]]
      
      arrayOp = 
      if(methods::hasArg(schema)) {
        assert_has_len(schema, "ERROR: Repo$load_array_config: schema string must be provided when attrs/dims missing")
        schemaDf = get_schema_df_from_schema_str(schema)
        private$new_arrayop_from_schema_df(fullArrayName, schemaDf)
      }
      else {
        assert(methods::hasArg(attrs) && methods::hasArg(dims),
          "ERROR: Repo$load_array_config: Array [%s]: No schema string or dims/attrs config are provided ", alias)
        private$new_arrayop_from_list(fullArrayName, list(
          dims = names(dims), attrs = names(attrs), 
          dtypes = c(attrs, invert.list(list('int64' = names(dims)))), dim_specs = dims
        ))
      }
      
      private$cached_schemas[[alias]] = arrayOp
    }
  )
  ,
  active = list(
    meta = function() private$metaList
  )
  ,
  public = list(
    #' @description
    #' Initialize function.
    #'
    #' Create a new Repo instance
    #' 
    #' Do NOT call this `initialize` function directly. Call `newRepo` instead to get a new Repo instance.
    #' @param namespace The default namespace
    #' @param dbAccess A DbAccess instantance that manages scidb connection
    initialize = function(default_namespace, dependency_object = NULL, config = NULL) {
      private$cached_schemas = list()
      private$metaList = list()
      
      private$dep = dependency_object
      
      private$set_meta('default_namespace', default_namespace)
      private$set_meta('schema_registry', list())
      private$set_meta('repo_version', "RepoBase")
      private$set_meta('scidb_version', dependency_object$get_scidb_version())
      
      # Reading settings from a config object if available
      if (!is.null(config)) {
        if (default_namespace == 'public')
          private$set_meta('default_namespace', config$namespace)

        # load settings
        # load arrays
        for(arr in config$arrays){
          do.call(private$load_array_config, arr)
        }
      }
    }
    ,
    #' @description 
    #' Run a query and expect results
    #' 
    #' Delegate to Repo's `dep$query` function
    #' Get a data frame from any ArrayOp instance or raw AFL
    #' 
    #' **NOTE** if `only_attributes = T`, then `op$to_afl(drop_dims = T)` which may differ from the default `op$to_afl()`
    #' @param what An ArrayOp instance or raw AFL 
    #' @param ... Extra settings passed to `dep$query` function
    query = function(what, ...) {
      assert(inherits(what, c('ArrayOpBase', 'character')),
             "Repo$to_df 'what' argument must be a character or ArrayOp")
      # If ... contains only_attributes = TRUE, then op's dimensions are effectively dropped.
      drop_dims = methods::hasArg('only_attributes') &&
        list(...)[['only_attributes']]
      afl = if (inherits(what, 'ArrayOpBase'))
        what$to_df_afl(drop_dims)
      else
        what
      
      tryCatch({
        df <- private$dep$query(afl, ...)
      },
      error = function(e) {
        # Print out AFL only when error occurs.
        print(afl)
        stop(e)
      })
      return(df)
      # private$dep$query(afl, ...)
    }
    ,
    #' @description 
    #' Run a command and do not expect results
    #' 
    #' Delegate to Repo's `dep$execute` function
    #' @param what An ArrayOp instance or raw AFL 
    #' @param ... Extra settings passed to `dep$execute` function
    execute = function(what, ...) {
      assert(inherits(what, c('ArrayOpBase', 'character')),
             "Repo$execute 'what' argument must be a character or ArrayOp")
      afl = if (inherits(what, 'ArrayOpBase'))
        what$to_afl()
      else
        what
      tryCatch({
        return(private$dep$execute(afl, ...))
      },
      error = function(e) {
        # Print out AFL only when error occurs.
        print(afl)
        stop(e)
      })
      # private$dep$execute(afl, ...)
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance with the proper scidb version
    #' 
    #' Should be overriden by sub-classes
    ArrayOp = function(...) 
      stop("RepoBase$ArrayOp function should be overriden by sub-classes. Cannot be called directly in the base class.")

    # Schema management -----------------------------------------------------------------------------------------------
    ,
    #' @description 
    #' Register a scidb array as an alias in the Repo
    #' 
    #' If array_name is not fully qualified, the current default_namespace meta info is taken.
    #' Later changes to the default_namespace will not affect already registered arrays
    register_schema_alias_by_array_name = function(alias, array_name, is_full_name = FALSE) {
      if(!is_full_name){
        array_name = sprintf("%s.%s", private$get_meta('default_namespace'), array_name)
      }
      newList = structure(array_name, names = alias)
      newRegistry = c(private$get_meta('schema_registry'), newList)
      private$set_meta('schema_registry', newRegistry)
    }
    ,
    get_alias_schema = function(alias) {
      val = private$cached_schemas[[alias]]
      if(is.null(val)){
        fullArrayName = private$get_meta('schema_registry')[[alias]]
        assert_has_len(fullArrayName, "ERROR: Repo$get_alias_schema: '%s' is not registered.", alias)
        val = self$load_schema_from_db(fullArrayName)
        private$cached_schemas[[alias]] <- val
      }
      return(val)
    }
    ,
    #' @description
    #' Load a new ArrayOp directly from scidb
    #' 
    #' The repo's `dep$get_schema_df` function is called to get array schema denoted by a data frame
    load_schema_from_db = function(full_array_name) {
      schemaDf = private$dep$get_schema_df(full_array_name)
      private$new_arrayop_from_schema_df(full_array_name, schemaDf)
    }
    ,
    #' @description 
    #' Set Repo meta data directly
    .set_meta = function(key, value) private$set_meta(key, value)
    ,
    #' @description 
    #' Get Repo meta data directly
    .get_meta = function(key) private$get_meta(key)
  )
)


#' Create a scidb-version-specific Repo instance 
#' 
#' The created Repo instance is automatically selected based on Repo-Scidb compatibility
#' 
#' This should be only way to create a new Repo instance, 
#' unless a specific Repo version is known/desired beforehand, which is useful in test cases.
#' @param dependency_obj A named list that contains 4 functions: get_scidb_version, query, execute and get_schema_df
#' If left NULL, will require a scidb connection 'db' to create the dependency_obj automatically.
#' @param db a scidb connection returned by scidb::scidbconnect
#' @export
newRepoBase = function(default_namespace = 'public', dependency_obj = NULL, db = NULL, config = NULL){
  # Validate dependency_obj has all required methods
  requiredNames = c('get_scidb_version', 'query', 'execute', 'get_schema_df')
  if(!.has_len(dependency_obj) && .has_len(db)){
    dependency_obj = default_dep_obj(db)
  }
  missingNames = base::setdiff(requiredNames, names(dependency_obj))
  assert_not_has_len(missingNames, 
    "newRepo 'dependency_obj' param is missing function(s): '%s'", paste(missingNames, collapse = ', '))
  
  # Check scidb version
  fullScidbVersion = dependency_obj$get_scidb_version()
  # Use major version for now
  scidbVersion = gsub("\\..+", '', fullScidbVersion)
  
  repoClass = switch (scidbVersion,
    '18' = RepoV18Old,
    '19' = RepoV19Old
  )
  assert(!is.null(repoClass), "ERROR in newRepo: unsupported scidb version %s", fullScidbVersion)
  result = repoClass$new(default_namespace, dependency_obj, config = config)
  return(result)
}

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
    # Should return a data.frame with columns: name, dtype, is_dimension
    get_schema_df = function(array_name) {
      # V3<vid:int64,ref:string,alt:string,extra:string> [chrom=1:24:0:1; pos=1:*:0:1000000; alt_id=0:*:0:1000]
      schemaStr = runQuery(sprintf("show(%s)", array_name))[1, 'schema']
      get_schema_df_from_schema_str(schemaStr)
    }
  )
}

get_schema_df_from_schema_str = function(schemaStr) {
  matched = stringr::str_match_all(schemaStr, "\\<(.+)\\>\\s*\\[(.+)\\]")[[1]]
  assert(nrow(matched) == 1, "ERROR: Repo$get_schema_df_from_schema_str: Invalid schema string: %s", schemaStr)
  attrStr = matched[1,2]
  dimStr = matched[1,3]
  attrMatrix = stringr::str_match_all(attrStr, "(\\w+):\\s*([^;,:]+)")[[1]]
  dimMatrix = stringr::str_match_all(dimStr, "(\\w+)[=\\s]*([^;]*)")[[1]]
  numAttrs = nrow(attrMatrix)
  numDims = nrow(dimMatrix)
  data.frame(
    name = c(attrMatrix[, 2], dimMatrix[, 2]),
    dtype = c(attrMatrix[, 3], rep('int64', numDims)),
    is_dimension = c(rep(F, numAttrs), rep(T, numDims)),
    dim_spec = c(rep('', numAttrs), dimMatrix[, 3]),
    stringsAsFactors = FALSE
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
    ignore_error = function(){
      get_meta('ignore_error', FALSE)
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
    execute_raw = function(what, ...) {
      try(dep$execute(what, ...), silent = ignore_error())
    }
    ,
    query_raw = function(what, ...) {
      try(dep$query(what, ...), silent = ignore_error())
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
  active = list(
    #' @field .private For internal testing only. Do not access this field to avoid unintended consequences!!!
    .private = function() private
  )
  ,
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
          aliasArray = array_alias_registry[[what]]
          assert_has_len(aliasArray, "ERROR:Repo$get_array: '%s' is not a registered array alias.", what)
          if(is.character(aliasArray)){
            cached = load_array_from_scidb(aliasArray)
            private$array_alias_registry[[what]] <- cached
            return(cached)
          }
          return(aliasArray)
        }
      } else if(inherits(what, "ArrayOpBase")){
        return(what)
      }
      stopf("ERROR:Repo$get_array: param 'what' is not an array alias, schema string or an ArrayOp instance. class(what)=%s",
            paste(class(what), collapse = ','))
    }
    ,
    #' Register a list of arrays with aliases
    #'  
    #' Register a list of arrays which can be later accessed via their aliases
    #' 
    #' @param items A list where names are aliases and values are arrayOp instances or raw array names
    register_array = function(items){
      assert_named_list(items, "ERROR:Repo$register_array: param 'items' must be a named list where every element has a name, and the value is an ArrayOp instance or raw array name.")
      private$array_alias_registry = utils::modifyList(private$array_alias_registry, items)
    }
    ,
    #' Get an ArrayOp instance from scidb by its full name 
    #' 
    #' No cache is provided with this function. 
    #' See `register_array` for details on registering an array alias with cache.
    #' 
    #' @param full_array_name A fully qualified array name (e.g. myNamespace.arrayName)
    #' @return An ArrayOp instance
    load_array_from_scidb = function(full_array_name){
      assert(is.character(full_array_name) && length(full_array_name) == 1,
             "ERROR:Repo$load_array_from_scidb: param 'full_array_name' must be a single scidb array name")
      schemaStr = query_raw(afl(full_array_name %project% 'schema'))[['schema']]
      assert(is.character(schemaStr),
             "ERROR:Repo$load_array_from_scidb: '%s' is not a valid scidb array", full_array_name)
      get_array(schemaStr)
    }
  )
)

