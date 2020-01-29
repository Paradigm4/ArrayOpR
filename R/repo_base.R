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
newRepo = function(default_namespace = 'public', dependency_obj = NULL, db = NULL, config = NULL){
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
    '18' = RepoV18,
    '19' = RepoV19
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

#' Invert names and values of a list (or named vector)
#'
#' @param obj a named list
#' @param .as.list Return a list if set TRUE; otherwise a named vector
#' @return A list or named vector whose key/values are inverted from the original `obj`
invert.list = function(obj, .as.list = T) {
  res = structure(
    unlist(mapply(rep, names(obj), sapply(obj, length)), use.names=F),
    names = unlist(obj)
  )
  if(.as.list) res = as.list(res)
  res
}

