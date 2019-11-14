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
    initialize = function(default_namespace, dependency_object = NULL) {
      private$cached_schemas = list()
      private$metaList = list()
      
      private$dep = dependency_object
      
      private$set_meta('default_namespace', default_namespace)
      private$set_meta('schema_registry', list())
      private$set_meta('repo_version', "RepoBase")
      private$set_meta('scidb_version', dependency_object$get_scidb_version())
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
    #' Load Array Schema directly from scidb
    load_schema_from_db = function(full_array_name) {
      schemaDf = private$dep$get_schema_df(full_array_name)
      mandatoryCols = c('name', 'dtype', 'is_dimension')
      assert(all(mandatoryCols %in% names(schemaDf)),
        "ERROR: Repo$load_schema_from_db: schema data frame should have all the columns: '%s', but got '%s'",
        mandatoryCols, names(schemaDf)
      )
      attrs = schemaDf[!schemaDf$is_dimension, ]
      dims = schemaDf[schemaDf$is_dimension, ]
      dtypes = as.list(structure(schemaDf$dtype, names = schemaDf$name))
      arrayOp = self$ArrayOp(full_array_name, dims$name, attrs$name, dtypes = dtypes)
      return(arrayOp)
    }
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
newRepo = function(default_namespace = 'ns', dependency_obj = NULL, db){
  # Validate dependency_obj has all required methods
  requiredNames = c('get_scidb_version', 'query', 'execute', 'get_schema_df')
  if(!.has_len(dependency_obj) && methods::hasArg('db')){
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
  repoClass$new(default_namespace, dependency_obj)
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
      # Get dimensions
      dimDf = runQuery(sprintf("project(dimensions(%s), name, type)", array_name))
      # Get attributes
      attrDf = runQuery(sprintf("project(attributes(%s), name, type_id)", array_name))
      data.frame(
        name = c(dimDf$name, attrDf$name),
        dtype = c(dimDf$type, attrDf$type_id),
        is_dimension = c(rep(T, nrow(dimDf)), rep(F, nrow(attrDf))),
        stringsAsFactors = FALSE
      )
    }
  )
}


