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
    #' @param afl Scidb AFL statement
    #' @param ... Extra settings passed to `dep$query` function
    query = function(afl, ...) {
      private$dep$query(afl, ...)
    }
    ,
    #' @description 
    #' Run a command and do not expect results
    #' 
    #' Delegate to Repo's `dep$execute` function
    #' @param afl Scidb AFL statement
    #' @param ... Extra settings passed to `dep$execute` function
    execute = function(afl, ...) {
      private$dep$execute(afl, ...)
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
      arrayOp = self$ArrayOp(full_array_name, attrs$name, dims$name, dtypes = dtypes)
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
#' @export
newRepo = function(default_namespace = 'ns', dependency_obj){
  # Validate dependency_obj has all required methods
  requiredNames = c('get_scidb_version', 'query', 'execute', 'get_schema_df')
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


