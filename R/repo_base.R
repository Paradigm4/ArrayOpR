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
    dep = NULL,
    meta = NULL
    ,
    set_meta = function(key, value) {
      private$meta[[key]] <- value
    }
    ,
    get_meta = function(key) {
      private$meta[[key]]
    }
  ),
  active = list(
    meta_info = function() private$meta
  ),
  
  public = list(
    cached_schemas = NULL
    ,
    #' @description
    #' Initialize function.
    #'
    #' Create a new Repo instance
    #' 
    #' Do NOT call this `initialize` function directly. Call `newRepo` instead to get a new Repo instance.
    #' @param namespace The default namespace
    #' @param dbAccess A DbAccess instantance that manages scidb connection
    initialize = function(default_namespace, dependency_object = NULL) {
      self$cached_schemas = list()
      private$meta = list()
      
      private$dep = dependency_object
      
      private$set_meta('default_namespace', default_namespace)
      private$set_meta('schema_registry', list())
      private$set_meta('repo_version', "RepoBase")
      private$set_meta('scidb_version', dependency_object$get_version())
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
  )
)


#' @export
newRepo = function(default_namespace = 'ns', dependency_obj){
  # Validate dependency_obj has all required methods
  requiredNames = c('get_version', 'query', 'execute')
  missingNames = base::setdiff(requiredNames, names(dependency_obj))
  assert_not_has_len(missingNames, 
    "newRepo 'dependency_obj' param is missing function(s): '%s'", paste(missingNames, collapse = ', '))
  
  # Check scidb version
  scidbVersion = dependency_obj$get_version()
  # Use major version for now
  scidbVersion = gsub("\\..+", '', scidbVersion)
  
  repoClass = switch (scidbVersion,
    '18' = RepoV18,
    '19' = RepoV19
  )
  repoClass$new(default_namespace, dependency_obj)
}


