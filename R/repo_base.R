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
    default_namespace = NULL,
    dep = NULL,
    schema_registry = NULL,
    meta = NULL
  ),
  active = list(
    scidb_version = function() private$meta$scidb_version
  ),
  
  public = list(
    cached_schemas = NULL
    ,
    #' @description
    #' Initialize function
    #'
    #' Create a new Repo instance
    #' @param namespace The default namespace
    #' @param dbAccess A DbAccess instantance that manages scidb connection
    initialize = function(default_namespace, dependency_object = NULL) {
      self$cached_schemas = list()
      private$default_namespace = default_namespace
      private$dep = dependency_object
      private$schema_registry = list()
      
      private$meta = list(scidb_version = dependency_object$get_version())
    }
    ,
    query = function(afl, ...) {
      private$dep$query(afl, ...)
    }
    ,
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
  RepoBase$new(default_namespace, dependency_obj)
}


