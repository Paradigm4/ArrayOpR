#' Repo class which manages ArraySchemas that reside in one scidb installation
#' @description 
#' A repository for ArraySchemas that reside in one scidb installation
#' @details 
#' Repo class depends on a package-level dependency object 'DEP'
#' @export
Repo <- R6::R6Class("Repo",
  private = list(
    namespace = NULL,
    dbAccess = NULL,
    schema_registry = NULL,
    cached_schemas = NULL
  ),
  active = NULL,
  
  public = list(
    #' @description
    #' Initialize function
    #'
    #' Create a new Repo instance
    #' @param namespace The default namespace
    #' @param dbAccess A DbAccess instantance that manages scidb connection
    initialize = function(namespace, dbAccess = NULL) {
      private$namespace = namespace
      private$dbAccess = dbAccess
      private$cached_schemas = list()
      private$schema_registry = list()
    }
    ,
    #' @description 
    #' Get a data frame from any ArrayOp instance or raw AFL
    #' 
    #' **NOTE** if `only_attributes = T`, then `op$to_afl(drop_dims = T)` which may differ from the default `op$to_afl()`
    #' @param what An ArrayOp instance or raw AFL 
    #' @return a data.frame
    to_df = function(what, ...) {
      assert(inherits(what, c('ArrayOpBase', 'character')),
        "Repo$to_df 'what' argument must be a character or ArrayOp")
      # If ... contains only_attributes = TRUE, then op's dimensions are effectively dropped.
      drop_dims = methods::hasArg('only_attributes') &&
        list(...)[['only_attributes']]
      afl = if (inherits(what, 'ArrayOpBase'))
        what$to_afl(drop_dims)
      else
        what
      
      tryCatch({
        df <- dbAccess$load_df_from_afl(afl, ...)
      },
        error = function(e) {
          # Print out AFL only when error occurs.
          print(afl)
          stop(e)
        })
      return(df)
    }
    ,
    #' @description 
    #' Run scidb statements without expecting results
    #' 
    #' @param what An ArrayOp instance or raw AFL 
    #' If `what` is an `ArrayOp` instance, `what$to_afl()` will be used as the raw AFL 
    run_only = function(what, ...) {
      assert(inherits(what, c('ArrayOpBase', 'character')),
        "Repo$run_only 'what' argument must be a character or ArrayOp")
      afl = if (inherits(what, 'ArrayOpBase'))
        what$to_afl()
      else
        what
      tryCatch({
        df <- private$dbAccess$run_afl(afl, ...)
      },
        error = function(e) {
          # Print out AFL only when error occurs.
          print(afl)
          stop(e)
        })
    }
    ,
    #' @description 
    #' Register an array schema into the Repo
    #'
    #' @param alias a user friendly name for a scidb array
    #' @param array_name the actual array name in scidb
    register_schema = function(alias, array_name) {
      private$schema_registry[[alias]] <- array_name
    }
    ,
    #' @description 
    #' Load schema from Repo's cache
    #' @param schema_alias Alias of the array, not actual array name
    #' @return Cached ArraySchema or NULL if not found
    load_schema_from_cache = function(schema_alias){
      private$schema_registry[[schema_alias]]
    }
    ,
    #' @description 
    #' Load an array schema from scidb.
    #' NOTE: No caching involved, always load from scidb
    #'
    #' @param array_name Array name without namespace. Not array alias.
    #' @param namespace Scidb namespace, which defaults to the Repo's namespace
    load_schema_from_db = function(array_name, ns = self$namespace) {
      fullName <- sprintf("%s.%s", ns, array_name)
      attributes = dbAccess$load_schema_attrs(fullName)
      dimensions = dbAccess$load_schema_dimensions(fullName)
      fieldTypes = structure(as.list(c(dimensions$type, attributes$type)),
        names = c(dimensions$name, attributes$name))
      schema = ArraySchema(
        array_name,
        attributes$name,
        dimensions$name,
        namespace = ns,
        info = structure(list(fieldTypes), names = .DT)
      )
      return(schema)
    }
    ,
    #' @description 
    #' Remove versions of array snapshots
    #' If only `alias` is specified, remove all but the most recent version.
    #'
    #' @param alias a user friendly name for a scidb array
    #' @param version_id remove all versions up the `version_id`
    #' @param keep remove all but the last `keep` versions
    remove_array_versions = function(alias, version_id, keep) {
      assert(
        !(methods::hasArg('version_id') &&
            methods::hasArg('keep')),
        "Cannot specify version_id and keep at the same time!"
      )
      array = self[[alias]]
      if (methods::hasArg('version_id')) {
        afl = afl(array %remove_versions% version_id)
      } else if (methods::hasArg('keep')) {
        afl = afl(array %remove_versions% sprintf("keep: %s", keep))
      } else {
        afl = afl(array %remove_versions% NULL)
      }
      run_only(afl)
    }
  )
)

#' Return a registered array schema
#'
#' Load on first access, then use the cached entry.
#' @export
`[[.Repo` <- function(x, schema_alias, force_reload = FALSE) {
  item = x$cached_schemas[[schema_alias]]
  if (is.null(item) || force_reload) {
    arrayName = x$load_schema_from_cache(schema_alias)
    assert(length(arrayName) == 1,
      "Schema '%s' not found in Repo",
      schema_alias)
    item = x$load_schema_from_db(arrayName)
    x$cached_schemas[[schema_alias]] <- item
  }
  return(item)
}


