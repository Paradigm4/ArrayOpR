

#' Repo class which manages ArraySchemas that reside in one scidb installation
#' @description 
#' A repository for ArraySchemas that reside in one scidb installation
#' @note Repo class depends on a package-level dependency object `DEP`
Repo <- R6::R6Class("Repo",
  
  private = list(
    namespace = NULL,
    dbAccess = NULL,
    schema_registry = NULL,
    cached_schemas = NULL
  ),
  active = NULL,
  
  public = list(
    #' Initialize function
    #'
    #' Create a new Repo instance
    #' @param namespace The default namespace
    #' @param dbAccess A DbAccess instantance that manages scidb connection
    initialize = function(namespace, dbAccess = NULL) {
      self$namespace = namespace
      self$dbAccess = dbAccess
      self$cached_schemas = list()
      self$schema_registry = list()
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
        df <- dbAccess$run_afl(afl, ...)
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
    #' Load an array schema from scidb.
    #' NOTE: No caching involved, always load from scidb
    #'
    #' @param array_name Array name without namespace
    #' @param namespace Scidb namespace, which defaults to the Repo's namespace
    load_schema = function(array_name, ns = self$namespace) {
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
        afl = .afl(array$to_afl() %remove_versions% version_id)
      } else if (methods::hasArg('keep')) {
        afl = .afl(array$to_afl() %remove_versions% sprintf("keep: %s", keep))
      } else {
        afl = .afl(array$to_afl() %remove_versions% NULL)
      }
      run_only(afl)
    }
  )
)





#' Return a registered array schema
#'
#' Load on first access, then use the cached entry.
`[[.Repo` <- function(x, schema_alias, force_reload = FALSE) {
  item = x$cached_schemas[[schema_alias]]
  if (is.null(item) || force_reload) {
    arrayName = x$schema_registry[[schema_alias]]
    assert(length(arrayName) == 1,
      "Schema '%s' not found in Repo",
      schema_alias)
    item = x$load_schema(arrayName)
    x$cached_schemas[[schema_alias]] <- item
  }
  return(item)
}


#' R6 Class Representing a Person
#'
#' @description
#' A person has a name and a hair color.
#'
#' @details
#' A person can also greet you.
Person <- R6::R6Class("Person",
  public = list(
    #' @field name First or full name of the person.
    name = NULL,
    #' @field hair Hair color of the person.
    hair = NULL,
    #' @description
    #' Create a new person object.
    #' @param name Name.
    #' @param hair Hair color.
    #' @return A new `Person` object.
    initialize = function(name = NA, hair = NA) {
      self$name <- name
      self$hair <- hair
      self$greet()
    },
    #' @description
    #' Change hair color.
    #' @param val New hair color.
    #' @examples
    #' P <- Person("Ann", "black")
    #' P$hair
    #' P$set_hair("red")
    #' P$hair
    set_hair = function(val) {
      self$hair <- val
    },
    #' @description
    #' Say hi.
    greet = function() {
      cat(paste0("Hello, my name is ", self$name, ".\n"))
    }
  )
)
