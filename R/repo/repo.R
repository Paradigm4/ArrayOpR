Repo <- setRefClass("Repo",

  fields = c('namespace', 'dbAccess', 'schema_registry', 'cached_schemas'),

  methods = list(
    initialize = function(namespace, dbAccess = NULL, reset_schema_namespace = TRUE) {
      callSuper(namespace = namespace, dbAccess = dbAccess,
                schema_registry = list(), cached_schemas = list())
    }
  )
)


# Execute AFL -----------------------------------------------------------------------------------------------------

Repo$methods(

  to_df = function(what, ...){
    #' Get a data frame from any ArrayOp instance
    #' @note if `only_attributes = T`, then `op$to_afl(drop_dims = T)` which may differ from the default `op$to_afl()`
    #' @param what An ArrayOp instance or raw AFL expression

    assert(inherits(what, c('ArrayOpBase', 'character')), "Repo$to_df 'what' argument must be a character or ArrayOp")
    # If ... contains only_attributes = TRUE, then op's dimensions are effectively dropped.
    drop_dims = methods::hasArg('only_attributes') && list(...)[['only_attributes']]
    afl = if(inherits(what, 'ArrayOpBase')) what$to_afl(drop_dims) else what

    tryCatch({
        df <- dbAccess$load_df_from_afl(afl, ...)
      },
      error = function(e){
        # Print out AFL only when error occurs.
        print(afl)
        stop(e)
      }
    )
    return(df)
  }
  , run_only = function(what, ...) {
    assert(inherits(what, c('ArrayOpBase', 'character')), "Repo$run_only 'what' argument must be a character or ArrayOp")
    afl = if(inherits(what, 'ArrayOpBase')) what$to_afl() else what
    tryCatch({
      df <- dbAccess$run_afl(afl, ...)
    },
    error = function(e){
      # Print out AFL only when error occurs.
      print(afl)
      stop(e)
    }
    )
  }
)


# Array schema loading and caching  -------------------------------------------------------------------------------

Repo$methods(
  register_schema = function(alias, array_name) {
    #' Register an array schema into the Repo
    #'
    #' @param alias a user friendly name for a scidb array
    #' @param array_name the actual array name in scidb

    .self$schema_registry[[alias]] <- array_name
  }

  , load_schema = function(array_name, ns = .self$namespace) {
    #' Load an array schema from scidb.
    #' @note No caching involved, always load from scidb
    #'
    #' @param array_name Array name without namespace
    #' @param namespace Scidb namespace, which defaults to the Repo's namespace

    fullName <- sprintf("%s.%s", ns, array_name)
    attributes = dbAccess$load_schema_attrs(fullName)
    dimensions = dbAccess$load_schema_dimensions(fullName)
    fieldTypes = structure(as.list(c(dimensions$type, attributes$type)),
                           names = c(dimensions$name, attributes$name))
    schema = ArraySchema(array_name, attributes$name, dimensions$name, namespace = ns,
                         info = structure(list(fieldTypes), names = .DT))
    return(schema)
  }

)

`[[.Repo` <- function(x, schema_alias, force_reload = FALSE) {
  #' Return a registered array schema
  #'
  #' Load on first access, then use the cached entry.
  item = x$cached_schemas[[schema_alias]]
  if(is.null(item) || force_reload){
    arrayName = x$schema_registry[[schema_alias]]
    assert(length(arrayName) == 1, "Schema '%s' not found in Repo", schema_alias)
    item = x$load_schema(arrayName)
    x$cached_schemas[[schema_alias]] <- item
  }
  return(item)
}


# Array related functions -----------------------------------------------------------------------------------------

Repo$methods(
  remove_array_versions = function(alias, version_id, keep) {
    #' Remove versions of array snapshots
    #'
    #' @note if only `alias` is specified, remove all but the most recent version.
    #' @param alias a user friendly name for a scidb array
    #' @param version_id remove all versions up the `version_id`
    #' @keep remove all but the last `keep` versions
    assert(!(methods::hasArg('version_id') && methods::hasArg('keep')), "Cannot specify version_id and keep at the same time!")
    array = .self[[alias]]
    if(methods::hasArg('version_id')){
      afl = .afl(array$to_afl() %remove_versions% version_id)
    } else if(methods::hasArg('keep')) {
      afl = .afl(array$to_afl() %remove_versions% sprintf("keep: %s", keep))
    } else {
      afl = .afl(array$to_afl() %remove_versions% NULL)
    }
    run_only(afl)
  }

)
