
#' An environment for scidb access
#' 
#' DbAccess shield the `arrayop` package from depending directly on `scidb`
#' 
#' Within `arrayop` package, all scidb transcactions are done via DbAccess. 
#' No calls to `scidb::iquery` so that ArrayOp classes will not be affected by future scidb upgrade.
#' @param db scidb database connection object
#' @export
DbAccess <- function(db) {

  .dbquery_impl <- function(db,
    query,
    return,
    only_attributes) {
    logged_query = gsub("\\s+", " ", str_trim(query))
    # vw_logger_debug(message = logged_query)
    # query = compile_to_afl(query)
    qres = scidb::iquery(db = db,
      query = query,
      return = return,
      only_attributes = only_attributes)
    # vw_logger_debug(message = qres)
    return(qres)
  }

  # Wrapper function existing to optionally log queries and as a degree of freedom
  # for mocking queries to SciDB from the unit tests.
  dbquery = function(
    query_collection,
    return = FALSE,
    only_attributes = FALSE) {
    if (length(query_collection) > 1) {
      df_results = list()
      for (i in 1:length(query_collection)) {
        query = query_collection[i]
        qres = dbquery_impl(db = db,
          query = query,
          return = return,
          only_attributes = only_attributes)
        df_results[[i]] = qres
      }
      return(rbind.fill(df_results))
    }
    else {
      query = query_collection
      return(.dbquery_impl(db = db,
        query = query,
        return = return,
        only_attributes = only_attributes))
    }
  }

  load_schema_attrs <- function(fullArrayName) {
    # Get array attributes
    getAttrsQuery <- sprintf("project(attributes(%s), name, type_id)", fullArrayName)
    dfAttrs <- dbquery(query = getAttrsQuery, return = TRUE)
    colnames(dfAttrs)[colnames(dfAttrs) == 'type_id'] <- 'type'

    # Convert query results to a list of NumberField/TextField
    return(dfAttrs[, c('name', 'type')])
  }

  load_schema_dimensions <- function(fullArrayName) {
    # Get array dimensions
    # Actual dimensions include more than we currently need
    # name,start,length,chunk_interval,chunk_overlap,low,high,type
    # {0} 'i',0,4611686018427387904,1,0,0,2,'int64'
    getDimensionQuery <- sprintf("project(dimensions(%s), name, type)", fullArrayName)
    dfDimensions <- dbquery(query = getDimensionQuery, return = TRUE)
    return(dfDimensions[, c('name', 'type')])
  }

  load_df_from_afl <- function(afl, ...) {
    dbquery(afl, return = TRUE, ...)
  }

  run_afl <- function(afl, ...) {
    # Run AFL without returning any result
    dbquery(afl, return = FALSE, ...)
  }

}
