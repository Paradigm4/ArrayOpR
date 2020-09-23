source("R/private_utils/__source.R", local = TRUE, chdir = TRUE)

# DBUtils class ----

#' Database utility class
#' 
#' @description 
#' Common scidb operations
#' @details 
#' Please access this singleton instance via `arrayop::dbutils`. 
#' No instance creation is needed.
#' 
#' The default connection is used. So we need to call `arrayop::db_connect` first
#' before any of the `dbutils` db related function is available. 
#' 
#' We can call `dbutils$set_conn(another_scidb_connection_obj)` to set a different
#' ScidbConnection object, which should be extremely rare.
DBUtils = R6::R6Class(
  "DBUtils", portable = F, cloneable = F,
  public = list(
    #' @description 
    #' Generate a random array name so it does not collides with existing array names
    #' 
    #' @param prefix A string used as the prefix of generated array name
    #' @param n An integer, length of the random characters in array name 
    #' excluding the prefix
    #' @return A string of 'prefix' + n random chars
    random_array_name = function(prefix = "Rarrayop_", n = 10L) {
      sprintf("%s_%s_", prefix, rawToChar(as.raw(sample(c(65:90,97:122), n, replace=TRUE))))
    }
    ,
    #' @description 
    #' Generate a random field name
    #' @param n An integer, length of the random characters in the field name
    #' @return A string
    random_field_name = function(n = 10L) {
      sprintf("%s_", rawToChar(as.raw(sample(c(65:90,97:122), n, replace=TRUE))))
    }
    ,
    #' @description 
    #' Set a `ScidbConnection` object that `dbutils` uses for db operations
    #' 
    #' No need to set the connection in most cases. The default conneciton object
    #' is used.
    #' 
    #' @param new_conn A `ScidbConnection` object
    #' @return NULL
    set_conn = function(new_conn) {
      private$conn = new_conn
    }
    ,
    #' @description 
    #' Get the `ScidbConnection` object the `dbutils` is using
    #' 
    #' If no `set_conn` call is made, by default it returns the default
    #' `ScidbConnection` object, identical to `get_default_connection()`
    #' @return A `ScidbConnection` object
    get_conn = function(){
      if(is.null(private$conn)){
        private$conn = get_default_connection()
      }
      private$conn
    }
    ,
    #' @description 
    #' Clear cached arrayOp instances
    #' 
    #' For performance concerns, `dbutils` caches every arrayOp it retrieves from
    #' scidb.
    #' 
    #' Call this function if there are array changes in scidb after the scidb
    #' connection is established
    #' @return NULL
    clear_cache = function() {
      private$cached = list()
    }
    ,
    #' @description 
    #' Returns an ArrayOp instance of the "list arrays" opeartion in a 
    #' scidb namespace
    #' 
    #' Implemented by scidb `list(ns:myNamespace)`. 
    #' 
    #' Throw an error if the namespace does not exist in scidb.
    #' @param ns String, a scidb namespace
    #' @return An ArrayOp instance
    list_arrays_in_ns = function(ns = "public"){
      assert_single_str(ns)
      from_formatted_afl("list(ns:%s)", ns) 
    }
    ,
    #' @description 
    #' Return a list of arrayOp instances from a namespace
    #' 
    #' @param ns String, a scidb namespace
    #' @return A named list of arrayOp instances where names are array names
    #' (without namespace prefix) and values are arrayOp instances
    load_array_ops_from_namespace = function(ns = 'public'){
      arrayRecordsDf = list_arrays_in_ns(ns)$transmute(name, schema)$to_df()
      .conn = get_conn()
      new_named_list(
        sapply(arrayRecordsDf$schema, .conn$array_from_schema),
        names = arrayRecordsDf$name
      )
    }
    ,
    #' @description 
    #' Sanitize (data frame or scidb array) names
    #'
    #' First replace any non-alphanumerical letter to _
    #' Then trim off any leading or trailing underscores.
    #' 
    #' This is useful when data frames or files contain characters not supported
    #' as scidb field names.
    #' 
    #' @examples
    #' names(myDataFrame) <- dbutils$sanitize_names(names(myDataFrame))
    #' 
    #' @param original_names A string vector
    #' @return A string vector of sanitized names
    sanitize_names = function(original_names) {
      gsub("^[_]+|[_]+$", '',
           gsub('_+', '_',
                gsub('[^a-zA-Z0-9_]+', '_', original_names)))
    }
    ,
    #' @description 
    #' Sanitize names for an R object
    #' 
    #' `sanitize_names_for(myObj)` is equvilant to 
    #' `names(myObj) <- sanitize_names(names(myObj))`
    #' @param obj An R object whose names are not NULL. Applicable to data frames
    #' and any R object that meets the requirement `!is.null(names(obj))`
    #' @return The same obj with sanitized names (in-place modification)
    sanitize_names_for = function(obj) {
      assert_not_empty(names(obj), "No names found for `{.symbol}`", .symbol = deparse(substitute(obj)))
      names(obj) <- sanitize_names(names(obj))
      obj
    }
    ,
    #' @description 
    #' Return an ArrayOp instance that encapsualtes all namespaces in scidb
    #' 
    #' The namespaces visible to us is determined by our scidb role and previlige.
    #' 
    #' @examples
    #' dbutils$db_namespace()$to_df()
    #' @return An arrayOp instance
    db_namespaces= function(){
      from_formatted_afl("list('namespaces')") 
    }
    ,
    #' @description 
    #' Return an ArrayOp instance that encapsualtes all users in scidb
    #' 
    #' The scidb users visible to us is determined by our scidb role and previlige.
    #' 
    #' @examples
    #' dbutils$db_users()$to_df()
    #' 
    #' @return An arrayOp instance
    db_users = function(){
      from_formatted_afl("list('users')")
    }
    ,
    #' @description 
    #' Return an ArrayOp instance that encapsualtes all user roles in scidb
    #' 
    #' The roles visible to us is determined by our scidb role and previlige.
    #' 
    #' @examples 
    #' dbutils$db_roles()$to_df()
    #' @return An arrayOp instance
    db_roles = function(){
      from_formatted_afl("list('roles')")
    }
    ,
    #' @description 
    #' Return an ArrayOp instance that encapsualtes all scidb operators
    #' @examples 
    #' dbutils$db_operators()$to_df()
    #' @return An arrayOp instance
    db_operators = function(){
      from_formatted_afl("list('operators')")
    }  
    ,
    #' @description 
    #' Return an ArrayOp instance that encapsualtes scidb instances
    #' @examples 
    #' dbutils$db_instances()$to_df()
    #' @return An arrayOp instance
    db_instances = function(){
      from_formatted_afl("list('instances')")
    }  
    ,
    #' @description 
    #' Return an ArrayOp instance that encapsualtes running scidb queries
    #' @examples 
    #' dbutils$db_queries()$to_df()
    #' @return An arrayOp instance
    db_queries = function(){
      from_formatted_afl("list('queries')")
    }
    ,
    #' @description 
    #' Return an ArrayOp instance that encapsualtes all scidb macros
    #' @examples 
    #' dbutils$db_macros()$to_df()
    #' @return An arrayOp instance
    db_macros = function(){
      from_formatted_afl("list('macros')")
    }
    ,
    #' @description 
    #' Return an ArrayOp instance that encapsualtes all scidb data types
    #' @examples 
    #' dbutils$db_types()$to_df()
    #' @return An arrayOp instance
    db_types = function(){
      from_formatted_afl("list('types')")
    }
    ,
    #' @description 
    #' Return an ArrayOp instance that encapsualtes all scidb libraries
    #' @examples 
    #' dbutils$db_libraries()$to_df()
    #' @return An arrayOp instance
    db_libraries = function(){
      from_formatted_afl("list('libraries')")
    }
    ,
    #' @description 
    #' Return an ArrayOp instance that encapsualtes all scidb aggregate functions
    #' @examples 
    #' dbutils$db_aggregates()$to_df()
    #' @return An arrayOp instance
    db_aggregates = function(){
      from_formatted_afl("list('aggregates')")
    }
  )
  ,
  private = list(
    conn = NULL,
    cached = list(),
    from_formatted_afl = function(afl_template, ...){
      fullAfl = sprintf(afl_template, ...)
      result = private$cached[[fullAfl]]
      if(is.null(result)) {
        result = get_conn()$afl_expr(fullAfl)
        private$cached[[fullAfl]] = result
      }
      result
    }
  )
)

# dbutils singleton object ----
dbutils = DBUtils$new()

#' @export dbutils
NULL
