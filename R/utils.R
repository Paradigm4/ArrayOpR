source("R/private_utils/__source.R", local = TRUE, chdir = TRUE)

# DBUtils class ----

#' Database utility class
#' 
#' Common scidb operations
DBUtils = R6::R6Class(
  "DBUtils", portable = F, cloneable = F,
  public = list(
    #' @description 
    #' Generate a random array name so it does not collides with existing array names
    random_array_name = function(prefix = "Rarrayop_", n = 10L) {
      sprintf("%s_%s_", prefix, rawToChar(as.raw(sample(c(65:90,97:122), n, replace=TRUE))))
    }
    ,
    random_field_name = function(n = 10L) {
      sprintf("%s_", rawToChar(as.raw(sample(c(65:90,97:122), n, replace=TRUE))))
    }
    ,
    set_conn = function(new_conn) {
      private$conn = new_conn
    }
    ,
    get_conn = function(){
      if(is.null(private$conn)){
        private$conn = get_default_connection()
      }
      private$conn
    }
    ,
    clear_cache = function() {
      private$cached = list()
    }
    # start scidb utility functions
    ,
    list_arrays_in_ns = function(ns = "public"){
      assert_single_str(ns)
      from_formatted_afl("list(ns:%s)", ns) 
    }
    ,
    load_array_ops_from_namespace = function(ns = 'public'){
      arrayRecordsDf = list_arrays_in_ns(ns)$transmute('name', 'schema')$to_df()
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
    #' @param original_names A character vector
    #' @return A character vector of sanitized names
    sanitize_names = function(original_names) {
      gsub("^[_]+|[_]+$", '',
           gsub('_+', '_',
                gsub('[^a-zA-Z0-9_]+', '_', original_names)))
    }
    ,
    sanitize_names_for = function(obj) {
      assert_not_empty(names(obj), "No names found for `{.symbol}`", .symbol = deparse(substitute(obj)))
      names(obj) <- sanitize_names(names(obj))
      obj
    }
    ,
    db_namespaces= function(){
      from_formatted_afl("list('namespaces')") 
    }
    ,
    db_users = function(){
      from_formatted_afl("list('users')")
    }
    ,
    db_roles = function(){
      from_formatted_afl("list('roles')")
    }
    ,
    db_operators = function(){
      from_formatted_afl("list('operators')")
    }  
    ,
    db_instances = function(){
      from_formatted_afl("list('instances')")
    }  
    ,
    db_queries = function(){
      from_formatted_afl("list('queries')")
    }
    ,
    db_macros = function(){
      from_formatted_afl("list('macros')")
    }
    ,
    db_types = function(){
      from_formatted_afl("list('types')")
    }
    ,
    db_libraries = function(){
      from_formatted_afl("list('libraries')")
    }
    ,
    db_aggregates = function(){
      from_formatted_afl("list('aggregates')")
    }
  ),
  active = list(
    
    
  )
  ,
  private = list(
    conn = NULL,
    cached = list(),
    from_formatted_afl = function(afl_template, ...){
      fullAfl = sprintf(afl_template, ...)
      result = private$cached[[fullAfl]]
      if(is.null(result)) {
        result = get_conn()$array_from_afl(fullAfl)
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
