source("R/private_utils/__source.R", local = TRUE, chdir = TRUE)

# Export utility functions ----

#' An R env that hosts arrayop package utility functions
#' @export
dbutils = make_env(
  public = list(
    random_array_name = function(prefix = "Rarrayop_", n = 10L) {
      sprintf("%s_%s_", prefix, rawToChar(as.raw(sample(c(65:90,97:122), n, replace=TRUE))))
    }
    ,
    random_field_name = function(n = 10L) {
      sprintf("%s_", rawToChar(as.raw(sample(c(65:90,97:122), n, replace=TRUE))))
    }
    ,
    set_conn = function(new_conn) {
      .private$conn = new_conn
    }
    ,
    get_conn = function(){
      if(is.null(.private$conn)){
        .private$conn = get_default_connection()
      }
      .private$conn
    }
    ,
    clear_cache = function() {
      .private$cached = list()
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
  ),
  active = list(
    list_namespaces= function(){
      from_formatted_afl("list('namespaces')") 
    }
    ,
    list_users = function(){
      from_formatted_afl("list('users')")
    }
    ,
    list_roles = function(){
      from_formatted_afl("list('roles')")
    }
    ,
    list_operators = function(){
      from_formatted_afl("list('operators')")
    }  
  )
  ,
  private = list(
    conn = NULL,
    cached = list(),
    from_formatted_afl = function(afl_template, ...){
      fullAfl = sprintf(afl_template, ...)
      result = .private$cached[[fullAfl]]
      if(is.null(result)) {
        result = get_conn()$array_from_afl(fullAfl)
        .private$cached[[fullAfl]] = result
      }
      result
    }
  )
)
