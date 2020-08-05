## Shared code for individual tests in this folder ----
`%>%` = dplyr::`%>%`

# Wrap up shared functions in a class for easy-access ----

TestNS <- R6::R6Class(
  "TestNS", 
  portable = FALSE,
  private = list(
    # Private functions ----
    .remove_arrays_from_namespace = function(){
      arrayNames = get_array_names()
      for(arr in arrayNames){
        fullName = sprintf("%s.%s", NS, arr)
        conn$execute(afl(fullName | remove))
      }
    }
    ,
    .try_create_ns = function() {
      tryCatch(
        {
          conn$execute(afl(NS | create_namespace))
          catf("Created namespace '%s'\n", NS)
        },
        error = function(e){
          catf("Namespace '%s' already exists or cannot be created.\n", NS)
        }
      )
    }
    ,
    .try_drop_ns = function() {
      tryCatch(
        {
          conn$execute(afl(NS | drop_namespace))
          catf("Dropped namespace '%s' \n", NS)
        },
        error = function(e) {
          catf("Failed to drop namespace '%s' \n", NS)
        }
      )
    }
  )
  ,
  # Public functions ----
  public = list(
    NS = NULL, # namespace
    conn = NULL, # arrayop::ArrayOp instance
    
    initialize = function(namespace, conn){
      self$NS = namespace
      self$conn = conn
    }
    ,
    db_setup = function() {
      .try_create_ns()
      .remove_arrays_from_namespace()
    }
    ,
    db_cleanup = function() {
      .remove_arrays_from_namespace()
      .try_drop_ns()
    }
    ,
    cleanup_after_each_test = function() {
      .remove_arrays_from_namespace()
    }
    ,
    # Return an array name prefixed with the test namespace
    full_array_name = function(name) {
      sprintf("%s.%s", NS, name)  
    }
    ,
    create_local_arrayop = function(name, schema){
      conn$array_op_from_schema_str(sprintf(
        "%s.%s %s", NS, name, schema
      ))
    }
    ,
    # Get array names without namespace
    get_array_names = function() {
      conn$query(
        sprintf("project(list(ns:%s), name)", NS), only_attributes = T
      )[['name']]
    }
    ,
    # param target: an arrayOp instance
    # param content: R data frame
    reset_array_with_content = function(target, content, recreate = TRUE) {
      # Reset array
      if(recreate){
        try(target$remove_self(conn = conn), silent = T)
        conn$execute(sprintf("create array %s", str(target)))
      }
      # Upload data 
      target$
        build_new(content)$
        change_schema(target)$
        overwrite(target)$
        execute(conn)
      
    }
  )
)

# Shared instance for all in-db tests ----
testNS = TestNS$new(namespace = "arrayop_unittest", conn = conn)
