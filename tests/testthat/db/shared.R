## Shared code for individual tests in this folder ----
`%>%` = dplyr::`%>%`

# Wrap up shared functions in a class for easy-access ----

TestNS <- R6::R6Class(
  "TestNS", 
  portable = FALSE,
  private = list(
    # Private functions ----
    .remove_arrays_from_namespace = function(){
      arrayNames = repo$query(
        sprintf("project(list(ns:%s), name)", NS)
      )[['name']]
      for(arr in arrayNames){
        fullName = sprintf("%s.%s", NS, arr)
        repo$execute(afl(fullName | remove))
      }
    }
    ,
    .try_create_ns = function() {
      tryCatch(
        {
          repo$execute(afl(NS | create_namespace))
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
          repo$execute(afl(NS | drop_namespace))
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
    repo = NULL, # arrayop::ArrayOp instance
    
    initialize = function(namespace, repo){
      self$NS = namespace
      self$repo = repo
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
      repo$get_array(sprintf(
        "%s.%s %s", NS, name, schema
      ))
    }
    ,
    # Get array names without namespace
    get_array_names = function() {
      repo$query(
        sprintf("project(list(ns:%s), name)", NS), only_attributes = T
      )[['name']]
    }
    ,
    # param target: an arrayOp instance
    # param content: R data frame
    reset_array_with_content = function(target, content, recreate = TRUE) {
      # Reset array
      if(recreate){
        try(repo$.remove_array(target), silent = T)
        repo$.create_array(target)
      }
      # Upload data 
      repo$execute(
        target$
          build_new(content)$
          change_schema(target)$
          overwrite(target)
      )
    }
  )
)

# Shared instance for all in-db tests ----
testNS = TestNS$new(namespace = "arrayop_unittest", repo = repo)
